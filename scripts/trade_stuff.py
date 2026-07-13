from pathlib import Path
import duckdb
import pandas as pd
from datetime import datetime


DB_PATH = Path("database/nba.duckdb")

QUERY = """
select
    seasonid,
    personid,
    firstname,
    familyname,
    gamedate,
    minuteszscore,
    pointszscore,
    assistszscore,
    stealszscore,
    blockszscore,
    reboundszscore,
    threeszscore,
    doubledoublezscore,
    efgpctzscore,
    ftpctzscore,
    totalz,
    last_10_z
from (
    select 
        t1.*,
        t3.totalz as last_10_z
    from nba.fantasy.zscoresaverages t1
    inner join nba.fantasy.averages_current t2
        on t1.personid = t2.personid
       and t1.gamedate = t2.gamedate
    join nba.fantasy.zscores_last10 t3
        on t1.personid = t3.personid
       and t1.gamedate = t3.gamedate
)
order by totalz desc
"""
def write_trade_output(
    df: pd.DataFrame,
    my_team: str,
    their_team: str,
    out_dir: Path = Path("output"),
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_my = my_team.strip().lower().replace(" ", "_")
    safe_their = their_team.strip().lower().replace(" ", "_")

    # main file (timestamped)
    out_path = out_dir / f"trades_{safe_my}_vs_{safe_their}_{ts}.csv"
    df.to_csv(out_path, index=False)

    # convenience "latest" file
    latest_path = out_dir / f"trades_{safe_my}_vs_{safe_their}_latest.csv"
    df.to_csv(latest_path, index=False)

    return out_path

def load_player_zscores(db_path: Path = DB_PATH) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    df = con.execute(QUERY).df()
    con.close()

    numeric_cols = [
        "totalz",
        "last_10_z",
    ]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    return df

def add_trade_value(
    df: pd.DataFrame,
    season_weight: float = 0.6,
    last10_weight: float = 0.4,
) -> pd.DataFrame:
    out = df.copy()
    out["trade_value"] = (
        season_weight * out["totalz"]
        + last10_weight * out["last_10_z"]
    )
    return out

from typing import Sequence, Dict

def evaluate_trade(
    df: pd.DataFrame,
    side_a: Sequence[str],
    side_b: Sequence[str],
) -> Dict[str, dict]:
    """
    side_a / side_b = list of personid values each side is giving up
    """
    df = df.copy()
    df["personid"] = df["personid"].astype(str)

    a_out = df[df["personid"].isin(map(str, side_a))]
    b_out = df[df["personid"].isin(map(str, side_b))]

    def totals(x: pd.DataFrame) -> dict:
        return {
            "players": list(
                x.apply(
                    lambda r: f"{r.firstname} {r.familyname}", axis=1
                )
            ),
            "totalz": float(x["totalz"].sum()),
            "last_10_z": float(x["last_10_z"].sum()),
            "trade_value": float(x["trade_value"].sum()),
        }

    a_out_t = totals(a_out)
    b_out_t = totals(b_out)

    return {
        "side_a": {
            "gives": a_out_t,
            "gets": b_out_t,
            "net_trade_value": b_out_t["trade_value"] - a_out_t["trade_value"],
        },
        "side_b": {
            "gives": b_out_t,
            "gets": a_out_t,
            "net_trade_value": a_out_t["trade_value"] - b_out_t["trade_value"],
        },
    }

from itertools import combinations

def suggest_2for2_trades(
    df: pd.DataFrame,
    max_players: int = 20,
    top_n: int = 15,
) -> pd.DataFrame:
    """
    Returns the most balanced 2-for-2 swaps by trade_value
    """
    pool = (
        df.sort_values("trade_value", ascending=False)
          .head(max_players)
          .reset_index(drop=True)
    )

    rows = []

    for (a1, a2) in combinations(pool.itertuples(index=False), 2):
        a_val = a1.trade_value + a2.trade_value

        for (b1, b2) in combinations(pool.itertuples(index=False), 2):
            # skip same players
            if len({a1.personid, a2.personid, b1.personid, b2.personid}) < 4:
                continue

            b_val = b1.trade_value + b2.trade_value

            rows.append({
                "side_a": f"{a1.firstname} {a1.familyname}, {a2.firstname} {a2.familyname}",
                "side_b": f"{b1.firstname} {b1.familyname}, {b2.firstname} {b2.familyname}",
                "side_a_value": a_val,
                "side_b_value": b_val,
                "abs_diff": abs(a_val - b_val),
            })

    return (
        pd.DataFrame(rows)
        .sort_values("abs_diff")
        .head(top_n)
        .reset_index(drop=True)
    )


import json
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Sequence, Tuple, Optional

import pandas as pd

ROSTERS_PATH = Path("league_rosters.json")


def load_rosters(path: Path = ROSTERS_PATH) -> Dict[str, List[str]]:
    rosters = json.loads(path.read_text())
    # normalize to strings
    return {k: [str(x) for x in v] for k, v in rosters.items()}


def _name_map(df: pd.DataFrame) -> Dict[str, str]:
    tmp = df.copy()
    tmp["personid"] = tmp["personid"].astype(str)
    tmp["full_name"] = tmp["firstname"].astype(str) + " " + tmp["familyname"].astype(str)
    return dict(zip(tmp["personid"], tmp["full_name"]))


def filter_to_roster(df: pd.DataFrame, personids: Sequence[str]) -> pd.DataFrame:
    dfx = df.copy()
    dfx["personid"] = dfx["personid"].astype(str)
    keep = set(map(str, personids))
    return dfx[dfx["personid"].isin(keep)].copy()


def suggest_trades_between_rosters(
    df: pd.DataFrame,
    my_roster: Sequence[str],
    their_roster: Sequence[str],
    *,
    mode: str = "both",          # "fair", "win_for_me", "both"
    trade_types: Sequence[str] = ("1for1", "2for2"),
    top_n: int = 15,
    max_players_each: int = 14,  # safety cap in case a roster file is wrong
    min_minutesz: Optional[float] = None,  # optional filter to remove no-minutes players
) -> pd.DataFrame:
    """
    Returns candidate trades only using players on the two specified rosters.
    """

    dfx = df.copy()
    dfx["personid"] = dfx["personid"].astype(str)

    # optional filter: avoid dead players / DNP
    if min_minutesz is not None and "minuteszscore" in dfx.columns:
        dfx["minuteszscore"] = pd.to_numeric(dfx["minuteszscore"], errors="coerce")
        dfx = dfx[dfx["minuteszscore"].fillna(-999) >= min_minutesz].copy()

    my_ids = list(map(str, my_roster))[:max_players_each]
    th_ids = list(map(str, their_roster))[:max_players_each]

    my_df = filter_to_roster(dfx, my_ids)
    th_df = filter_to_roster(dfx, th_ids)

    # If a player is missing from df (bad id or not in tables), they won't appear
    if my_df.empty:
        raise ValueError("Your roster has no matching personids in the z-score table.")
    if th_df.empty:
        raise ValueError("Their roster has no matching personids in the z-score table.")

    nm = _name_map(dfx)

    def pack_players(rows: Sequence[dict]) -> Tuple[List[str], float]:
        ids = [str(r["personid"]) for r in rows]
        val = float(sum(r["trade_value"] for r in rows))
        return ids, val

    # pre-build dict records for fast combo iteration
    my_records = my_df[["personid", "trade_value", "totalz", "last_10_z"]].to_dict("records")
    th_records = th_df[["personid", "trade_value", "totalz", "last_10_z"]].to_dict("records")

    results: List[dict] = []

    # 1-for-1
    if "1for1" in trade_types:
        for a in my_records:
            for b in th_records:
                my_out = [a]
                th_out = [b]
                my_ids2, my_val = pack_players(my_out)
                th_ids2, th_val = pack_players(th_out)

                net_for_me = th_val - my_val  # positive means I gain value
                abs_diff = abs(th_val - my_val)

                results.append({
                    "type": "1for1",
                    "i_send": ", ".join(nm.get(pid, pid) for pid in my_ids2),
                    "i_get": ", ".join(nm.get(pid, pid) for pid in th_ids2),
                    "my_value_out": my_val,
                    "their_value_out": th_val,
                    "net_for_me": net_for_me,
                    "abs_diff": abs_diff,
                })

    # 2-for-2
    if "2for2" in trade_types and len(my_records) >= 2 and len(th_records) >= 2:
        for a1, a2 in combinations(my_records, 2):
            my_out = [a1, a2]
            my_ids2, my_val = pack_players(my_out)

            for b1, b2 in combinations(th_records, 2):
                th_out = [b1, b2]
                th_ids2, th_val = pack_players(th_out)

                net_for_me = th_val - my_val
                abs_diff = abs(th_val - my_val)

                results.append({
                    "type": "2for2",
                    "i_send": ", ".join(nm.get(pid, pid) for pid in my_ids2),
                    "i_get": ", ".join(nm.get(pid, pid) for pid in th_ids2),
                    "my_value_out": my_val,
                    "their_value_out": th_val,
                    "net_for_me": net_for_me,
                    "abs_diff": abs_diff,
                })

    out = pd.DataFrame(results)

    if out.empty:
        return out

    # Ranking strategy
    if mode == "fair":
        out = out.sort_values(["abs_diff", "net_for_me"], ascending=[True, False])
    elif mode == "win_for_me":
        out = out.sort_values(["net_for_me", "abs_diff"], ascending=[False, True])
    else:  # both
        # show deals that are close AND positive for me
        out = out.sort_values(["abs_diff", "net_for_me"], ascending=[True, False])

    return out.head(top_n).reset_index(drop=True)

def main():
    df = load_player_zscores()
    df = add_trade_value(df)

    rosters = load_rosters()

    my_team = "dylan"
    their_team = "zachhite"

    suggestions = suggest_trades_between_rosters(
        df,
        my_roster=rosters[my_team],
        their_roster=rosters[their_team],
        mode="both",                 # "fair" or "win_for_me"
        trade_types=("1for1", "2for2"),
        top_n=50,
        min_minutesz=-0.5,
    )

    # Print to terminal
    print(suggestions)

    # Write to output/
    out_file = write_trade_output(suggestions, my_team, their_team)
    print(f"\n✅ Wrote trade suggestions to: {out_file}")


if __name__ == "__main__":
    main()
