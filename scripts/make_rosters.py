from __future__ import annotations

import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher, get_close_matches
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
import pandas as pd

DB_PATH = Path("database/nba.duckdb")
ROSTERS_PATH = Path("league_rosters.json")

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

def load_player_zscores(db_path: Path = DB_PATH) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    df = con.execute(QUERY).df()
    con.close()

    # normalize types
    df["personid"] = df["personid"].astype(str)
    df["firstname"] = df["firstname"].astype(str)
    df["familyname"] = df["familyname"].astype(str)
    df["totalz"] = pd.to_numeric(df["totalz"], errors="coerce")
    df["last_10_z"] = pd.to_numeric(df["last_10_z"], errors="coerce")
    return df


def _norm(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def build_name_index(df: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, dict]]:
    """
    Returns:
      - name_to_personid: normalized full name -> personid
      - personid_to_row: personid -> useful info (name, totalz, last_10_z)
    If duplicates exist, we keep the row with the higher totalz.
    """
    personid_to_row: Dict[str, dict] = {}
    name_to_personid: Dict[str, str] = {}

    for r in df.itertuples(index=False):
        pid = str(r.personid)
        full = f"{r.firstname} {r.familyname}".strip()
        nfull = _norm(full)

        existing = personid_to_row.get(pid)
        if existing is None:
            personid_to_row[pid] = {
                "personid": pid,
                "name": full,
                "totalz": float(r.totalz) if pd.notna(r.totalz) else None,
                "last_10_z": float(r.last_10_z) if pd.notna(r.last_10_z) else None,
            }

        # map name -> best pid by totalz
        current_pid = name_to_personid.get(nfull)
        if current_pid is None:
            name_to_personid[nfull] = pid
        else:
            cur = personid_to_row.get(current_pid, {})
            cur_total = cur.get("totalz") or float("-inf")
            new_total = personid_to_row[pid].get("totalz") or float("-inf")
            if new_total > cur_total:
                name_to_personid[nfull] = pid

    return name_to_personid, personid_to_row


def search_candidates(
    user_input: str,
    all_names: List[str],
    name_to_personid: Dict[str, str],
    personid_to_row: Dict[str, dict],
    k: int = 7,
) -> List[dict]:
    """
    Returns a list of candidate rows with similarity.
    """
    q = _norm(user_input)
    # difflib shortlist
    close = get_close_matches(q, all_names, n=k, cutoff=0.0)

    # also rank by SequenceMatcher for better ordering
    scored = []
    for nm in close:
        score = SequenceMatcher(a=q, b=nm).ratio()
        pid = name_to_personid[nm]
        row = personid_to_row[pid]
        scored.append({
            "score": score,
            "personid": pid,
            "name": row["name"],
            "totalz": row["totalz"],
            "last_10_z": row["last_10_z"],
        })
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


def resolve_player_interactive(
    user_input: str,
    all_names: List[str],
    name_to_personid: Dict[str, str],
    personid_to_row: Dict[str, dict],
) -> str:
    """
    Asks you to pick the right player when fuzzy match is ambiguous.
    Returns personid.
    """
    cands = search_candidates(user_input, all_names, name_to_personid, personid_to_row, k=8)

    print(f"\nInput: '{user_input}'")
    for i, c in enumerate(cands, start=1):
        print(
            f"  {i}. {c['name']} (personid={c['personid']}) "
            f"score={c['score']:.2f} totalz={c['totalz']} last10={c['last_10_z']}"
        )
    print("  0. Skip / not found")

    while True:
        choice = input("Pick number: ").strip()
        if choice.isdigit():
            n = int(choice)
            if n == 0:
                return ""
            if 1 <= n <= len(cands):
                return cands[n - 1]["personid"]
        print("Please enter a valid number.")


def build_rosters_interactive(df: pd.DataFrame) -> Dict[str, List[str]]:
    name_to_pid, pid_to_row = build_name_index(df)
    all_names = list(name_to_pid.keys())

    rosters: Dict[str, List[str]] = {}

    print("\n=== Roster Builder ===")
    print("Enter a fantasy team name, then player names (one per line).")
    print("Blank player name finishes that team. Blank team name ends.\n")

    while True:
        team = input("Fantasy team name (blank to finish): ").strip()
        if not team:
            break

        team_ids: List[str] = []
        while True:
            player = input(f"  Player for '{team}' (blank to finish team): ").strip()
            if not player:
                break

            pid = resolve_player_interactive(player, all_names, name_to_pid, pid_to_row)
            if pid:
                if pid not in team_ids:
                    team_ids.append(pid)
                    print(f"  ✅ Added: {pid_to_row[pid]['name']} ({pid})")
                else:
                    print("  (Already added.)")
            else:
                print("  ⏭️ Skipped.")

        rosters[team] = team_ids
        print(f"Saved {len(team_ids)} players for team '{team}'.\n")

    return rosters


def save_rosters(rosters: Dict[str, List[str]], path: Path = ROSTERS_PATH) -> None:
    path.write_text(json.dumps(rosters, indent=2, sort_keys=True))
    print(f"\n✅ Wrote rosters to {path}")


def main() -> None:
    df = load_player_zscores()
    rosters = build_rosters_interactive(df)
    save_rosters(rosters)


if __name__ == "__main__":
    main()
