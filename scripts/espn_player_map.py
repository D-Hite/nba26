from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from difflib import SequenceMatcher
import unicodedata


DB_PATH = Path("database/nba.duckdb")


def norm_name(s: str) -> str:
    if s is None:
        return ""

    # Lower + trim
    s = s.strip().lower()

    # Strip accents/diacritics: "dončić" -> "doncic"
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # Remove common suffixes
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", s)

    # Remove punctuation (keep letters/spaces)
    s = re.sub(r"[^a-z\s]", "", s)

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()

    return s



def best_name_match(query: str, candidates: pd.Series) -> tuple[int, float]:
    best_i = -1
    best_score = -1.0
    for i, cand in enumerate(candidates):
        score = SequenceMatcher(a=query, b=cand).ratio()
        if score > best_score:
            best_score = score
            best_i = i
    return best_i, best_score


def main() -> None:
    con = duckdb.connect(str(DB_PATH))

    # 1) NBA names from your full player dimension (base.players_combined)
    nba = con.execute("""
        WITH names AS (
            SELECT
                CAST(personId AS VARCHAR) AS personid,
                firstName AS firstname,
                familyName AS familyname,
                gameDate,
                ROW_NUMBER() OVER (
                    PARTITION BY personId
                    ORDER BY gameDate DESC NULLS LAST
                ) AS rn
            FROM base.players_combined
            WHERE personId IS NOT NULL
              AND firstName IS NOT NULL
              AND familyName IS NOT NULL
        )
        SELECT personid, firstname, familyname
        FROM names
        WHERE rn = 1
    """).df()

    nba["nba_name"] = (nba["firstname"].astype(str) + " " + nba["familyname"].astype(str)).str.strip()
    nba["nba_name_norm"] = nba["nba_name"].map(norm_name)

    # 2) ESPN names from your snapshot (current)
    espn = con.execute("""
        SELECT DISTINCT
            CAST(playerId AS VARCHAR) AS espn_player_id,
            name AS espn_name
        FROM nba.espn.fantasy_roster_current
        WHERE playerId IS NOT NULL AND name IS NOT NULL
    """).df()

    espn["espn_name_norm"] = espn["espn_name"].map(norm_name)

    rows = []
    for r in espn.itertuples(index=False):
        q = r.espn_name_norm

        # exact match first
        exact = nba[nba["nba_name_norm"] == q]
        if not exact.empty:
            personid = exact.iloc[0]["personid"]
            nba_name = exact.iloc[0]["nba_name"]
            rows.append({
                "espn_player_id": r.espn_player_id,
                "personid": personid,
                "match_type": "exact_norm",
                "match_score": 1.0,
                "espn_name": r.espn_name,
                "nba_name": nba_name,
                "created_at": datetime.utcnow().isoformat(timespec="seconds"),
            })
            continue

        FUZZY_MIN_SCORE = 0.92  # anything below this becomes unmatched

        best_i, best_score = best_name_match(q, nba["nba_name_norm"])
        if best_i >= 0 and best_score >= FUZZY_MIN_SCORE:
            personid = nba.iloc[best_i]["personid"]
            nba_name = nba.iloc[best_i]["nba_name"]
            rows.append({
                "espn_player_id": r.espn_player_id,
                "personid": personid,
                "match_type": "fuzzy_norm",
                "match_score": float(best_score),
                "espn_name": r.espn_name,
                "nba_name": nba_name,
                "created_at": datetime.utcnow().isoformat(timespec="seconds"),
            })
        else:
            rows.append({
                "espn_player_id": r.espn_player_id,
                "personid": None,
                "match_type": "unmatched",
                "match_score": float(best_score) if best_i >= 0 else None,
                "espn_name": r.espn_name,
                "nba_name": None,
                "created_at": datetime.utcnow().isoformat(timespec="seconds"),
            })


    map_df = pd.DataFrame(rows)
    map_df["needs_review"] = map_df["match_type"].eq("fuzzy_norm") & (map_df["match_score"] < 0.92)

    con.execute("CREATE SCHEMA IF NOT EXISTS nba.espn;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS nba.espn.player_map (
            espn_player_id VARCHAR,
            personid VARCHAR,
            match_type VARCHAR,
            match_score DOUBLE,
            espn_name VARCHAR,
            nba_name VARCHAR,
            created_at VARCHAR,
            needs_review BOOLEAN
        );
    """)

    con.execute("DELETE FROM nba.espn.player_map;")
    con.register("map_df", map_df)
    con.execute("INSERT INTO nba.espn.player_map SELECT * FROM map_df;")
    con.unregister("map_df")

    con.close()

    print(f"✅ Built player map: {len(map_df)} rows")
    print(f"⚠️ Needs review: {int(map_df['needs_review'].sum())} rows")


if __name__ == "__main__":
    main()
