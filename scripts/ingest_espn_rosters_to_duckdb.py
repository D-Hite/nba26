from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pandas as pd


DB_PATH = Path("database/nba.duckdb")
JSON_PATH = Path("output/league_rosters.json")

SNAPSHOT_TABLE = "nba.espn.fantasy_roster_snapshot"


NINE_CAT_KEYS = ["PTS", "REB", "AST", "STL", "BLK", "TO", "3PM", "FG%", "FT%"]


def flatten_rosters(rosters: Dict[str, List[Dict[str, Any]]], snapshot_date: date) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for team_name, players in rosters.items():
        for p in players:
            nine = p.get("nine_cat_averages") or {}
            row = {
                "snapshot_date": snapshot_date.isoformat(),
                "fantasy_team": team_name,

                "playerId": p.get("playerId"),
                "name": p.get("name"),
                "position": p.get("position"),
                "proTeam": p.get("proTeam"),

                "acquisitionType": p.get("acquisitionType"),
                "injured": p.get("injured"),
                "injuryStatus": p.get("injuryStatus"),

                "avg_points": p.get("avg_points"),
                "total_points": p.get("total_points"),
                "projected_avg_points": p.get("projected_avg_points"),
                "projected_total_points": p.get("projected_total_points"),

                "posRank": p.get("posRank"),
            }

            # Expand nine-cat into columns
            for k in NINE_CAT_KEYS:
                col = "nine_" + (
                    k.replace("%", "pct")
                     .replace("3PM", "3pm")
                     .lower()
                )
                row[col] = nine.get(k)

            rows.append(row)

    df = pd.DataFrame(rows)

    # Type hygiene
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    df["playerId"] = pd.to_numeric(df["playerId"], errors="coerce").astype("Int64")
    df["posRank"] = pd.to_numeric(df["posRank"], errors="coerce").astype("Int64")

    for c in ["avg_points", "total_points", "projected_avg_points", "projected_total_points"] + \
             [col for col in df.columns if col.startswith("nine_")]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS nba;")
    con.execute("CREATE SCHEMA IF NOT EXISTS nba.espn;")

    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
        snapshot_date DATE,
        fantasy_team TEXT,

        playerId BIGINT,
        name TEXT,
        position TEXT,
        proTeam TEXT,

        acquisitionType TEXT,
        injured BOOLEAN,
        injuryStatus TEXT,

        avg_points DOUBLE,
        total_points DOUBLE,
        projected_avg_points DOUBLE,
        projected_total_points DOUBLE,

        posRank INTEGER,

        nine_pts DOUBLE,
        nine_reb DOUBLE,
        nine_ast DOUBLE,
        nine_stl DOUBLE,
        nine_blk DOUBLE,
        nine_to DOUBLE,
        nine_3pm DOUBLE,
        nine_fgpct DOUBLE,
        nine_ftpct DOUBLE
    );
    """)

    # Prevent duplicate inserts for same day/team/player
    con.execute(f"""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_roster_snapshot
    ON {SNAPSHOT_TABLE}(snapshot_date, fantasy_team, playerId);
    """)


def upsert_snapshot(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    # DuckDB supports INSERT OR REPLACE when there’s a UNIQUE index constraint.
    con.register("df_snapshot", df)

    con.execute(f"""
    INSERT OR REPLACE INTO {SNAPSHOT_TABLE}
    SELECT
        snapshot_date,
        fantasy_team,
        playerId,
        name,
        position,
        proTeam,
        acquisitionType,
        injured,
        injuryStatus,
        avg_points,
        total_points,
        projected_avg_points,
        projected_total_points,
        posRank,
        nine_pts,
        nine_reb,
        nine_ast,
        nine_stl,
        nine_blk,
        nine_to,
        nine_3pm,
        nine_fgpct,
        nine_ftpct
    FROM df_snapshot
    """)

    con.unregister("df_snapshot")


def create_current_view(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
    CREATE OR REPLACE VIEW nba.espn.fantasy_roster_current AS
    WITH latest AS (
        SELECT max(snapshot_date) AS snapshot_date
        FROM nba.espn.fantasy_roster_snapshot
    )
    SELECT s.*
    FROM nba.espn.fantasy_roster_snapshot s
    JOIN latest l
      ON s.snapshot_date = l.snapshot_date;
    """)


def main() -> None:
    rosters = json.loads(JSON_PATH.read_text())
    snap_date = date.today()

    df = flatten_rosters(rosters, snap_date)

    con = duckdb.connect(str(DB_PATH))
    ensure_schema(con)
    upsert_snapshot(con, df)
    create_current_view(con)
    con.close()

    print(f"✅ Upserted {len(df)} rows into {SNAPSHOT_TABLE} for {snap_date}")


if __name__ == "__main__":
    main()
