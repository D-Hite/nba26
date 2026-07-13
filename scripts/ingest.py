import os
import time
import random
import argparse
from datetime import datetime
from typing import Dict, List, Set, Tuple

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from requests.exceptions import Timeout
from urllib3.exceptions import ReadTimeoutError
from concurrent.futures import ThreadPoolExecutor, as_completed

import nba_api.stats.endpoints as ep

from utils import logger
from utils.duckdb_sink import (
    get_duckdb_conn,
    upsert_delete_insert,
    existing_gameids,
    table_info,
    upsert_log,
    DUCKDB_SCHEMA,
)

# ============================================================
# CONFIG
# ============================================================

load_dotenv(find_dotenv())
BASE_PATH = os.getcwd()

ENDPOINTS = ["advanced", "fourfactors", "misc", "scoring", "traditional"]
FD = {
    "advanced": ep.boxscoreadvancedv3.BoxScoreAdvancedV3,
    "fourfactors": ep.boxscorefourfactorsv3.BoxScoreFourFactorsV3,
    "misc": ep.boxscoremiscv3.BoxScoreMiscV3,
    "scoring": ep.boxscorescoringv3.BoxScoreScoringV3,
    "traditional": ep.boxscoretraditionalv3.BoxScoreTraditionalV3,
}

DEFAULT_DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    "/Users/dhite/Documents/GitHub/nba26duckdb/database/nba.duckdb",
)

NBA_FETCH_WORKERS = int(os.getenv("NBA_FETCH_WORKERS", "8"))

# ============================================================
# PATH HELPERS
# ============================================================

def get_nba_season(date=None) -> str:
    date = date or datetime.today()
    year = date.year
    start = year - 1 if date.month <= 7 else year
    end = start + 1
    return f"{start}-{str(end)[-2:]}"


def _local_file_paths(endpoint: str, season: str) -> Tuple[str, str]:
    team_path = os.path.join(BASE_PATH, "data", "raw", "teams", endpoint, f"teams_{endpoint}{season}.csv")
    players_path = os.path.join(BASE_PATH, "data", "raw", "players", endpoint, f"players_{endpoint}{season}.csv")
    return team_path, players_path


def _append_df_to_csv(df: pd.DataFrame, path: str) -> None:
    if df is None or df.empty:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=write_header)


# ============================================================
# LINES (FAST: DUCKDB reads directory)
# ============================================================

def ingest_lines_simple(duck_conn) -> None:
    # Make sure BASE_PATH points to repo root; if this file is in scripts/, use parent:
    # repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    repo_root = os.getcwd()
    lines_glob = os.path.join(repo_root, "data", "raw", "lines", "*.csv")

    schema = DUCKDB_SCHEMA
    table = "lines_table"

    logger.info(f"Lines ingest glob: {lines_glob}")

    duck_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    try:
        duck_conn.execute(f"""
            CREATE OR REPLACE TABLE {schema}.{table} AS
            SELECT *
            FROM read_csv_auto('{lines_glob}', header=true, union_by_name=true);
        """)
        cnt = duck_conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        logger.info(f"Lines table refreshed: {schema}.{table} ({cnt} rows)")
    except Exception as e:
        logger.error(f"Lines ingest failed. Path={lines_glob}. Error: {e}")
        raise

# ============================================================
# LOG
# ============================================================

def fetch_log() -> Tuple[pd.DataFrame, Set[str]]:
    """
    Fetch full season log, write to local cached CSV, return (log_df, distinct_game_ids)
    """
    season = get_nba_season()
    log_dir = os.path.join(BASE_PATH, "data", "raw", "log")
    log_file = os.path.join(log_dir, f"log{season}.csv")

    try:
        result = ep.leaguegamefinder.LeagueGameFinder(season_nullable=season)
        all_games = result.get_data_frames()[0]

        rs = all_games[(all_games.SEASON_ID == "2" + season[:4]) & (all_games.GAME_ID.str.startswith("002"))]
        ps = all_games[(all_games.SEASON_ID == "4" + season[:4]) & (all_games.GAME_ID.str.startswith("004"))]
        cup = all_games[(all_games.SEASON_ID == "6" + season[:4]) & (all_games.GAME_ID.str.startswith("006"))]

        log_df = pd.concat([rs, ps, cup], ignore_index=True)
        game_ids = {str(g).zfill(10) for g in log_df["GAME_ID"]}

        os.makedirs(log_dir, exist_ok=True)
        log_df.to_csv(log_file, index=False)
        logger.info(f"Wrote log CSV {log_file} ({len(log_df)} team-rows).")

        return log_df, game_ids

    except Exception as e:
        logger.error(f"Failed live fetch; loading cached log {season}: {e}")
        if not os.path.exists(log_file):
            raise
        log_df = pd.read_csv(log_file, dtype={"GAME_ID": str})
        game_ids = {str(g).zfill(10) for g in log_df["GAME_ID"]}
        return log_df, game_ids


# ============================================================
# NBA API FETCH (RETRIES) + PARALLEL BATCH FETCH
# ============================================================

def get_game(func, gid: str):
    retries = 0
    max_retries = 3

    while retries < max_retries:
        try:
            game = func(game_id=gid).get_data_frames()
            return game[0], game[1]  # players_df, teams_df
        except (TimeoutError, Timeout, ReadTimeoutError):
            retries += 1
            time.sleep(random.uniform(0.2, 0.8) * (retries + 1))
        except Exception as e:
            logger.error(f"Permanent fail for {gid}: {e}")
            return None, None

    logger.error(f"Max retries exceeded for {gid}")
    return None, None


def fetch_games_for_endpoint(endpoint: str, gids: List[str], max_workers: int) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """
    Fetch many gameids in parallel for one endpoint.
    Returns: (players_df, teams_df, ok_gids)
    """
    func = FD[endpoint]

    players_frames: List[pd.DataFrame] = []
    teams_frames: List[pd.DataFrame] = []
    ok: List[str] = []

    def _fetch(gid: str):
        p, t = get_game(func, gid)
        return gid, p, t

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_fetch, gid): gid for gid in gids}
        for fut in as_completed(futures):
            gid, p, t = fut.result()
            if p is None or t is None or p.empty or t.empty:
                continue
            players_frames.append(p)
            teams_frames.append(t)
            ok.append(gid)

    players_df = pd.concat(players_frames, ignore_index=True) if players_frames else pd.DataFrame()
    teams_df = pd.concat(teams_frames, ignore_index=True) if teams_frames else pd.DataFrame()

    return players_df, teams_df, ok


# ============================================================
# DUCKDB UPSERT (BATCH)
# ============================================================

def upsert_endpoint_batch(duck_conn, endpoint: str, teams_df: pd.DataFrame, players_df: pd.DataFrame) -> None:
    """
    Batch upsert for an endpoint (one call per table).
    """
    ts, tt, tkeys = table_info(endpoint, is_team=True)
    ps, pt, pkeys = table_info(endpoint, is_team=False)

    if teams_df is not None and not teams_df.empty:
        upsert_delete_insert(duck_conn, teams_df, schema=ts, table=tt, key_cols=tkeys)
    if players_df is not None and not players_df.empty:
        upsert_delete_insert(duck_conn, players_df, schema=ps, table=pt, key_cols=pkeys)


# ============================================================
# FAST INGEST + FAST RESCRAPE
# ============================================================

def ingest_daily(duck_conn) -> None:
    """
    Fast ingest:
      - log upsert
      - lines ingest (DuckDB reads dir)
      - per endpoint:
          * compute needed gids
          * fetch all needed gids in parallel
          * batch upsert once per table
          * write CSV backup directly from fetched dfs (no DuckDB readback)
    """
    # DuckDB performance knobs (safe)
    try:
        duck_conn.execute("PRAGMA enable_object_cache=true;")
        duck_conn.execute(f"PRAGMA threads={max(2, NBA_FETCH_WORKERS)};")
    except Exception:
        pass

    season = get_nba_season()
    log_df, season_games = fetch_log()
    upsert_log(duck_conn, log_df)

    # lines
    try:
        ingest_lines_simple(duck_conn)
    except Exception as e:
        logger.error(f"Lines ingest failed: {e}")

    logger.info(f"--- Ingest start (season={season}) ---")

    for endpoint in ENDPOINTS:
        stored = existing_gameids(duck_conn, endpoint)
        needed = sorted(season_games - stored)

        logger.info(f"{endpoint}: {len(needed)} games missing")

        if not needed:
            continue

        # 1) fetch all missing games (parallel)
        players_df, teams_df, ok_gids = fetch_games_for_endpoint(endpoint, needed, max_workers=NBA_FETCH_WORKERS)

        if not ok_gids:
            logger.warning(f"{endpoint}: no games fetched successfully")
            continue

        # 2) one batch upsert per table
        try:
            upsert_endpoint_batch(duck_conn, endpoint, teams_df, players_df)
        except Exception as e:
            logger.error(f"{endpoint}: batch upsert failed: {e}")
            continue

        # 3) CSV backup (fast: write from dfs we already have)
        try:
            team_file, player_file = _local_file_paths(endpoint, season)
            _append_df_to_csv(teams_df, team_file)
            _append_df_to_csv(players_df, player_file)
        except Exception as e:
            logger.error(f"{endpoint}: CSV backup failed: {e}")

        logger.info(f"{endpoint}: inserted {len(set(ok_gids))} games")

    logger.info("--- Ingest done ---")


def rescrape_games(duck_conn, gameids: List[str]) -> None:
    """
    Fast rescrape:
      - for each endpoint:
          * fetch all gids in parallel
          * batch upsert once per table
      - refresh log (optional)
      - refresh lines (optional)
    """
    # DuckDB performance knobs (safe)
    try:
        duck_conn.execute("PRAGMA enable_object_cache=true;")
        duck_conn.execute(f"PRAGMA threads={max(2, NBA_FETCH_WORKERS)};")
    except Exception:
        pass

    season = get_nba_season()
    gids = [str(g).zfill(10) for g in gameids]
    gids_set = set(gids)

    logger.info(f"--- Rescrape start: {len(gids_set)} games ---")

    for endpoint in ENDPOINTS:
        players_df, teams_df, ok_gids = fetch_games_for_endpoint(endpoint, gids, max_workers=NBA_FETCH_WORKERS)

        if not ok_gids:
            logger.warning(f"Rescrape {endpoint}: nothing fetched")
            continue

        try:
            upsert_endpoint_batch(duck_conn, endpoint, teams_df, players_df)
        except Exception as e:
            logger.error(f"Rescrape {endpoint}: batch upsert failed: {e}")
            continue

        # Optional: refresh CSVs for just these gids (your old logic read+rewrite CSVs).
        # If you want speed over perfect CSV hygiene, you can skip rewriting.
        logger.info(f"Rescrape {endpoint}: updated {len(set(ok_gids))} games")

    # refresh log table
    try:
        log_df, _ = fetch_log()
        upsert_log(duck_conn, log_df)
    except Exception as e:
        logger.error(f"Rescrape: log refresh failed: {e}")

    # refresh lines
    try:
        ingest_lines_simple(duck_conn)
    except Exception as e:
        logger.error(f"Rescrape: lines refresh failed: {e}")

    logger.info("--- Rescrape done ---")


# ============================================================
# CLI
# ============================================================

def _parse_gameids_arg(arg: str) -> List[str]:
    """
    Accepts:
      - "0022500059"
      - "0022500059,0022500060"
      - "0022500059 0022500060"
    """
    if not arg:
        return []
    parts = arg.replace(",", " ").split()
    return [p.strip() for p in parts if p.strip()]


def main():
    parser = argparse.ArgumentParser(description="NBA ingest (DuckDB fast) + optional rescrape set of games")
    parser.add_argument("--duckdb-path", default=DEFAULT_DUCKDB_PATH)

    parser.add_argument("--ingest", action="store_true", help="Run daily ingestion (fast).")
    parser.add_argument("--rescrape", default="", help="Rescrape games (comma or space separated gameIds).")
    parser.add_argument("--rescrape-file", default="", help="Path to text file with gameIds (one per line).")

    args = parser.parse_args()
    duck_conn = get_duckdb_conn(args.duckdb_path)

    try:
        if args.ingest:
            ingest_daily(duck_conn)

        gids: List[str] = []
        gids.extend(_parse_gameids_arg(args.rescrape))

        if args.rescrape_file:
            with open(args.rescrape_file, "r") as f:
                gids.extend([line.strip() for line in f.readlines() if line.strip()])

        if gids:
            rescrape_games(duck_conn, gids)

    finally:
        try:
            duck_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
