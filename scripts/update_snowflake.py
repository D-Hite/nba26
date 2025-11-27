import os
import time
import random
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv, find_dotenv

from requests.exceptions import Timeout
from urllib3.exceptions import ReadTimeoutError
import nba_api.stats.endpoints as ep

from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

from utils import logger


# ============================================================
#                      CONFIG / CONSTANTS
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


# ============================================================
#                     SNOWFLAKE UTILITIES
# ============================================================

def get_snowflake_conn():
    """Create and return a Snowflake connection."""
    return connect(
        account="RPOSWFZ-OIB57673",
        user="grunk",
        password=os.getenv("PAT"),
        warehouse="compute_wh",
        database="NBA",
        schema="RAW",
    )


def upsert_to_snowflake(df, endpoint, is_team, conn):
    """
    Upserts a DataFrame into Snowflake using a staging table and MERGE.
    Handles both team and player endpoints.
    """

    # Normalize column names to uppercase for Snowflake
    df.columns = [c.upper() for c in df.columns]

    if is_team:
        target_table = f"TEAMS_{endpoint.upper()}"
        stage_table  = f"TEAMS_{endpoint.upper()}_STAGE"
        if endpoint.lower() == "traditional":
            key_cols = ["GAMEID", "TEAMID", "STARTERSBENCH"]
        else:
            key_cols = ["GAMEID", "TEAMID"]
    else:
        target_table = f"PLAYERS_{endpoint.upper()}"
        stage_table  = f"PLAYERS_{endpoint.upper()}_STAGE"
        key_cols = ["GAMEID", "TEAMID", "PERSONID"]

    col_defs = ", ".join([f"{col} STRING" for col in df.columns])
    create_stage_sql = f"""
        CREATE TABLE IF NOT EXISTS STAGE.{stage_table} ({col_defs});
    """
    with conn.cursor() as cur:
        cur.execute(create_stage_sql)
    
    try:
        write_pandas(
            conn,
            df,
            table_name=stage_table,
            schema="STAGE",
            database="NBA"
        )
    except Exception as e:
        logger.error(f"problem uploading data for {target_table}, {e}")

    non_key_cols = [c for c in df.columns if c not in key_cols]
    update_clause = ", ".join([f"t.{c} = COALESCE(s.{c}, t.{c})" for c in non_key_cols])

    merge_sql = f"""
        MERGE INTO NBA.RAW.{target_table} AS t
        USING NBA.STAGE.{stage_table} AS s
        ON {" AND ".join([f"t.{k} = s.{k}" for k in key_cols])}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({','.join(df.columns)})
        VALUES ({','.join(['s.' + c for c in df.columns])});
    """

    with conn.cursor() as cur:
        cur.execute(merge_sql)
        cur.execute(f"TRUNCATE TABLE STAGE.{stage_table}")

    logger.info(f"Upsert complete for {target_table} ({len(df)} rows).")


# ============================================================
#                     LOCAL FILE UTILITIES
# ============================================================

def _local_file_paths(endpoint, season):
    team_path = os.path.join(BASE_PATH, "data", "raw", "teams", endpoint,
                             f"teams_{endpoint}{season}.csv")
    players_path = os.path.join(BASE_PATH, "data", "raw", "players", endpoint,
                                f"players_{endpoint}{season}.csv")
    return team_path, players_path


def load_local_stats(endpoint, season):
    """Return (team_df, player_df) if local CSVs exist else empty DataFrames."""
    team_file, player_file = _local_file_paths(endpoint, season)

    team_df = pd.read_csv(team_file, dtype={"gameId": str}) if os.path.exists(team_file) else pd.DataFrame()
    player_df = pd.read_csv(player_file, dtype={"gameId": str}) if os.path.exists(player_file) else pd.DataFrame()

    return team_df, player_df


def upload_missing_local(endpoint, season, conn):
    """
    Sync local CSVs → Snowflake by inserting only missing games.
    Only used in initial bootstrapping.
    """
    team_df, player_df = load_local_stats(endpoint, season)
    if team_df.empty and player_df.empty:
        logger.info(f"No cached local data for {endpoint}.")
        return

    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT GAMEID FROM RAW.TEAMS_{endpoint.upper()}")
        sf_team_games = {row[0] for row in cur.fetchall()}

        cur.execute(f"SELECT DISTINCT GAMEID FROM RAW.PLAYERS_{endpoint.upper()}")
        sf_player_games = {row[0] for row in cur.fetchall()}

    local_games = set(team_df["gameId"]) | set(player_df["gameId"])
    missing = local_games - (sf_team_games & sf_player_games)

    if not missing:
        logger.info(f"No missing local data for {endpoint}.")
        return

    logger.info(f"Uploading {len(missing)} missing local games for {endpoint}.")

    team_missing = team_df[team_df["gameId"].isin(missing)]
    player_missing = player_df[player_df["gameId"].isin(missing)]

    write_pandas(conn, team_missing.rename(columns=str.upper),
                 f"TEAMS_{endpoint.upper()}")
    write_pandas(conn, player_missing.rename(columns=str.upper),
                 f"PLAYERS_{endpoint.upper()}")


def write_data(endpoint, season, tstats, pstats, snowflake=False, overwrite=False, conn=None):
    """
    Writes/overwrites local CSV rows for a game and optionally performs MERGE into Snowflake.
    """
    game_id = tstats["gameId"].iloc[0]
    team_file, player_file = _local_file_paths(endpoint, season)

    os.makedirs(os.path.dirname(team_file), exist_ok=True)
    os.makedirs(os.path.dirname(player_file), exist_ok=True)

    # --- TEAMS CSV ---
    if os.path.exists(team_file):
        existing = pd.read_csv(team_file, dtype={"gameId": str})
        if overwrite:
            existing = existing[existing["gameId"] != game_id]
        pd.concat([existing, tstats], ignore_index=True).to_csv(team_file, index=False)
    else:
        tstats.to_csv(team_file, index=False)

    # --- PLAYERS CSV ---
    if os.path.exists(player_file):
        existing = pd.read_csv(player_file, dtype={"gameId": str})
        if overwrite:
            existing = existing[existing["gameId"] != game_id]
        pd.concat([existing, pstats], ignore_index=True).to_csv(player_file, index=False)
    else:
        pstats.to_csv(player_file, index=False)

    logger.info(f"Local updated: {endpoint} game {game_id}")

    if snowflake and conn is not None:
        upsert_to_snowflake(tstats.copy(), endpoint, is_team=True, conn=conn)
        upsert_to_snowflake(pstats.copy(), endpoint, is_team=False, conn=conn)


# ============================================================
#                         API UTILITIES
# ============================================================

def get_nba_season(date=None):
    """Return current NBA season string, e.g. '2023-24'."""
    date = date or datetime.today()
    year = date.year
    start = year - 1 if date.month < 7 else year
    end = start + 1
    return f"{start}-{str(end)[-2:]}"


def fetch_log():
    """Fetch the season log of NBA games or fallback to local cached log."""
    season = get_nba_season()
    gidset = set()
    try:
        result = ep.leaguegamefinder.LeagueGameFinder(season_nullable=season)
        all_games = result.get_data_frames()[0]

        rs = all_games[(all_games.SEASON_ID == "2" + season[:4]) &
                       (all_games.GAME_ID.str.startswith("002"))]

        ps = all_games[(all_games.SEASON_ID == "4" + season[:4]) &
                       (all_games.GAME_ID.str.startswith("004"))]

        cup = all_games[(all_games.SEASON_ID == "6" + season[:4]) &
                        (all_games.GAME_ID.str.startswith("006"))]

        log = pd.concat([rs, ps, cup])
        gidset = {gid.zfill(10) for gid in log["GAME_ID"]}

    except Exception as e:
        logger.error(f"Failed live fetch; loading cached log {season}: {e}")
        path = os.path.join(BASE_PATH, "data", "raw", "log", f"log{season}.csv")
        log = pd.read_csv(path, dtype={"GAME_ID": str})
        gidset = {gid.zfill(10) for gid in log["GAME_ID"]}

    return log, gidset


def get_game(func, gid):
    """Fetch one game with retry/backoff."""
    retries = 0
    max_retries = 4

    while retries < max_retries:
        try:
            game = func(game_id=gid).get_data_frames()
            return game[0], game[1]

        except (TimeoutError, Timeout, ReadTimeoutError):
            retries += 1
            delay = random.uniform(1, 3) * (2 ** retries)
            time.sleep(delay)

        except Exception as e:
            logger.error(f"Permanent fail for {gid}: {e}")
            return None, None

    logger.error(f"Max retries exceeded for {gid}")
    return None, None


# ============================================================
#                        INGESTION LOGIC
# ============================================================

def get_gid_list(conn):
    """
    Main ingestion loop:
    - Sync local missing → Snowflake
    - Sync log table
    - Fetch missing API games for each endpoint
    """
    log, season_games = fetch_log()
    season_id = log["SEASON_ID"].iloc[0]
    season = get_nba_season()

    logger.info(f"--- Syncing local data first (season: {season}) ---")

    for endpoint in ENDPOINTS:
        upload_missing_local(endpoint, season, conn)

    # --- Update LOG_TABLE ---
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT GAME_ID FROM RAW.LOG_TABLE")
        stored_logs = {r[0] for r in cur.fetchall()}

    new_logs = log[~log["GAME_ID"].isin(stored_logs)]

    if not new_logs.empty:
        write_pandas(conn, new_logs, "LOG_TABLE")
        logger.info(f"Added {len(new_logs)} new games to LOG_TABLE")

    logger.info("--- Fetching missing games from API ---")

    # --- Fetch missing games for each endpoint ---
    for endpoint in ENDPOINTS:

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT t1.GAMEID
                FROM RAW.TEAMS_{endpoint.upper()} t1
                JOIN RAW.LOG_TABLE t2 
                  ON t1.GAMEID = t2.GAME_ID
                WHERE t2.SEASON_ID = {int(season_id)}
                """
            )
            team_games = {r[0] for r in cur.fetchall()}

            cur.execute(
                f"""
                SELECT DISTINCT t1.GAMEID
                FROM RAW.PLAYERS_{endpoint.upper()} t1
                JOIN RAW.LOG_TABLE t2 
                  ON t1.GAMEID = t2.GAME_ID
                WHERE t2.SEASON_ID = {int(season_id)}
                """
            )
            player_games = {r[0] for r in cur.fetchall()}

        stored = team_games & player_games
        needed = season_games - stored

        logger.info(f"{endpoint}: {len(needed)} games missing")

        for gid in needed:
            players, teams = get_game(FD[endpoint], gid)

            if players is None:
                logger.warning(f"Could not fetch {gid} for {endpoint}")
                continue

            write_data(
                endpoint,
                season,
                teams,
                players,
                snowflake=True,
                overwrite=False,
                conn=conn
            )


# ============================================================
#                     RESCRAPE SINGLE GAME
# ============================================================
def update_log_for_game(gid, conn):
    """
    Update the log for a single game (local CSV + Snowflake LOG_TABLE)
    """
    season = get_nba_season()
    log_file = os.path.join(BASE_PATH, "data", "raw", "log", f"log{season}.csv")

    # --- Fetch fresh log ---
    log, _ = fetch_log()
    game_log = log[log["GAME_ID"] == gid]
    if game_log.empty:
        logger.warning(f"No log info found for game {gid}. Skipping log update.")
        return

    # --- Update local log CSV ---
    if os.path.exists(log_file):
        local_log = pd.read_csv(log_file, dtype={"GAME_ID": str})
        local_log = local_log[local_log["GAME_ID"] != gid]  # remove old row
        combined_log = pd.concat([local_log, game_log], ignore_index=True)
    else:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        combined_log = game_log

    combined_log.to_csv(log_file, index=False)
    logger.info(f"Updated local log CSV for game {gid}")

    # --- Update Snowflake LOG_TABLE ---
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM RAW.LOG_TABLE WHERE GAME_ID = '{gid}'")
        write_pandas(conn, game_log, "LOG_TABLE")
        logger.info(f"Updated Snowflake LOG_TABLE for game {gid}")
    except Exception as e:
        logger.error(f"Failed updating LOG_TABLE in Snowflake for game {gid}: {e}")
def rescrape_single_game(gid, conn):
    """
    Re-scrape one game ID for ALL endpoints and update:
        - local CSVs (teams & players)
        - Snowflake tables (teams & players)
        - local log CSV
        - Snowflake LOG_TABLE
    """
    season = get_nba_season()
    logger.info(f"--- Re-scraping game {gid} for season {season} ---")

    # --- Update endpoints ---
    for endpoint_name in ENDPOINTS:
        func = FD[endpoint_name]

        try:
            players, teams = get_game(func, gid)
        except Exception as e:
            logger.error(f"Failed to fetch {endpoint_name} for {gid}: {e}")
            continue

        if players is None or teams is None:
            logger.warning(f"No data returned for endpoint {endpoint_name}, game {gid}. Skipping.")
            continue

        # Write both local CSVs + Snowflake MERGE (overwrite=True ensures update)
        try:
            write_data(
                endpoint_name,
                season,
                teams,
                players,
                snowflake=True,
                overwrite=True
            )
            logger.info(f"Updated game {gid} for endpoint {endpoint_name}")
        except Exception as e:
            logger.error(f"Error writing data for {endpoint_name}, game {gid}: {e}")

    # --- Update log table (local + Snowflake) ---
    try:
        update_log_for_game(gid, conn)
    except Exception as e:
        logger.error(f"Failed updating log for game {gid}: {e}")

    logger.info(f"--- Finished re-scraping game {gid} ---")

# ============================================================
#                          MAIN
# ============================================================

if __name__ == "__main__":
    conn = get_snowflake_conn()
    # get_gid_list(conn)
    gid = '0022500059'
    rescrape_single_game(gid, conn)
    conn.close()
