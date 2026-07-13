# duckdb_sink.py
import os
from typing import List, Set, Tuple, Optional

import duckdb
import pandas as pd


DUCKDB_SCHEMA = "raw"


def get_duckdb_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute("PRAGMA threads=4;")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {DUCKDB_SCHEMA};")
    return conn


def _df_cols_lower(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _table_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = ? AND table_name = ?
    LIMIT 1
    """
    return conn.execute(q, [schema, table]).fetchone() is not None


def _get_columns(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> List[str]:
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = ? AND table_name = ?
    ORDER BY ordinal_position
    """
    return [r[0] for r in conn.execute(q, [schema, table]).fetchall()]


def _create_table_all_text(conn: duckdb.DuckDBPyConnection, schema: str, table: str, cols: List[str]) -> None:
    col_ddl = ", ".join([f'"{c}" TEXT' for c in cols])
    conn.execute(f'CREATE TABLE IF NOT EXISTS {schema}."{table}" ({col_ddl});')


def _add_missing_cols_as_text(conn: duckdb.DuckDBPyConnection, schema: str, table: str, incoming_cols: List[str]) -> None:
    existing = set(_get_columns(conn, schema, table))
    for c in incoming_cols:
        if c not in existing:
            conn.execute(f'ALTER TABLE {schema}."{table}" ADD COLUMN "{c}" TEXT;')


def upsert_delete_insert(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    schema: str,
    table: str,
    key_cols: List[str],
) -> None:
    """
    Schema-drift-tolerant upsert for DuckDB raw tables:
      - store all raw columns as TEXT
      - auto-add new columns as they appear
      - upsert using temp table + DELETE/INSERT by natural keys
    """
    if df is None or df.empty:
        return

    df = _df_cols_lower(df)
    key_cols = [k.lower() for k in key_cols]

    for k in key_cols:
        if k not in df.columns:
            raise ValueError(f"Missing key column '{k}' for {schema}.{table}. df cols={list(df.columns)}")

    if not _table_exists(conn, schema, table):
        _create_table_all_text(conn, schema, table, list(df.columns))

    _add_missing_cols_as_text(conn, schema, table, list(df.columns))

    tmp = f"__tmp_{table}"
    conn.execute(f'DROP TABLE IF EXISTS {schema}."{tmp}";')

    conn.register("incoming_df", df)
    conn.execute(f'CREATE TABLE {schema}."{tmp}" AS SELECT * FROM incoming_df;')
    conn.unregister("incoming_df")

    join_cond = " AND ".join([f't."{k}" = s."{k}"' for k in key_cols])

    conn.execute("BEGIN;")
    try:
        conn.execute(
            f'''
            DELETE FROM {schema}."{table}" t
            USING {schema}."{tmp}" s
            WHERE {join_cond};
            '''
        )

        target_cols = _get_columns(conn, schema, table)
        common_cols = [c for c in target_cols if c in df.columns]
        col_list = ", ".join([f'"{c}"' for c in common_cols])

        conn.execute(
            f'''
            INSERT INTO {schema}."{table}" ({col_list})
            SELECT {col_list} FROM {schema}."{tmp}";
            '''
        )

        conn.execute(f'DROP TABLE IF EXISTS {schema}."{tmp}";')
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def table_info(endpoint: str, is_team: bool) -> Tuple[str, str, List[str]]:
    """
    DuckDB naming convention:
      raw.teams_<endpoint>
      raw.players_<endpoint>
      raw.log_table
    """
    schema = DUCKDB_SCHEMA
    if is_team:
        table = f"teams_{endpoint.lower()}"
        if endpoint.lower() == "traditional":
            key_cols = ["gameid", "teamid", "startersbench"]
        else:
            key_cols = ["gameid", "teamid"]
    else:
        table = f"players_{endpoint.lower()}"
        key_cols = ["gameid", "teamid", "personid"]
    return schema, table, key_cols


def existing_gameids(conn: duckdb.DuckDBPyConnection, endpoint: str) -> Set[str]:
    """
    Consider game stored if BOTH teams_<ep> and players_<ep> have it.
    """
    ts, tt, _ = table_info(endpoint, is_team=True)
    ps, pt, _ = table_info(endpoint, is_team=False)

    team_games: Set[str] = set()
    player_games: Set[str] = set()

    try:
        team_games = {r[0] for r in conn.execute(f'SELECT DISTINCT gameid FROM {ts}."{tt}"').fetchall()}
    except Exception:
        pass

    try:
        player_games = {r[0] for r in conn.execute(f'SELECT DISTINCT gameid FROM {ps}."{pt}"').fetchall()}
    except Exception:
        pass

    return team_games & player_games


def upsert_log(conn: duckdb.DuckDBPyConnection, log_df: pd.DataFrame) -> None:
    """
    Store NBA log (team-level rows) to raw.log_table
    Key: (game_id, team_id)
    """
    if log_df is None or log_df.empty:
        return

    df = log_df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    upsert_delete_insert(conn, df, schema=DUCKDB_SCHEMA, table="log_table", key_cols=["game_id", "team_id"])

def df_for_gameids(conn: duckdb.DuckDBPyConnection, schema: str, table: str, gameids: List[str]) -> pd.DataFrame:
    if not gameids:
        return pd.DataFrame()
    gids = [str(g) for g in gameids]
    # build VALUES list safely
    values = ",".join([f"('{g}')" for g in gids])
    sql = f"""
    WITH gids(gameid) AS (VALUES {values})
    SELECT t.*
    FROM {schema}."{table}" t
    JOIN gids g ON t.gameid = g.gameid
    """
    return conn.execute(sql).df()