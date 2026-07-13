MODEL (
  name base.lines_table,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column gameDate
  ),
  dialect duckdb
);

WITH raw_data AS (
  SELECT
    -- raw.lines_table.date is BIGINT like 20260110
    TRY_CAST(
      SUBSTR(CAST(date AS VARCHAR), 1, 4) || '-' ||
      SUBSTR(CAST(date AS VARCHAR), 5, 2) || '-' ||
      SUBSTR(CAST(date AS VARCHAR), 7, 2)
      AS DATE
    ) AS p_date,

    team,
    TRY_CAST(line AS DOUBLE)  AS line,
    TRY_CAST(total AS DOUBLE) AS total
  FROM raw.lines_table
),

log_typed AS (
  SELECT
    game_id,
    TRY_CAST(game_date AS DATE) AS game_date,
    team_abbreviation,
    team_name
  FROM raw.log_table
)

SELECT
  lt.game_id AS gameId,
  lt.game_date AS gameDate,
  lt.team_abbreviation AS teamTriCode,
  rd.line AS line,
  rd.total AS ou
FROM raw_data rd
JOIN raw.line_team_mapping mp
  ON mp.raw_data_team_name = rd.team
JOIN log_typed lt
  ON lt.game_date = rd.p_date
 AND lt.team_name = mp.log_table_team_name
WHERE lt.game_date IS NOT NULL
  AND rd.p_date IS NOT NULL;
