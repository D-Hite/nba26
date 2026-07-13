MODEL (
  name raw.current_season,
  kind VIEW,
  dialect duckdb
);

SELECT
  MAX(CAST(season_id AS INTEGER)) AS seasonId
FROM raw.log_table
WHERE season_id LIKE '2%';
