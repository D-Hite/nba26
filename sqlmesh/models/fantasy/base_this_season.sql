MODEL (
  name fantasy.base_stats_current_season,
  kind VIEW,
  description "Fantasy basketball stats for the current season (slice of fantasy.base_stats)"
);
SELECT
  *
FROM fantasy.base_stats
WHERE seasonId = (
  SELECT seasonId
  FROM raw.current_season
);
