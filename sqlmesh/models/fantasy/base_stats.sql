MODEL (
  name fantasy.base_stats,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column gameDate
  ),
  description "fantasy basketball stats"
);
SELECT
  seasonId,
  gameId,
  gameDate,
  firstName,
  familyName,
  personId,
  playedFlag,
  isLastGame,
  MINUTES,
  points,
  assists,
  steals,
  blocks,
  reboundsTotal,
  threePointersMade,
  effectiveFieldGoalPercentage,
  CASE WHEN freeThrowsAttempted = 0 THEN NULL ELSE freeThrowsPercentage END as freeThrowsPercentage,
  CASE WHEN 
    points >= 10 AND (assists >= 10 OR blocks >= 10 OR steals >= 10 OR reboundsTotal >= 10)
    OR assists >= 10 AND (blocks >= 10 OR steals >= 10 OR reboundsTotal >= 10)
    OR reboundsTotal >= 10 AND (blocks >= 10 OR steals >= 10 OR assists >= 10)
  THEN 1
  ELSE 0
  END AS doubleDouble
    
FROM BASE.players_processed