MODEL (
  name fantasy.averages,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column gameDate
  ),
  description "rolling season-to-date fantasy basketball averages per player"
);

WITH fb AS (
  SELECT
    seasonId,
    gameId,
    gameDate,
    firstName,
    familyName,
    personId,
    playedFlag,
    isLastGame,
    minutes,
    points,
    assists,
    steals,
    blocks,
    reboundsTotal,
    threePointersMade,
    effectiveFieldGoalPercentage,
    freeThrowsPercentage,
    doubleDouble
  FROM fantasy.base_stats
)

SELECT
  -------------------------------------------------------------------
  -- GAME / PLAYER CONTEXT
  -------------------------------------------------------------------
  seasonId,
  gameId,
  gameDate,
  firstName,
  familyName,
  personId,
  playedFlag,
  isLastGame,

  -------------------------------------------------------------------
  -- ROLLING SEASON-TO-DATE AVERAGES (ONLY WHEN PLAYER PLAYS)
  -------------------------------------------------------------------

  -- Availability
  AVG(playedFlag) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_played_flag,

  -- Minutes
  AVG(
    CASE WHEN playedFlag = 1 THEN minutes END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_minutes,

  -- Core fantasy stats
  AVG(
    CASE WHEN playedFlag = 1 THEN points END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_points,

  AVG(
    CASE WHEN playedFlag = 1 THEN assists END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_assists,

  AVG(
    CASE WHEN playedFlag = 1 THEN steals END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_steals,

  AVG(
    CASE WHEN playedFlag = 1 THEN blocks END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_blocks,

  AVG(
    CASE WHEN playedFlag = 1 THEN reboundsTotal END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_total_rebounds,

  AVG(
    CASE WHEN playedFlag = 1 THEN threePointersMade END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_three_pointers_made,

  -- Efficiency
  AVG(
    CASE WHEN playedFlag = 1 THEN effectiveFieldGoalPercentage END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_effective_field_goal_percentage,

  AVG(
    CASE WHEN playedFlag = 1 THEN freeThrowsPercentage END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_free_throw_percentage,

  -- Double-double rate
  AVG(
    CASE WHEN playedFlag = 1 THEN doubleDouble END
  ) OVER (
    PARTITION BY personId, seasonId
    ORDER BY CAST(gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS average_double_double_rate

FROM fb;
