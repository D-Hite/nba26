MODEL (
  name fantasy.averages_last_10,
  kind VIEW,
  description 'Rolling last 10 game averages per player/season/game.'
);

WITH per_game AS (
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
),

rolling AS (
    SELECT
        seasonId,
        gameId,
        gameDate,
        firstName,
        familyName,
        personId,
        playedFlag,
        isLastGame,

        -- rolling last 10 averages
        AVG(minutes) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS minutes_last10,

        AVG(points) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS points_last10,

        AVG(assists) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS assists_last10,

        AVG(steals) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS steals_last10,

        AVG(blocks) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS blocks_last10,

        AVG(reboundsTotal) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS rebounds_last10,

        AVG(threePointersMade) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS threePointersMade_last10,

        AVG(doubleDouble) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS doubleDouble_rate_last10,

        AVG(effectiveFieldGoalPercentage) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS effectiveFieldGoalPercentage_last10,

        AVG(freeThrowsPercentage) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS freeThrowsPercentage_last10,

        COUNT(*) OVER (
            PARTITION BY seasonId, personId
            ORDER BY gameDate
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS games_in_window
    FROM per_game
)

SELECT *
FROM rolling
-- If you ONLY want rows with a full 10-game window:
WHERE games_in_window >= 10;
