MODEL (
  name fantasy.zScores_last10,
  kind VIEW,
  description 'Rolling last 10 game averages with season-level Z-scores.'
);

WITH last10 AS (
    SELECT
        seasonId,
        gameId,
        gameDate,
        firstName,
        familyName,
        personId,
        playedFlag,
        isLastGame,
        games_in_window,

        minutes_last10,
        points_last10,
        assists_last10,
        steals_last10,
        blocks_last10,
        rebounds_last10,
        threePointersMade_last10,
        doubleDouble_rate_last10,
        effectiveFieldGoalPercentage_last10,
        freeThrowsPercentage_last10
    FROM fantasy.averages_last_10_current
),

z AS (
    SELECT
        seasonId,
        personId,
        firstName,
        familyName,
        gameDate,

        (minutes_last10 - AVG(minutes_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(minutes_last10) OVER (PARTITION BY seasonId), 0) AS minutesZScore,

        (points_last10 - AVG(points_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(points_last10) OVER (PARTITION BY seasonId), 0) AS pointsZScore,

        (assists_last10 - AVG(assists_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(assists_last10) OVER (PARTITION BY seasonId), 0) AS assistsZScore,

        (steals_last10 - AVG(steals_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(steals_last10) OVER (PARTITION BY seasonId), 0) AS stealsZScore,

        (blocks_last10 - AVG(blocks_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(blocks_last10) OVER (PARTITION BY seasonId), 0) AS blocksZScore,

        (rebounds_last10 - AVG(rebounds_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(rebounds_last10) OVER (PARTITION BY seasonId), 0) AS reboundsZScore,

        (threePointersMade_last10 - AVG(threePointersMade_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(threePointersMade_last10) OVER (PARTITION BY seasonId), 0) AS threesZScore,

        (doubleDouble_rate_last10 - AVG(doubleDouble_rate_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(doubleDouble_rate_last10) OVER (PARTITION BY seasonId), 0) AS doubleDoubleZScore,

        (effectiveFieldGoalPercentage_last10 - AVG(effectiveFieldGoalPercentage_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(effectiveFieldGoalPercentage_last10) OVER (PARTITION BY seasonId), 0) AS efgPctZScore,

        (freeThrowsPercentage_last10 - AVG(freeThrowsPercentage_last10) OVER (PARTITION BY seasonId))
            / NULLIF(STDDEV(freeThrowsPercentage_last10) OVER (PARTITION BY seasonId), 0) AS ftPctZScore
    FROM last10
)

SELECT
    *,
    pointsZScore +
    assistsZScore +
    stealsZScore +
    blocksZScore +
    reboundsZScore +
    threesZScore +
    doubleDoubleZScore +
    efgPctZScore +
    ftPctZScore AS totalZ
FROM z
ORDER BY totalZ DESC;
