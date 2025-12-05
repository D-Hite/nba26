MODEL (
  name fantasy.zScoresCumulative,
  kind VIEW,
  description """Fantasy basketball cumulative counting-stat Z-scores.
                 One row per player per season (last game of season)."""                 
);

WITH cumulative AS (
    SELECT
        seasonId,
        gameId,
        gameDate,
        firstName,
        familyName,
        personId,
        playedFlag,
        isLastGame,

        SUM(minutes) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS minutesCumulative,

        SUM(points) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS pointsCumulative,

        SUM(assists) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS assistsCumulative,

        SUM(steals) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS stealsCumulative,

        SUM(blocks) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS blocksCumulative,

        SUM(reboundsTotal) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS reboundsCumulative,

        SUM(threePointersMade) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS threesCumulative,

        SUM(doubleDouble) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS doubleDoubleCumulative,

        AVG(
            CASE WHEN playedFlag = 1 THEN effectiveFieldGoalPercentage ELSE NULL END
        ) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avgEfgPct,

        AVG(
            CASE WHEN playedFlag = 1 THEN freeThrowsPercentage ELSE NULL END
        ) OVER (
            PARTITION BY personId, seasonId
            ORDER BY CAST(gameDate AS DATE)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avgFtPct

    FROM fantasy.base_stats
),

seasonTotals AS (
    SELECT
        seasonId,
        gameId,
        gameDate,
        firstName,
        familyName,
        personId,
        playedFlag,
        isLastGame,
        minutesCumulative,
        pointsCumulative,
        assistsCumulative,
        stealsCumulative,
        blocksCumulative,
        reboundsCumulative,
        threesCumulative,
        doubleDoubleCumulative,
        avgEfgPct,
        avgFtPct
    FROM cumulative
    WHERE isLastGame = 1
),

zScores AS (
    SELECT
        seasonId,
        personId,
        firstName,
        familyName,
        gameDate,

        (minutesCumulative - AVG(minutesCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(minutesCumulative) OVER (PARTITION BY seasonId)
            AS minutesZScore,

        (pointsCumulative - AVG(pointsCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(pointsCumulative) OVER (PARTITION BY seasonId)
            AS pointsZScore,

        (assistsCumulative - AVG(assistsCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(assistsCumulative) OVER (PARTITION BY seasonId)
            AS assistsZScore,

        (stealsCumulative - AVG(stealsCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(stealsCumulative) OVER (PARTITION BY seasonId)
            AS stealsZScore,

        (blocksCumulative - AVG(blocksCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(blocksCumulative) OVER (PARTITION BY seasonId)
            AS blocksZScore,

        (reboundsCumulative - AVG(reboundsCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(reboundsCumulative) OVER (PARTITION BY seasonId)
            AS reboundsZScore,

        (threesCumulative - AVG(threesCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(threesCumulative) OVER (PARTITION BY seasonId)
            AS threesZScore,

        (doubleDoubleCumulative - AVG(doubleDoubleCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(doubleDoubleCumulative) OVER (PARTITION BY seasonId)
            AS doubleDoubleZScore,

        (avgEfgPct - AVG(avgEfgPct) OVER (PARTITION BY seasonId))
            / STDDEV(avgEfgPct) OVER (PARTITION BY seasonId)
            AS efgPctZScore,

        (avgFtPct - AVG(avgFtPct) OVER (PARTITION BY seasonId))
            / STDDEV(avgFtPct) OVER (PARTITION BY seasonId)
            AS ftPctZScore

    FROM seasonTotals
)

SELECT
    seasonId,
    personId,
    firstName,
    familyName,
    gameDate,
    minutesZScore,
    pointsZScore,
    assistsZScore,
    stealsZScore,
    blocksZScore,
    reboundsZScore,
    threesZScore,
    doubleDoubleZScore,
    efgPctZScore,
    ftPctZScore,

    pointsZScore +
    assistsZScore +
    stealsZScore +
    blocksZScore +
    reboundsZScore +
    threesZScore +
    doubleDoubleZScore +
    efgPctZScore +
    ftPctZScore AS totalZ
FROM zScores
ORDER BY totalZ DESC;
