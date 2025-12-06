MODEL (
  name fantasy.zScoresAverages,
  kind VIEW,
  description "Fantasy basketball cumulative season averages matched with season-level Z-scores."
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

        average_minutes                AS minutesCumulative,
        average_points                 AS pointsCumulative,
        average_assists                AS assistsCumulative,
        average_steals                 AS stealsCumulative,
        average_blocks                 AS blocksCumulative,
        average_total_rebounds         AS reboundsCumulative,
        average_three_pointers_made    AS threesCumulative,
        average_double_double_rate     AS doubleDoubleCumulative,
        average_effective_field_goal_percentage AS avgEfgPct,
        average_free_throw_percentage  AS avgFtPct

    FROM fantasy.averages_current
),
zScores AS (
    SELECT
        seasonId,
        personId,
        firstName,
        familyName,
        gameDate,

        (minutesCumulative - AVG(minutesCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(minutesCumulative) OVER (PARTITION BY seasonId) AS minutesZScore,

        (pointsCumulative - AVG(pointsCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(pointsCumulative) OVER (PARTITION BY seasonId) AS pointsZScore,

        (assistsCumulative - AVG(assistsCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(assistsCumulative) OVER (PARTITION BY seasonId) AS assistsZScore,

        (stealsCumulative - AVG(stealsCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(stealsCumulative) OVER (PARTITION BY seasonId) AS stealsZScore,

        (blocksCumulative - AVG(blocksCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(blocksCumulative) OVER (PARTITION BY seasonId) AS blocksZScore,

        (reboundsCumulative - AVG(reboundsCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(reboundsCumulative) OVER (PARTITION BY seasonId) AS reboundsZScore,

        (threesCumulative - AVG(threesCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(threesCumulative) OVER (PARTITION BY seasonId) AS threesZScore,

        (doubleDoubleCumulative - AVG(doubleDoubleCumulative) OVER (PARTITION BY seasonId))
            / STDDEV(doubleDoubleCumulative) OVER (PARTITION BY seasonId) AS doubleDoubleZScore,

        (avgEfgPct - AVG(avgEfgPct) OVER (PARTITION BY seasonId))
            / STDDEV(avgEfgPct) OVER (PARTITION BY seasonId) AS efgPctZScore,

        (avgFtPct - AVG(avgFtPct) OVER (PARTITION BY seasonId))
            / STDDEV(avgFtPct) OVER (PARTITION BY seasonId) AS ftPctZScore

    FROM cumulative
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
FROM zScores
ORDER BY totalZ DESC;
