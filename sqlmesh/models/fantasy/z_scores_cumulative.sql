MODEL (
  name fantasy.zScoresCumulative,
  kind VIEW,
  dialect duckdb,
  description "Fantasy basketball cumulative Z-scores computed from fantasy.season_cumulative."
);

WITH base AS (
  SELECT *
  FROM fantasy.season_cumulative
),

z AS (
  SELECT
    seasonId,
    personId,
    firstName,
    familyName,

    minutesCumulative,
    pointsCumulative,
    assistsCumulative,
    stealsCumulative,
    blocksCumulative,
    reboundsCumulative,
    threesCumulative,
    doubleDoubleCumulative,
    avgEfgPct,
    avgFtPct,

    (minutesCumulative - AVG(minutesCumulative) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(minutesCumulative) OVER (PARTITION BY seasonId), 0) AS minutesZScore,

    (pointsCumulative - AVG(pointsCumulative) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(pointsCumulative) OVER (PARTITION BY seasonId), 0) AS pointsZScore,

    (assistsCumulative - AVG(assistsCumulative) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(assistsCumulative) OVER (PARTITION BY seasonId), 0) AS assistsZScore,

    (stealsCumulative - AVG(stealsCumulative) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(stealsCumulative) OVER (PARTITION BY seasonId), 0) AS stealsZScore,

    (blocksCumulative - AVG(blocksCumulative) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(blocksCumulative) OVER (PARTITION BY seasonId), 0) AS blocksZScore,

    (reboundsCumulative - AVG(reboundsCumulative) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(reboundsCumulative) OVER (PARTITION BY seasonId), 0) AS reboundsZScore,

    (threesCumulative - AVG(threesCumulative) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(threesCumulative) OVER (PARTITION BY seasonId), 0) AS threesZScore,

    (doubleDoubleCumulative - AVG(doubleDoubleCumulative) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(doubleDoubleCumulative) OVER (PARTITION BY seasonId), 0) AS doubleDoubleZScore,

    (avgEfgPct - AVG(avgEfgPct) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(avgEfgPct) OVER (PARTITION BY seasonId), 0) AS efgPctZScore,

    (avgFtPct - AVG(avgFtPct) OVER (PARTITION BY seasonId))
      / NULLIF(STDDEV(avgFtPct) OVER (PARTITION BY seasonId), 0) AS ftPctZScore
  FROM base
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
FROM z;
