MODEL (
  name fantasy.weekly_stats,
  kind VIEW,
  dialect duckdb
);

WITH base AS (
  SELECT
    bs.seasonId,
    bs.gameId,
    bs.gameDate,
    bs.firstName,
    bs.familyName,
    bs.personId,
    bs.playedFlag,
    bs.isLastGame,
    bs.minutes,
    bs.points,
    bs.assists,
    bs.steals,
    bs.blocks,
    bs.reboundsTotal,
    bs.threePointersMade,
    bs.effectiveFieldGoalPercentage,
    bs.freeThrowsPercentage,
    bs.doubleDouble
  FROM fantasy.base_stats bs
  WHERE bs.seasonId = (SELECT seasonId FROM raw.current_season)
),

withWeeks AS (
  SELECT
    base.*,
    CAST(date_trunc('week', CAST(base.gameDate AS DATE)) AS DATE) AS weekStartDate,
    CAST(date_trunc('week', CAST(base.gameDate AS DATE)) + INTERVAL '6 day' AS DATE) AS weekEndDate
  FROM base
),

typed AS (
  SELECT
    withWeeks.*,

    TRY_CAST(minutes AS DOUBLE) AS minutes_num,
    TRY_CAST(points AS DOUBLE) AS points_num,
    TRY_CAST(assists AS DOUBLE) AS assists_num,
    TRY_CAST(reboundsTotal AS DOUBLE) AS rebounds_num,
    TRY_CAST(steals AS DOUBLE) AS steals_num,
    TRY_CAST(blocks AS DOUBLE) AS blocks_num,
    TRY_CAST(threePointersMade AS DOUBLE) AS threes_num,
    TRY_CAST(doubleDouble AS DOUBLE) AS doubleDouble_num
  FROM withWeeks
),

weeklyAgg AS (
  SELECT
    personId,
    firstName,
    familyName,
    seasonId,
    weekStartDate,
    weekEndDate,
    CONCAT(seasonId, '_', strftime(weekStartDate, '%G-%V')) AS seasonWeekId,

    COUNT(*) FILTER (WHERE playedFlag = 1) AS gamesPlayedWeek,

    SUM(CASE WHEN playedFlag = 1 THEN minutes_num      END) AS minutesSum,
    SUM(CASE WHEN playedFlag = 1 THEN points_num       END) AS pointsSum,
    SUM(CASE WHEN playedFlag = 1 THEN assists_num      END) AS assistsSum,
    SUM(CASE WHEN playedFlag = 1 THEN rebounds_num     END) AS reboundsSum,
    SUM(CASE WHEN playedFlag = 1 THEN steals_num       END) AS stealsSum,
    SUM(CASE WHEN playedFlag = 1 THEN blocks_num       END) AS blocksSum,
    SUM(CASE WHEN playedFlag = 1 THEN threes_num       END) AS threesSum,
    SUM(CASE WHEN playedFlag = 1 THEN doubleDouble_num END) AS doubleDoubleSum,

    AVG(CASE WHEN playedFlag = 1 THEN minutes_num      END) AS minutesAvg,
    AVG(CASE WHEN playedFlag = 1 THEN points_num       END) AS pointsAvg,
    AVG(CASE WHEN playedFlag = 1 THEN assists_num      END) AS assistsAvg,
    AVG(CASE WHEN playedFlag = 1 THEN rebounds_num     END) AS reboundsAvg,
    AVG(CASE WHEN playedFlag = 1 THEN steals_num       END) AS stealsAvg,
    AVG(CASE WHEN playedFlag = 1 THEN blocks_num       END) AS blocksAvg,
    AVG(CASE WHEN playedFlag = 1 THEN threes_num       END) AS threesAvg,

    AVG(CASE WHEN playedFlag = 1 THEN TRY_CAST(effectiveFieldGoalPercentage AS DOUBLE) END) AS efgPctAvg,
    AVG(CASE WHEN playedFlag = 1 THEN TRY_CAST(freeThrowsPercentage         AS DOUBLE) END) AS ftPctAvg
  FROM typed
  GROUP BY
    personId, firstName, familyName, seasonId, weekStartDate, weekEndDate
),

week_index AS (
  SELECT
    seasonId,
    weekStartDate,
    DENSE_RANK() OVER (PARTITION BY seasonId ORDER BY weekStartDate) AS weekNumber
  FROM (
    SELECT DISTINCT seasonId, weekStartDate
    FROM weeklyAgg
  )
),

weeklyZ AS (
  SELECT
    wa.personId,
    wa.firstName,
    wa.familyName,
    wa.seasonId,
    wi.weekNumber,
    wa.weekStartDate,
    wa.weekEndDate,
    wa.seasonWeekId,
    wa.gamesPlayedWeek,

    wa.minutesSum,
    wa.pointsSum,
    wa.assistsSum,
    wa.reboundsSum,
    wa.stealsSum,
    wa.blocksSum,
    wa.threesSum,
    wa.doubleDoubleSum,

    wa.minutesAvg,
    wa.pointsAvg,
    wa.assistsAvg,
    wa.reboundsAvg,
    wa.stealsAvg,
    wa.blocksAvg,
    wa.threesAvg,
    wa.efgPctAvg,
    wa.ftPctAvg,

    (wa.pointsSum  - AVG(wa.pointsSum)  OVER (PARTITION BY wa.seasonId, wa.weekStartDate))
      / NULLIF(STDDEV_SAMP(wa.pointsSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate), 0) AS pointsZScoreWeek,

    (wa.assistsSum - AVG(wa.assistsSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate))
      / NULLIF(STDDEV_SAMP(wa.assistsSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate), 0) AS assistsZScoreWeek,

    (wa.reboundsSum - AVG(wa.reboundsSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate))
      / NULLIF(STDDEV_SAMP(wa.reboundsSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate), 0) AS reboundsZScoreWeek,

    (wa.stealsSum - AVG(wa.stealsSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate))
      / NULLIF(STDDEV_SAMP(wa.stealsSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate), 0) AS stealsZScoreWeek,

    (wa.blocksSum - AVG(wa.blocksSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate))
      / NULLIF(STDDEV_SAMP(wa.blocksSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate), 0) AS blocksZScoreWeek,

    (wa.threesSum - AVG(wa.threesSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate))
      / NULLIF(STDDEV_SAMP(wa.threesSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate), 0) AS threesZScoreWeek,

    (wa.doubleDoubleSum - AVG(wa.doubleDoubleSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate))
      / NULLIF(STDDEV_SAMP(wa.doubleDoubleSum) OVER (PARTITION BY wa.seasonId, wa.weekStartDate), 0) AS doubleDoubleZScoreWeek
  FROM weeklyAgg wa
  JOIN week_index wi
    ON wa.seasonId = wi.seasonId
   AND wa.weekStartDate = wi.weekStartDate
)

SELECT
  personId,
  firstName,
  familyName,
  seasonId,
  weekNumber,
  weekStartDate,
  weekEndDate,
  seasonWeekId,
  gamesPlayedWeek,

  minutesSum,
  pointsSum,
  assistsSum,
  reboundsSum,
  stealsSum,
  blocksSum,
  threesSum,
  doubleDoubleSum,

  minutesAvg,
  pointsAvg,
  assistsAvg,
  reboundsAvg,
  stealsAvg,
  blocksAvg,
  threesAvg,
  efgPctAvg,
  ftPctAvg,

  pointsZScoreWeek,
  assistsZScoreWeek,
  reboundsZScoreWeek,
  stealsZScoreWeek,
  blocksZScoreWeek,
  threesZScoreWeek,
  doubleDoubleZScoreWeek,

  pointsZScoreWeek + assistsZScoreWeek + reboundsZScoreWeek + stealsZScoreWeek +
  blocksZScoreWeek + threesZScoreWeek + doubleDoubleZScoreWeek AS totalZWeek
FROM weeklyZ;
