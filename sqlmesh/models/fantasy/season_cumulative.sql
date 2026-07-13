MODEL (
  name fantasy.season_cumulative,
  kind VIEW,
  dialect duckdb,
  description "Season-to-date cumulative totals + avg eFG/FT per player (one row per player/season)."
);

WITH current_season AS (
  SELECT seasonId FROM raw.current_season
)

SELECT
  bs.seasonId,
  bs.personId,
  MAX(bs.firstName)  AS firstName,
  MAX(bs.familyName) AS familyName,

  COUNT(*) FILTER (WHERE bs.playedFlag = 1) AS gamesPlayed,

  SUM(CASE WHEN bs.playedFlag = 1 THEN bs.minutes            END) AS minutesCumulative,
  SUM(CASE WHEN bs.playedFlag = 1 THEN bs.points             END) AS pointsCumulative,
  SUM(CASE WHEN bs.playedFlag = 1 THEN bs.assists            END) AS assistsCumulative,
  SUM(CASE WHEN bs.playedFlag = 1 THEN bs.steals             END) AS stealsCumulative,
  SUM(CASE WHEN bs.playedFlag = 1 THEN bs.blocks             END) AS blocksCumulative,
  SUM(CASE WHEN bs.playedFlag = 1 THEN bs.reboundsTotal      END) AS reboundsCumulative,
  SUM(CASE WHEN bs.playedFlag = 1 THEN bs.threePointersMade  END) AS threesCumulative,
  SUM(CASE WHEN bs.playedFlag = 1 THEN bs.doubleDouble       END) AS doubleDoubleCumulative,

  AVG(CASE WHEN bs.playedFlag = 1 THEN bs.effectiveFieldGoalPercentage END) AS avgEfgPct,
  AVG(CASE WHEN bs.playedFlag = 1 THEN bs.freeThrowsPercentage         END) AS avgFtPct

FROM fantasy.base_stats bs
WHERE bs.seasonId = (SELECT seasonId FROM current_season)
GROUP BY bs.seasonId, bs.personId;
