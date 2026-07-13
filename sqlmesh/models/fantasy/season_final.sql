MODEL (
  name fantasy.season_final,
  kind VIEW,
  dialect duckdb,
  description "One row per player: season-to-date averages, season cumulative totals, and z-scores on both."
);

WITH current_season AS (
  SELECT seasonId
  FROM raw.current_season
),

-- averages is per-game grain; keep only season-to-date snapshot
avg AS (
  SELECT *
  FROM fantasy.averages
  WHERE seasonId = (SELECT seasonId FROM current_season)
  AND isLastGame = 1
),

zavg AS (
  SELECT *
  FROM fantasy.zScoresAverages
  WHERE seasonId = (SELECT seasonId FROM current_season)
),

cum AS (
  SELECT *
  FROM fantasy.season_cumulative
  WHERE seasonId = (SELECT seasonId FROM current_season)
),

zcum AS (
  SELECT *
  FROM fantasy.zScoresCumulative
  WHERE seasonId = (SELECT seasonId FROM current_season)
)

SELECT
  COALESCE(avg.seasonId, zavg.seasonId, cum.seasonId, zcum.seasonId) AS seasonId,
  COALESCE(avg.personId, zavg.personId, cum.personId, zcum.personId) AS personId,

  COALESCE(avg.firstName, zavg.firstName, cum.firstName, zcum.firstName) AS firstName,
  COALESCE(avg.familyName, zavg.familyName, cum.familyName, zcum.familyName) AS familyName,

  /* ---------- averages (per game) ---------- */
  avg.average_minutes             AS avg_minutes,
  avg.average_points              AS avg_points,
  avg.average_assists             AS avg_assists,
  avg.average_total_rebounds      AS avg_rebounds,
  avg.average_steals              AS avg_steals,
  avg.average_blocks              AS avg_blocks,
  avg.average_three_pointers_made AS avg_threes,
  avg.average_effective_field_goal_percentage AS avg_efg_pct,
  avg.average_free_throw_percentage           AS avg_ft_pct,

  /* ---------- cumulative totals (raw) ---------- */
  cum.gamesPlayed,
  cum.minutesCumulative      AS cum_minutes,
  cum.pointsCumulative       AS cum_points,
  cum.assistsCumulative      AS cum_assists,
  cum.reboundsCumulative     AS cum_rebounds,
  cum.stealsCumulative       AS cum_steals,
  cum.blocksCumulative       AS cum_blocks,
  cum.threesCumulative       AS cum_threes,
  cum.doubleDoubleCumulative AS cum_double_double,
  cum.avgEfgPct              AS cum_avg_efg_pct,
  cum.avgFtPct               AS cum_avg_ft_pct,

  /* ---------- z-scores on averages ---------- */
  zavg.pointsZScore       AS zavg_points,
  zavg.assistsZScore      AS zavg_assists,
  zavg.reboundsZScore     AS zavg_rebounds,
  zavg.stealsZScore       AS zavg_steals,
  zavg.blocksZScore       AS zavg_blocks,
  zavg.threesZScore       AS zavg_threes,
  zavg.doubleDoubleZScore AS zavg_double_double,
  zavg.efgPctZScore       AS zavg_efg_pct,
  zavg.ftPctZScore        AS zavg_ft_pct,
  zavg.totalZ             AS zavg_total,

  /* ---------- z-scores on cumulative ---------- */
  zcum.minutesZScore      AS zcum_minutes,
  zcum.pointsZScore       AS zcum_points,
  zcum.assistsZScore      AS zcum_assists,
  zcum.reboundsZScore     AS zcum_rebounds,
  zcum.stealsZScore       AS zcum_steals,
  zcum.blocksZScore       AS zcum_blocks,
  zcum.threesZScore       AS zcum_threes,
  zcum.doubleDoubleZScore AS zcum_double_double,
  zcum.efgPctZScore       AS zcum_efg_pct,
  zcum.ftPctZScore        AS zcum_ft_pct,
  zcum.totalZ             AS zcum_total

FROM avg
FULL OUTER JOIN zavg
  ON avg.seasonId = zavg.seasonId
 AND avg.personId = zavg.personId
FULL OUTER JOIN cum
  ON COALESCE(avg.seasonId, zavg.seasonId) = cum.seasonId
 AND COALESCE(avg.personId, zavg.personId) = cum.personId
FULL OUTER JOIN zcum
  ON COALESCE(avg.seasonId, zavg.seasonId, cum.seasonId) = zcum.seasonId
 AND COALESCE(avg.personId, zavg.personId, cum.personId) = zcum.personId
;
