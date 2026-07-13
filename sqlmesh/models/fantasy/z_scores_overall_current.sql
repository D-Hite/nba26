MODEL (
  name fantasy.zscores_overall_current,
  kind VIEW,
  dialect duckdb,
  description "Fantasy basketball combined z score table"
);
WITH current_season AS (
  SELECT MAX(seasonid) AS seasonid
  FROM nba.fantasy.zscorescumulative
),

-- latest "averages" row per player (because zscoresaverages is game-dated)
avg_latest AS (
  SELECT *
  FROM (
    SELECT
      seasonid,
      CAST(personid AS VARCHAR) AS personid,
      firstname,
      familyname,
      gamedate,

      minuteszscore,
      pointszscore,
      assistszscore,
      stealszscore,
      blockszscore,
      reboundszscore,
      threeszscore,
      doubledoublezscore,
      efgpctzscore,
      ftpctzscore,
      totalz,

      ROW_NUMBER() OVER (
        PARTITION BY seasonid, personid
        ORDER BY gamedate DESC NULLS LAST
      ) AS rn
    FROM nba.fantasy.zscoresaverages
    WHERE seasonid = (SELECT seasonid FROM current_season)
  )
  WHERE rn = 1
),

-- latest "last10" row per player
l10_latest AS (
  SELECT *
  FROM (
    SELECT
      seasonid,
      CAST(personid AS VARCHAR) AS personid,
      gamedate,
      totalz AS last_10_totalz,

      ROW_NUMBER() OVER (
        PARTITION BY seasonid, personid
        ORDER BY gamedate DESC NULLS LAST
      ) AS rn
    FROM nba.fantasy.zscores_last10
    WHERE seasonid = (SELECT seasonid FROM current_season)
  )
  WHERE rn = 1
),

-- cumulative is already per-player (no gamedate)
cum AS (
  SELECT
    seasonid,
    CAST(personid AS VARCHAR) AS personid,
    firstname,
    familyname,

    minuteszscore AS cum_minuteszscore,
    pointszscore AS cum_pointszscore,
    assistszscore AS cum_assistszscore,
    stealszscore AS cum_stealszscore,
    blockszscore AS cum_blockszscore,
    reboundszscore AS cum_reboundszscore,
    threeszscore AS cum_threeszscore,
    doubledoublezscore AS cum_doubledoublezscore,
    efgpctzscore AS cum_efgpctzscore,
    ftpctzscore AS cum_ftpctzscore,
    totalz AS cum_totalz
  FROM nba.fantasy.zscorescumulative
  WHERE seasonid = (SELECT seasonid FROM current_season)
)

SELECT
  c.seasonid,
  c.personid,

  -- prefer cumulative name, fallback to avg_latest
  COALESCE(c.firstname, a.firstname) AS firstname,
  COALESCE(c.familyname, a.familyname) AS familyname,

  -- dates so you can see freshness
  a.gamedate AS avg_gamedate,
  l.gamedate AS last10_gamedate,

  -- totals (easy to use)
  c.cum_totalz,
  a.totalz AS avg_totalz,
  l.last_10_totalz,

  -- avg category z (prefix avg_)
  a.minuteszscore      AS avg_minuteszscore,
  a.pointszscore       AS avg_pointszscore,
  a.assistszscore      AS avg_assistszscore,
  a.stealszscore       AS avg_stealszscore,
  a.blockszscore       AS avg_blockszscore,
  a.reboundszscore     AS avg_reboundszscore,
  a.threeszscore       AS avg_threeszscore,
  a.doubledoublezscore AS avg_doubledoublezscore,
  a.efgpctzscore       AS avg_efgpctzscore,
  a.ftpctzscore        AS avg_ftpctzscore,

  -- cum category z (already prefixed in cte)
  c.cum_minuteszscore,
  c.cum_pointszscore,
  c.cum_assistszscore,
  c.cum_stealszscore,
  c.cum_blockszscore,
  c.cum_reboundszscore,
  c.cum_threeszscore,
  c.cum_doubledoublezscore,
  c.cum_efgpctzscore,
  c.cum_ftpctzscore

FROM cum c
LEFT JOIN avg_latest a
  ON a.seasonid = c.seasonid
 AND a.personid = c.personid
LEFT JOIN l10_latest l
  ON l.seasonid = c.seasonid
 AND l.personid = c.personid;
