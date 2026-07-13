MODEL (
  name raw.teams_traditional_totals,
  kind VIEW,
  dialect duckdb
);

SELECT
  gameid,
  teamid,
  teamcity,
  teamname,
  teamtricode,
  teamslug,

  MAX(minutes) AS minutes,

  SUM(COALESCE(TRY_CAST(fieldgoalsmade AS DOUBLE), 0))         AS fieldgoalsmade,
  SUM(COALESCE(TRY_CAST(fieldgoalsattempted AS DOUBLE), 0))    AS fieldgoalsattempted,
  AVG(TRY_CAST(fieldgoalspercentage AS DOUBLE))                AS fieldgoalspercentage,

  SUM(COALESCE(TRY_CAST(threepointersmade AS DOUBLE), 0))      AS threepointersmade,
  SUM(COALESCE(TRY_CAST(threepointersattempted AS DOUBLE), 0)) AS threepointersattempted,
  AVG(TRY_CAST(threepointerspercentage AS DOUBLE))             AS threepointerspercentage,

  SUM(COALESCE(TRY_CAST(freethrowsmade AS DOUBLE), 0))         AS freethrowsmade,
  SUM(COALESCE(TRY_CAST(freethrowsattempted AS DOUBLE), 0))    AS freethrowsattempted,
  AVG(TRY_CAST(freethrowspercentage AS DOUBLE))                AS freethrowspercentage,

  SUM(COALESCE(TRY_CAST(reboundsoffensive AS DOUBLE), 0))      AS reboundsoffensive,
  SUM(COALESCE(TRY_CAST(reboundsdefensive AS DOUBLE), 0))      AS reboundsdefensive,
  SUM(COALESCE(TRY_CAST(reboundstotal AS DOUBLE), 0))          AS reboundstotal,

  SUM(COALESCE(TRY_CAST(assists AS DOUBLE), 0))                AS assists,
  SUM(COALESCE(TRY_CAST(steals AS DOUBLE), 0))                 AS steals,
  SUM(COALESCE(TRY_CAST(blocks AS DOUBLE), 0))                 AS blocks,
  SUM(COALESCE(TRY_CAST(turnovers AS DOUBLE), 0))              AS turnovers,
  SUM(COALESCE(TRY_CAST(foulspersonal AS DOUBLE), 0))          AS foulspersonal,
  SUM(COALESCE(TRY_CAST(points AS DOUBLE), 0))                 AS points

FROM raw.teams_traditional
GROUP BY
  gameid, teamid, teamcity, teamname, teamtricode, teamslug;
