MODEL (
  name base.players_combined,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column gameDate
  ),
  dialect duckdb
);

WITH joined AS (
  SELECT DISTINCT
    -- 1) Metadata (typed)
    TRY_CAST(lt.season_id AS BIGINT) AS seasonId,
    TRY_CAST(lt.team_id AS BIGINT) AS teamId,
    lt.team_abbreviation AS teamAbbreviation,
    lt.team_name AS teamName,
    CAST(lt.game_id AS VARCHAR) AS gameId,
    TRY_CAST(lt.game_date AS DATE) AS gameDate,
    lt.matchup AS matchup,
    lt.wl AS winLoss,

    -- 2) Player identity (ids typed, strings kept)
    TRY_CAST(
      COALESCE(
        pf.personid,
        pa.personid,
        pm.personid,
        ps.personid,
        pt.personid
      ) AS BIGINT
    ) AS personId,

    COALESCE(pf.firstname, pa.firstname, pm.firstname, ps.firstname, pt.firstname) AS firstName,
    COALESCE(pf.familyname, pa.familyname, pm.familyname, ps.familyname, pt.familyname) AS familyName,
    COALESCE(pf.playerslug, pa.playerslug, pm.playerslug, ps.playerslug, pt.playerslug) AS playerSlug,
    COALESCE(pf.position, pa.position, pm.position, ps.position, pt.position) AS position,
    COALESCE(pf.comment, pa.comment, pm.comment, ps.comment, pt.comment) AS comment,
    COALESCE(pf.teamcity, pa.teamcity, pm.teamcity, ps.teamcity, pt.teamcity) AS teamCity,

    -- 3) Core box score stats (typed numeric)
    COALESCE(pf.minutes, pa.minutes, pm.minutes, ps.minutes, pt.minutes) AS minutes,

    TRY_CAST(pt.points AS DOUBLE) AS points,
    TRY_CAST(pt.fieldgoalsmade AS DOUBLE) AS fieldGoalsMade,
    TRY_CAST(pt.fieldgoalsattempted AS DOUBLE) AS fieldGoalsAttempted,
    TRY_CAST(pt.fieldgoalspercentage AS DOUBLE) AS fieldGoalsPercentage,
    TRY_CAST(pt.threepointersmade AS DOUBLE) AS threePointersMade,
    TRY_CAST(pt.threepointersattempted AS DOUBLE) AS threePointersAttempted,
    TRY_CAST(pt.threepointerspercentage AS DOUBLE) AS threePointersPercentage,
    TRY_CAST(pt.freethrowsmade AS DOUBLE) AS freeThrowsMade,
    TRY_CAST(pt.freethrowsattempted AS DOUBLE) AS freeThrowsAttempted,
    TRY_CAST(pt.freethrowspercentage AS DOUBLE) AS freeThrowsPercentage,
    TRY_CAST(pt.reboundsoffensive AS DOUBLE) AS reboundsOffensive,
    TRY_CAST(pt.reboundsdefensive AS DOUBLE) AS reboundsDefensive,
    TRY_CAST(pt.reboundstotal AS DOUBLE) AS reboundsTotal,
    TRY_CAST(pt.assists AS DOUBLE) AS assists,
    TRY_CAST(pt.steals AS DOUBLE) AS steals,

    COALESCE(TRY_CAST(pm.blocks AS DOUBLE), TRY_CAST(pt.blocks AS DOUBLE)) AS blocks,
    COALESCE(TRY_CAST(pm.foulspersonal AS DOUBLE), TRY_CAST(pt.foulspersonal AS DOUBLE)) AS foulsPersonal,

    TRY_CAST(pt.turnovers AS DOUBLE) AS turnovers,
    TRY_CAST(pt.plusminuspoints AS DOUBLE) AS plusMinusPoints,

    -- 4) Advanced metrics (typed numeric)
    TRY_CAST(pa.estimatedoffensiverating AS DOUBLE) AS estimatedOffensiveRating,
    TRY_CAST(pa.offensiverating AS DOUBLE) AS offensiveRating,
    TRY_CAST(pa.estimateddefensiverating AS DOUBLE) AS estimatedDefensiveRating,
    TRY_CAST(pa.defensiverating AS DOUBLE) AS defensiveRating,
    TRY_CAST(pa.estimatednetrating AS DOUBLE) AS estimatedNetRating,
    TRY_CAST(pa.netrating AS DOUBLE) AS netRating,
    TRY_CAST(pa.assistpercentage AS DOUBLE) AS assistPercentage,
    TRY_CAST(pa.assisttoturnover AS DOUBLE) AS assistToTurnover,
    TRY_CAST(pa.assistratio AS DOUBLE) AS assistRatio,

    COALESCE(
      TRY_CAST(pf.offensivereboundpercentage AS DOUBLE),
      TRY_CAST(pa.offensivereboundpercentage AS DOUBLE)
    ) AS offensiveReboundPercentage,

    TRY_CAST(pa.defensivereboundpercentage AS DOUBLE) AS defensiveReboundPercentage,
    TRY_CAST(pa.reboundpercentage AS DOUBLE) AS reboundPercentage,
    TRY_CAST(pf.teamturnoverpercentage AS DOUBLE) AS teamTurnoverPercentage,

    COALESCE(
      TRY_CAST(pf.effectivefieldgoalpercentage AS DOUBLE),
      TRY_CAST(pa.effectivefieldgoalpercentage AS DOUBLE)
    ) AS effectiveFieldGoalPercentage,

    TRY_CAST(pa.trueshootingpercentage AS DOUBLE) AS trueShootingPercentage,
    TRY_CAST(pa.usagepercentage AS DOUBLE) AS usagePercentage,
    TRY_CAST(pa.estimatedusagepercentage AS DOUBLE) AS estimatedUsagePercentage,
    TRY_CAST(pa.estimatedpace AS DOUBLE) AS estimatedPace,
    TRY_CAST(pa.pace AS DOUBLE) AS pace,
    TRY_CAST(pa.paceper40 AS DOUBLE) AS pacePer40,
    TRY_CAST(pa.possessions AS DOUBLE) AS possessions,
    TRY_CAST(pa.pie AS DOUBLE) AS PIE,

    -- 5) Misc / derived (typed numeric)
    TRY_CAST(pf.freethrowattemptrate AS DOUBLE) AS freeThrowAttemptRate,
    TRY_CAST(pf.oppeffectivefieldgoalpercentage AS DOUBLE) AS oppEffectiveFieldGoalPercentage,
    TRY_CAST(pf.oppfreethrowattemptrate AS DOUBLE) AS oppFreeThrowAttemptRate,
    TRY_CAST(pf.oppteamturnoverpercentage AS DOUBLE) AS oppTeamTurnoverPercentage,
    TRY_CAST(pf.oppoffensivereboundpercentage AS DOUBLE) AS oppOffensiveReboundPercentage,

    TRY_CAST(pm.pointsoffturnovers AS DOUBLE) AS pointsOffTurnovers,
    TRY_CAST(pm.pointssecondchance AS DOUBLE) AS pointsSecondChance,
    TRY_CAST(pm.pointsfastbreak AS DOUBLE) AS pointsFastBreak,
    TRY_CAST(pm.pointspaint AS DOUBLE) AS pointsPaint,
    TRY_CAST(pm.opppointsoffturnovers AS DOUBLE) AS oppPointsOffTurnovers,
    TRY_CAST(pm.opppointssecondchance AS DOUBLE) AS oppPointsSecondChance,
    TRY_CAST(pm.opppointsfastbreak AS DOUBLE) AS oppPointsFastBreak,
    TRY_CAST(pm.opppointspaint AS DOUBLE) AS oppPointsPaint,
    TRY_CAST(pm.blocksagainst AS DOUBLE) AS blocksAgainst,
    TRY_CAST(pm.foulsdrawn AS DOUBLE) AS foulsDrawn,

    -- 6) Scoring % breakdown (typed numeric)
    TRY_CAST(ps.percentagefieldgoalsattempted2pt AS DOUBLE) AS percentageFieldGoalsAttempted2pt,
    TRY_CAST(ps.percentagefieldgoalsattempted3pt AS DOUBLE) AS percentageFieldGoalsAttempted3pt,
    TRY_CAST(ps.percentagepoints2pt AS DOUBLE) AS percentagePoints2pt,
    TRY_CAST(ps.percentagepointsmidrange2pt AS DOUBLE) AS percentagePointsMidrange2pt,
    TRY_CAST(ps.percentagepoints3pt AS DOUBLE) AS percentagePoints3pt,
    TRY_CAST(ps.percentagepointsfastbreak AS DOUBLE) AS percentagePointsFastBreak,
    TRY_CAST(ps.percentagepointsfreethrow AS DOUBLE) AS percentagePointsFreeThrow,
    TRY_CAST(ps.percentagepointsoffturnovers AS DOUBLE) AS percentagePointsOffTurnovers,
    TRY_CAST(ps.percentagepointspaint AS DOUBLE) AS percentagePointsPaint,
    TRY_CAST(ps.percentageassisted2pt AS DOUBLE) AS percentageAssisted2pt,
    TRY_CAST(ps.percentageunassisted2pt AS DOUBLE) AS percentageUnassisted2pt,
    TRY_CAST(ps.percentageassisted3pt AS DOUBLE) AS percentageAssisted3pt,
    TRY_CAST(ps.percentageunassisted3pt AS DOUBLE) AS percentageUnassisted3pt,
    TRY_CAST(ps.percentageassistedfgm AS DOUBLE) AS percentageAssistedFGM,
    TRY_CAST(ps.percentageunassistedfgm AS DOUBLE) AS percentageUnassistedFGM

  FROM raw.log_table lt
  LEFT JOIN raw.players_traditional pt
    ON TRY_CAST(lt.game_id AS BIGINT) = TRY_CAST(pt.gameid AS BIGINT)
   AND lt.team_abbreviation = pt.teamtricode
  LEFT JOIN raw.players_advanced pa
    ON TRY_CAST(pt.gameid AS BIGINT) = TRY_CAST(pa.gameid AS BIGINT)
   AND TRY_CAST(pt.personid AS BIGINT) = TRY_CAST(pa.personid AS BIGINT)
  LEFT JOIN raw.players_fourfactors pf
    ON TRY_CAST(pa.gameid AS BIGINT) = TRY_CAST(pf.gameid AS BIGINT)
   AND TRY_CAST(pa.personid AS BIGINT) = TRY_CAST(pf.personid AS BIGINT)
  LEFT JOIN raw.players_scoring ps
    ON TRY_CAST(pf.gameid AS BIGINT) = TRY_CAST(ps.gameid AS BIGINT)
   AND TRY_CAST(pf.personid AS BIGINT) = TRY_CAST(ps.personid AS BIGINT)
  LEFT JOIN raw.players_misc pm
    ON TRY_CAST(ps.gameid AS BIGINT) = TRY_CAST(pm.gameid AS BIGINT)
   AND TRY_CAST(ps.personid AS BIGINT) = TRY_CAST(pm.personid AS BIGINT)
)

SELECT *
FROM joined
WHERE gameDate IS NOT NULL;
