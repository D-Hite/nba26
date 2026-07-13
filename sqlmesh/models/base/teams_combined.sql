MODEL (
  name base.teams_combined,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column gameDate
  ),
  dialect duckdb
);

WITH joined AS (
  SELECT
    -- 1) IDs / keys (keep as VARCHAR)
    COALESCE(
      CAST(lt.game_id AS VARCHAR),
      CAST(tf.gameid AS VARCHAR),
      CAST(bl.gameId AS VARCHAR),
      CAST(ta.gameid AS VARCHAR),
      CAST(tm.gameid AS VARCHAR),
      CAST(ts.gameid AS VARCHAR),
      CAST(tt.gameid AS VARCHAR)
    ) AS gameId,

    COALESCE(
      TRY_CAST(lt.game_date AS DATE),
      bl.gameDate
    ) AS gameDate,

    COALESCE(
      lt.team_abbreviation,
      tf.teamtricode,
      bl.teamTricode,
      ta.teamtricode,
      tm.teamtricode,
      ts.teamtricode,
      tt.teamtricode
    ) AS teamTricode,

    TRY_CAST(lt.season_id AS BIGINT) AS seasonId,

    COALESCE(
      lt.team_id,
      tf.teamid,
      ta.teamid,
      tm.teamid,
      ts.teamid,
      tt.teamid
    ) AS teamId,

    COALESCE(
      lt.team_name,
      tf.teamname,
      ta.teamname,
      tm.teamname,
      ts.teamname,
      tt.teamname
    ) AS teamName,

    COALESCE(tf.teamcity, ta.teamcity, tm.teamcity, ts.teamcity, tt.teamcity) AS teamCity,

    lt.matchup AS matchup,

    -- 2) Outcome + betting
    lt.wl AS winLoss,
    TRY_CAST(lt.plus_minus AS DOUBLE) AS plusMinus,
    TRY_CAST(bl.line AS DOUBLE) AS line,
    TRY_CAST(bl.ou AS DOUBLE) AS overUnder,

    -- 3) Core numeric stats (prefer log_table, else totals)
    TRY_CAST(COALESCE(lt.min, tf.minutes, ta.minutes, tm.minutes, ts.minutes, tt.minutes) AS DOUBLE) AS minutes,

    COALESCE(TRY_CAST(lt.pts AS DOUBLE), TRY_CAST(tt.points AS DOUBLE)) AS points,
    COALESCE(TRY_CAST(lt.fgm AS DOUBLE), TRY_CAST(tt.fieldgoalsmade AS DOUBLE)) AS fieldGoalsMade,
    COALESCE(TRY_CAST(lt.fga AS DOUBLE), TRY_CAST(tt.fieldgoalsattempted AS DOUBLE)) AS fieldGoalsAttempted,
    COALESCE(TRY_CAST(lt.fg3m AS DOUBLE), TRY_CAST(tt.threepointersmade AS DOUBLE)) AS threePointersMade,
    COALESCE(TRY_CAST(lt.fg3a AS DOUBLE), TRY_CAST(tt.threepointersattempted AS DOUBLE)) AS threePointersAttempted,
    COALESCE(TRY_CAST(lt.ftm AS DOUBLE), TRY_CAST(tt.freethrowsmade AS DOUBLE)) AS freeThrowsMade,
    COALESCE(TRY_CAST(lt.fta AS DOUBLE), TRY_CAST(tt.freethrowsattempted AS DOUBLE)) AS freeThrowsAttempted,
    COALESCE(TRY_CAST(lt.oreb AS DOUBLE), TRY_CAST(tt.reboundsoffensive AS DOUBLE)) AS reboundsOffensive,
    COALESCE(TRY_CAST(lt.dreb AS DOUBLE), TRY_CAST(tt.reboundsdefensive AS DOUBLE)) AS reboundsDefensive,
    COALESCE(TRY_CAST(lt.reb AS DOUBLE), TRY_CAST(tt.reboundstotal AS DOUBLE)) AS reboundsTotal,
    COALESCE(TRY_CAST(lt.ast AS DOUBLE), TRY_CAST(tt.assists AS DOUBLE)) AS assists,
    COALESCE(TRY_CAST(lt.stl AS DOUBLE), TRY_CAST(tt.steals AS DOUBLE)) AS steals,
    COALESCE(TRY_CAST(lt.blk AS DOUBLE), TRY_CAST(tm.blocks AS DOUBLE), TRY_CAST(tt.blocks AS DOUBLE)) AS blocks,
    COALESCE(TRY_CAST(lt.pf AS DOUBLE), TRY_CAST(tm.foulspersonal AS DOUBLE), TRY_CAST(tt.foulspersonal AS DOUBLE)) AS foulsPersonal,
    COALESCE(TRY_CAST(lt.tov AS DOUBLE), TRY_CAST(tt.turnovers AS DOUBLE)) AS turnovers,

    -- 4) Misc scoring (all from teams_misc)
    TRY_CAST(tm.pointsoffturnovers AS DOUBLE) AS pointsOffTurnovers,
    TRY_CAST(tm.pointssecondchance AS DOUBLE) AS pointsSecondChance,
    TRY_CAST(tm.pointsfastbreak AS DOUBLE) AS pointsFastBreak,
    TRY_CAST(tm.pointspaint AS DOUBLE) AS pointsPaint,
    TRY_CAST(tm.opppointsoffturnovers AS DOUBLE) AS oppPointsOffTurnovers,
    TRY_CAST(tm.opppointssecondchance AS DOUBLE) AS oppPointsSecondChance,
    TRY_CAST(tm.opppointsfastbreak AS DOUBLE) AS oppPointsFastBreak,
    TRY_CAST(tm.opppointspaint AS DOUBLE) AS oppPointsPaint,
    TRY_CAST(tm.blocksagainst AS DOUBLE) AS blocksAgainst,
    TRY_CAST(tm.foulsdrawn AS DOUBLE) AS foulsDrawn,

    -- 5) Percentages / advanced (cast to DOUBLE)
    COALESCE(TRY_CAST(lt.fg_pct AS DOUBLE), TRY_CAST(tt.fieldgoalspercentage AS DOUBLE)) AS fieldGoalsPercentage,
    COALESCE(TRY_CAST(lt.fg3_pct AS DOUBLE), TRY_CAST(tt.threepointerspercentage AS DOUBLE)) AS threePointersPercentage,
    COALESCE(TRY_CAST(lt.ft_pct AS DOUBLE), TRY_CAST(tt.freethrowspercentage AS DOUBLE)) AS freeThrowsPercentage,

    TRY_CAST(ta.estimatedoffensiverating AS DOUBLE) AS estimatedOffensiveRating,
    TRY_CAST(ta.offensiverating AS DOUBLE) AS offensiveRating,
    TRY_CAST(ta.estimateddefensiverating AS DOUBLE) AS estimatedDefensiveRating,
    TRY_CAST(ta.defensiverating AS DOUBLE) AS defensiveRating,
    TRY_CAST(ta.estimatednetrating AS DOUBLE) AS estimatedNetRating,
    TRY_CAST(ta.netrating AS DOUBLE) AS netRating,
    TRY_CAST(ta.assistpercentage AS DOUBLE) AS assistPercentage,
    TRY_CAST(ta.assisttoturnover AS DOUBLE) AS assistToTurnover,
    TRY_CAST(ta.assistratio AS DOUBLE) AS assistRatio,

    COALESCE(TRY_CAST(tf.offensivereboundpercentage AS DOUBLE), TRY_CAST(ta.offensivereboundpercentage AS DOUBLE)) AS offensiveReboundPercentage,
    TRY_CAST(ta.defensivereboundpercentage AS DOUBLE) AS defensiveReboundPercentage,
    TRY_CAST(ta.reboundpercentage AS DOUBLE) AS reboundPercentage,
    TRY_CAST(ta.estimatedteamturnoverpercentage AS DOUBLE) AS estimatedTeamTurnoverPercentage,
    TRY_CAST(tf.teamturnoverpercentage AS DOUBLE) AS teamTurnoverPercentage,
    COALESCE(TRY_CAST(tf.effectivefieldgoalpercentage AS DOUBLE), TRY_CAST(ta.effectivefieldgoalpercentage AS DOUBLE)) AS effectiveFieldGoalPercentage,
    TRY_CAST(ta.trueshootingpercentage AS DOUBLE) AS trueShootingPercentage,
    TRY_CAST(ta.usagepercentage AS DOUBLE) AS usagePercentage,
    TRY_CAST(ta.estimatedusagepercentage AS DOUBLE) AS estimatedUsagePercentage,
    TRY_CAST(ta.estimatedpace AS DOUBLE) AS estimatedPace,
    TRY_CAST(ta.pace AS DOUBLE) AS pace,
    TRY_CAST(ta.paceper40 AS DOUBLE) AS pacePer40,
    TRY_CAST(ta.possessions AS DOUBLE) AS possessions,
    TRY_CAST(ta.pie AS DOUBLE) AS PIE,

    TRY_CAST(tf.freethrowattemptrate AS DOUBLE) AS freeThrowAttemptRate,
    TRY_CAST(tf.oppeffectivefieldgoalpercentage AS DOUBLE) AS oppEffectiveFieldGoalPercentage,
    TRY_CAST(tf.oppfreethrowattemptrate AS DOUBLE) AS oppFreeThrowAttemptRate,
    TRY_CAST(tf.oppteamturnoverpercentage AS DOUBLE) AS oppTeamTurnoverPercentage,
    TRY_CAST(tf.oppoffensivereboundpercentage AS DOUBLE) AS oppOffensiveReboundPercentage,

    -- 6) Scoring percent breakdown
    TRY_CAST(ts.percentagefieldgoalsattempted2pt AS DOUBLE) AS percentageFieldGoalsAttempted2pt,
    TRY_CAST(ts.percentagefieldgoalsattempted3pt AS DOUBLE) AS percentageFieldGoalsAttempted3pt,
    TRY_CAST(ts.percentagepoints2pt AS DOUBLE) AS percentagePoints2pt,
    TRY_CAST(ts.percentagepointsmidrange2pt AS DOUBLE) AS percentagePointsMidrange2pt,
    TRY_CAST(ts.percentagepoints3pt AS DOUBLE) AS percentagePoints3pt,
    TRY_CAST(ts.percentagepointsfastbreak AS DOUBLE) AS percentagePointsFastBreak,
    TRY_CAST(ts.percentagepointsfreethrow AS DOUBLE) AS percentagePointsFreeThrow,
    TRY_CAST(ts.percentagepointsoffturnovers AS DOUBLE) AS percentagePointsOffTurnovers,
    TRY_CAST(ts.percentagepointspaint AS DOUBLE) AS percentagePointsPaint,
    TRY_CAST(ts.percentageassisted2pt AS DOUBLE) AS percentageAssisted2pt,
    TRY_CAST(ts.percentageunassisted2pt AS DOUBLE) AS percentageUnassisted2pt,
    TRY_CAST(ts.percentageassisted3pt AS DOUBLE) AS percentageAssisted3pt,
    TRY_CAST(ts.percentageunassisted3pt AS DOUBLE) AS percentageUnassisted3pt,
    TRY_CAST(ts.percentageassistedfgm AS DOUBLE) AS percentageAssistedFGM,
    TRY_CAST(ts.percentageunassistedfgm AS DOUBLE) AS percentageUnassistedFGM

  FROM raw.log_table lt
  LEFT JOIN raw.teams_advanced ta
    ON TRY_CAST(lt.game_id AS BIGINT) = TRY_CAST(ta.gameid AS BIGINT)
   AND lt.team_abbreviation = ta.teamtricode
  LEFT JOIN raw.teams_fourfactors tf
    ON TRY_CAST(lt.game_id AS BIGINT) = TRY_CAST(tf.gameid AS BIGINT)
   AND lt.team_abbreviation = tf.teamtricode
  LEFT JOIN raw.teams_traditional_totals tt
    ON TRY_CAST(lt.game_id AS BIGINT) = TRY_CAST(tt.gameid AS BIGINT)
   AND lt.team_abbreviation = tt.teamtricode
  LEFT JOIN raw.teams_scoring ts
    ON TRY_CAST(lt.game_id AS BIGINT) = TRY_CAST(ts.gameid AS BIGINT)
   AND lt.team_abbreviation = ts.teamtricode
  LEFT JOIN raw.teams_misc tm
    ON TRY_CAST(lt.game_id AS BIGINT) = TRY_CAST(tm.gameid AS BIGINT)
   AND lt.team_abbreviation = tm.teamtricode
  LEFT JOIN base.lines_table bl
    ON TRY_CAST(lt.game_id AS BIGINT) = TRY_CAST(bl.gameId AS BIGINT)
   AND lt.team_abbreviation = bl.teamTricode
)

SELECT *
FROM joined
WHERE gameDate IS NOT NULL;
