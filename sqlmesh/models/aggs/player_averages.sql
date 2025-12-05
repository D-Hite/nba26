MODEL (
    name aggs.player_averages,
    kind VIEW
);

SELECT
    ---------------------------------------------------------------------
    -- GAME / PLAYER CONTEXT (RAW, NON-AGGREGATED)
    ---------------------------------------------------------------------
    pp.seasonId,
    pp.teamId,
    pp.teamAbbreviation,
    pp.teamName,
    pp.teamCity,
    pp.gameId,
    pp.nextGameId,
    pp.gameDate,

    pp.personId,
    pp.firstName,
    pp.familyName,
    pp.playerSlug,
    pp.position,
    pp.comment,

    -- Sequence / availability context
    pp.gameCount,
    pp.playedFlag,
    pp.gamesPlayed,
    pp.isLastGame,

    -- Game context from log
    pp.matchup,
    pp.winLoss,

    -- Team / schedule context from teams_processed
    pp.homeGame,
    pp.isBackToBack,
    pp.is3In4,
    pp.is4In6,
    pp.daysSinceLastGame,
    pp.isLastTeamGame,
    pp.winsSoFar,
    pp.lossesSoFar,
    pp.last10WinPercentage,
    pp.winsVsOpponent,
    pp.lossesVsOpponent,

    ---------------------------------------------------------------------
    -- ===========================================================
    -- ROLLING SEASON-TO-DATE AVERAGES FOR PLAYER STATS
    -- (ONLY COUNT GAMES WHERE playedFlag = 1)
    -- ===========================================================
    ---------------------------------------------------------------------

    -- Availability / minutes
    AVG(pp.playedFlag) OVER w AS average_played_flag,  -- share of games played

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.minutes END
    ) OVER w AS average_minutes,

    ---------------------------------------------------------------------
    -- CORE BOX SCORE COUNTING STATS
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.points END
    ) OVER w AS average_points,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.reboundsOffensive END
    ) OVER w AS average_offensive_rebounds,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.reboundsDefensive END
    ) OVER w AS average_defensive_rebounds,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.reboundsTotal END
    ) OVER w AS average_total_rebounds,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.assists END
    ) OVER w AS average_assists,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.steals END
    ) OVER w AS average_steals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.blocks END
    ) OVER w AS average_blocks,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.turnovers END
    ) OVER w AS average_turnovers,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.foulsPersonal END
    ) OVER w AS average_personal_fouls,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.plusMinusPoints END
    ) OVER w AS average_plus_minus_points,

    ---------------------------------------------------------------------
    -- SHOOTING VOLUME AND EFFICIENCY
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.fieldGoalsMade END
    ) OVER w AS average_field_goals_made,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.fieldGoalsAttempted END
    ) OVER w AS average_field_goals_attempted,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.fieldGoalsPercentage END
    ) OVER w AS average_field_goal_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.threePointersMade END
    ) OVER w AS average_three_pointers_made,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.threePointersAttempted END
    ) OVER w AS average_three_pointers_attempted,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.threePointersPercentage END
    ) OVER w AS average_three_point_field_goal_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.freeThrowsMade END
    ) OVER w AS average_free_throws_made,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.freeThrowsAttempted END
    ) OVER w AS average_free_throws_attempted,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.freeThrowsPercentage END
    ) OVER w AS average_free_throw_percentage,

    ---------------------------------------------------------------------
    -- ADVANCED RATINGS
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.offensiveRating END
    ) OVER w AS average_offensive_rating,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.estimatedOffensiveRating END
    ) OVER w AS average_estimated_offensive_rating,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.defensiveRating END
    ) OVER w AS average_defensive_rating,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.estimatedDefensiveRating END
    ) OVER w AS average_estimated_defensive_rating,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.netRating END
    ) OVER w AS average_net_rating,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.estimatedNetRating END
    ) OVER w AS average_estimated_net_rating,

    ---------------------------------------------------------------------
    -- CREATION / ASSIST METRICS
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.assistPercentage END
    ) OVER w AS average_assist_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.assistToTurnover END
    ) OVER w AS average_assist_to_turnover_ratio,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.assistRatio END
    ) OVER w AS average_assist_ratio,

    ---------------------------------------------------------------------
    -- REBOUND RATE METRICS
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.offensiveReboundPercentage END
    ) OVER w AS average_offensive_rebound_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.defensiveReboundPercentage END
    ) OVER w AS average_defensive_rebound_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.reboundPercentage END
    ) OVER w AS average_total_rebound_percentage,

    ---------------------------------------------------------------------
    -- TURNOVER AND SHOOTING EFFICIENCY METRICS
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.teamTurnoverPercentage END
    ) OVER w AS average_team_turnover_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.effectiveFieldGoalPercentage END
    ) OVER w AS average_effective_field_goal_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.trueShootingPercentage END
    ) OVER w AS average_true_shooting_percentage,

    ---------------------------------------------------------------------
    -- USAGE AND PACE METRICS
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.usagePercentage END
    ) OVER w AS average_usage_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.estimatedUsagePercentage END
    ) OVER w AS average_estimated_usage_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.estimatedPace END
    ) OVER w AS average_estimated_pace,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.pace END
    ) OVER w AS average_pace,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.pacePer40 END
    ) OVER w AS average_pace_per_40_minutes,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.possessions END
    ) OVER w AS average_possessions,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.PIE END
    ) OVER w AS average_player_impact_estimate,

    ---------------------------------------------------------------------
    -- FOUR FACTORS AND OPPONENT EFFICIENCY
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.freeThrowAttemptRate END
    ) OVER w AS average_free_throw_attempt_rate,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.oppEffectiveFieldGoalPercentage END
    ) OVER w AS average_opponent_effective_field_goal_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.oppFreeThrowAttemptRate END
    ) OVER w AS average_opponent_free_throw_attempt_rate,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.oppTeamTurnoverPercentage END
    ) OVER w AS average_opponent_team_turnover_percentage,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.oppOffensiveReboundPercentage END
    ) OVER w AS average_opponent_offensive_rebound_percentage,

    ---------------------------------------------------------------------
    -- SCORING PROFILE (RAW POINT TYPES)
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.pointsOffTurnovers END
    ) OVER w AS average_points_off_turnovers,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.pointsSecondChance END
    ) OVER w AS average_second_chance_points,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.pointsFastBreak END
    ) OVER w AS average_fast_break_points,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.pointsPaint END
    ) OVER w AS average_points_in_the_paint,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.oppPointsOffTurnovers END
    ) OVER w AS average_opponent_points_off_turnovers,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.oppPointsSecondChance END
    ) OVER w AS average_opponent_second_chance_points,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.oppPointsFastBreak END
    ) OVER w AS average_opponent_fast_break_points,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.oppPointsPaint END
    ) OVER w AS average_opponent_points_in_the_paint,

    ---------------------------------------------------------------------
    -- MISCELLANEOUS EVENT COUNTS
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.blocksAgainst END
    ) OVER w AS average_blocks_against,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.foulsDrawn END
    ) OVER w AS average_fouls_drawn,

    ---------------------------------------------------------------------
    -- SCORING DISTRIBUTION PERCENTAGES
    ---------------------------------------------------------------------
    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentageFieldGoalsAttempted2pt END
    ) OVER w AS average_percentage_field_goals_attempted_two_point_field_goals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentageFieldGoalsAttempted3pt END
    ) OVER w AS average_percentage_field_goals_attempted_three_point_field_goals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentagePoints2pt END
    ) OVER w AS average_percentage_points_from_two_point_field_goals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentagePointsMidrange2pt END
    ) OVER w AS average_percentage_points_from_midrange_two_point_field_goals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentagePoints3pt END
    ) OVER w AS average_percentage_points_from_three_point_field_goals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentagePointsFastBreak END
    ) OVER w AS average_percentage_points_from_fast_breaks,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentagePointsFreeThrow END
    ) OVER w AS average_percentage_points_from_free_throws,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentagePointsOffTurnovers END
    ) OVER w AS average_percentage_points_off_turnovers,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentagePointsPaint END
    ) OVER w AS average_percentage_points_in_the_paint,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentageAssisted2pt END
    ) OVER w AS average_percentage_assisted_two_point_field_goals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentageUnassisted2pt END
    ) OVER w AS average_percentage_unassisted_two_point_field_goals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentageAssisted3pt END
    ) OVER w AS average_percentage_assisted_three_point_field_goals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentageUnassisted3pt END
    ) OVER w AS average_percentage_unassisted_three_point_field_goals,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentageAssistedFGM END
    ) OVER w AS average_percentage_assisted_field_goals_made,

    AVG(
        CASE WHEN pp.playedFlag = 1 THEN pp.percentageUnassistedFGM END
    ) OVER w AS average_percentage_unassisted_field_goals_made

FROM base.players_processed pp

WINDOW w AS (
    PARTITION BY pp.personId, pp.seasonId
    ORDER BY CAST(pp.gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);
