MODEL (
    name aggs.team_averages,
    kind VIEW
);

SELECT
    -- ============================================================
    --  IDENTIFIERS & GAME CONTEXT
    -- ============================================================
    tp.gameId,
    tp.gameDate,
    tp.seasonId,
    tp.seasonType,
    tp.teamId,
    tp.teamTricode,
    tp.teamName,
    tp.teamCity,
    tp.homeTeam,
    tp.awayTeam,
    tp.homeGame,
    tp.MATCHUP,
    tp.gameNumber,
    tp.daysSinceLastGame,
    tp.isBackToBack,
    tp.is3In4,
    tp.is4In6,
    tp.isLastTeamGame,
    tp.line,
    tp.overUnder,

    -- ============================================================
    --  RAW OUTCOMES
    -- ============================================================
    tp.winLoss,
    tp.points,
    tp.opponentPoints,
    tp.totalPoints,
    tp.scoreDiff,
    tp.coverResult,
    tp.overUnderResult,

    -- ============================================================
    --  AVERAGES — HIGH-LEVEL PERFORMANCE
    -- ============================================================
    AVG(tp.points) OVER w AS average_points,
    AVG(tp.opponentPoints) OVER w AS average_opponent_points,
    AVG(tp.totalPoints) OVER w AS average_total_points,
    AVG(tp.scoreDiff) OVER w AS average_score_difference,

    -- ============================================================
    --  SHOOTING — FIELD GOALS
    -- ============================================================
    AVG(tp.fieldGoalsMade) OVER w AS average_field_goals_made,
    AVG(tp.fieldGoalsAttempted) OVER w AS average_field_goals_attempted,
    AVG(tp.fieldGoalsPercentage) OVER w AS average_field_goal_percentage,

    -- ============================================================
    --  SHOOTING — THREE POINTERS
    -- ============================================================
    AVG(tp.threePointersMade) OVER w AS average_three_pointers_made,
    AVG(tp.threePointersAttempted) OVER w AS average_three_pointers_attempted,
    AVG(tp.threePointersPercentage) OVER w AS average_three_point_percentage,

    -- ============================================================
    --  SHOOTING — FREE THROWS
    -- ============================================================
    AVG(tp.freeThrowsMade) OVER w AS average_free_throws_made,
    AVG(tp.freeThrowsAttempted) OVER w AS average_free_throws_attempted,
    AVG(tp.freeThrowsPercentage) OVER w AS average_free_throw_percentage,

    -- ============================================================
    --  REBOUNDING
    -- ============================================================
    AVG(tp.reboundsOffensive) OVER w AS average_offensive_rebounds,
    AVG(tp.reboundsDefensive) OVER w AS average_defensive_rebounds,
    AVG(tp.reboundsTotal) OVER w AS average_total_rebounds,

    -- ============================================================
    --  CORE COUNTING STATS
    -- ============================================================
    AVG(tp.assists) OVER w AS average_assists,
    AVG(tp.steals) OVER w AS average_steals,
    AVG(tp.blocks) OVER w AS average_blocks,
    AVG(tp.turnovers) OVER w AS average_turnovers,
    AVG(tp.foulsPersonal) OVER w AS average_personal_fouls,

    -- ============================================================
    --  ADVANCED RATINGS
    -- ============================================================
    AVG(tp.estimatedOffensiveRating) OVER w AS average_estimated_offensive_rating,
    AVG(tp.offensiveRating) OVER w AS average_offensive_rating,
    AVG(tp.estimatedDefensiveRating) OVER w AS average_estimated_defensive_rating,
    AVG(tp.defensiveRating) OVER w AS average_defensive_rating,
    AVG(tp.estimatedNetRating) OVER w AS average_estimated_net_rating,
    AVG(tp.netRating) OVER w AS average_net_rating,

    -- ============================================================
    --  ADVANCED POSSESSION & ASSIST STATS
    -- ============================================================
    AVG(tp.assistPercentage) OVER w AS average_assist_percentage,
    AVG(tp.assistToTurnover) OVER w AS average_assist_to_turnover_ratio,
    AVG(tp.assistRatio) OVER w AS average_assist_ratio,

    -- ============================================================
    --  ADVANCED REBOUND RATES
    -- ============================================================
    AVG(tp.offensiveReboundPercentage) OVER w AS average_offensive_rebound_percentage,
    AVG(tp.defensiveReboundPercentage) OVER w AS average_defensive_rebound_percentage,
    AVG(tp.reboundPercentage) OVER w AS average_total_rebound_percentage,

    -- ============================================================
    --  SHOOTING EFFICIENCY METRICS
    -- ============================================================
    AVG(tp.teamTurnoverPercentage) OVER w AS average_team_turnover_percentage,
    AVG(tp.effectiveFieldGoalPercentage) OVER w AS average_effective_field_goal_percentage,
    AVG(tp.trueShootingPercentage) OVER w AS average_true_shooting_percentage,
    AVG(tp.usagePercentage) OVER w AS average_usage_percentage,
    AVG(tp.estimatedUsagePercentage) OVER w AS average_estimated_usage_percentage,

    -- ============================================================
    --  PACE METRICS
    -- ============================================================
    AVG(tp.estimatedPace) OVER w AS average_estimated_pace,
    AVG(tp.pace) OVER w AS average_pace,
    AVG(tp.pacePer40) OVER w AS average_pace_per_40_minutes,
    AVG(tp.possessions) OVER w AS average_possessions,
    AVG(tp.PIE) OVER w AS average_player_impact_estimate,

    -- ============================================================
    --  FOUR FACTORS & OPPONENT EFFICIENCY
    -- ============================================================
    AVG(tp.freeThrowAttemptRate) OVER w AS average_free_throw_attempt_rate,
    AVG(tp.oppEffectiveFieldGoalPercentage) OVER w AS average_opponent_effective_field_goal_percentage,
    AVG(tp.oppFreeThrowAttemptRate) OVER w AS average_opponent_free_throw_attempt_rate,
    AVG(tp.oppTeamTurnoverPercentage) OVER w AS average_opponent_turnover_percentage,
    AVG(tp.oppOffensiveReboundPercentage) OVER w AS average_opponent_offensive_rebound_percentage,

    -- ============================================================
    --  SCORING PROFILE (POINT TYPES)
    -- ============================================================
    AVG(tp.pointsOffTurnovers) OVER w AS average_points_off_turnovers,
    AVG(tp.pointsSecondChance) OVER w AS average_second_chance_points,
    AVG(tp.pointsFastBreak) OVER w AS average_fast_break_points,
    AVG(tp.pointsPaint) OVER w AS average_points_in_the_paint,

    AVG(tp.oppPointsOffTurnovers) OVER w AS average_opponent_points_off_turnovers,
    AVG(tp.oppPointsSecondChance) OVER w AS average_opponent_second_chance_points,
    AVG(tp.oppPointsFastBreak) OVER w AS average_opponent_fast_break_points,
    AVG(tp.oppPointsPaint) OVER w AS average_opponent_points_in_the_paint,

    -- ============================================================
    --  SCORING DISTRIBUTION PERCENTAGES
    -- ============================================================
    AVG(tp.percentageFieldGoalsAttempted2pt) OVER w AS average_percentage_field_goals_attempted_2pt,
    AVG(tp.percentageFieldGoalsAttempted3pt) OVER w AS average_percentage_field_goals_attempted_3pt,

    AVG(tp.percentagePoints2pt) OVER w AS average_percentage_points_from_2pt,
    AVG(tp.percentagePointsMidrange2pt) OVER w AS average_percentage_points_from_midrange_2pt,
    AVG(tp.percentagePoints3pt) OVER w AS average_percentage_points_from_3pt,
    AVG(tp.percentagePointsFastBreak) OVER w AS average_percentage_points_from_fast_breaks,
    AVG(tp.percentagePointsFreeThrow) OVER w AS average_percentage_points_from_free_throws,
    AVG(tp.percentagePointsOffTurnovers) OVER w AS average_percentage_points_off_turnovers,
    AVG(tp.percentagePointsPaint) OVER w AS average_percentage_points_in_paint,

    -- Assisted / Unassisted
    AVG(tp.percentageAssisted2pt) OVER w AS average_percentage_assisted_2pt,
    AVG(tp.percentageUnassisted2pt) OVER w AS average_percentage_unassisted_2pt,
    AVG(tp.percentageAssisted3pt) OVER w AS average_percentage_assisted_3pt,
    AVG(tp.percentageUnassisted3pt) OVER w AS average_percentage_unassisted_3pt,

    AVG(tp.percentageAssistedFGM) OVER w AS average_percentage_assisted_field_goals_made,

    -- The example you provided:
    AVG(tp.percentageUnassistedFGM) OVER w AS average_percentage_unassisted_field_goals_made

FROM base.teams_processed tp

WINDOW w AS (
    PARTITION BY tp.seasonId, tp.teamId
    ORDER BY CAST(tp.gameDate AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);
