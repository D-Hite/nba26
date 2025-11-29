MODEL (
  name base.teams_combined,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column gameDate
  )
);
SELECT DISTINCT
    -- 1. Information / Metadata
    COALESCE(
        CAST(raw.log_table.GAME_ID AS VARCHAR),
        CAST(raw.teams_fourfactors.gameId AS VARCHAR),
        CAST(base.lines_table.gameId AS VARCHAR),
        CAST(raw.teams_advanced.gameId AS VARCHAR),
        CAST(raw.teams_misc.gameId AS VARCHAR),
        CAST(raw.teams_scoring.gameId AS VARCHAR),
        CAST(raw.teams_traditional_totals.gameId AS VARCHAR)
    ) AS gameId,
    COALESCE(raw.log_table.GAME_DATE, base.lines_table.gameDate) AS gameDate,
    COALESCE(
        raw.log_table.TEAM_ABBREVIATION,
        raw.teams_fourfactors.teamTricode,
        base.lines_table.teamTricode,
        raw.teams_advanced.teamTricode,
        raw.teams_misc.teamTricode,
        raw.teams_scoring.teamTricode,
        raw.teams_traditional_totals.teamTricode
    ) AS teamTricode,
    raw.log_table.SEASON_ID as seasonId,
    COALESCE(
        raw.log_table.TEAM_ID,
        raw.teams_fourfactors.teamId,
        raw.teams_advanced.teamId,
        raw.teams_misc.teamId,
        raw.teams_scoring.teamId,
        raw.teams_traditional_totals.teamId
    ) AS teamId,
    COALESCE(
        raw.log_table.TEAM_NAME,
        raw.teams_fourfactors.teamName,
        raw.teams_advanced.teamName,
        raw.teams_misc.teamName,
        raw.teams_scoring.teamName,
        raw.teams_traditional_totals.teamName
    ) AS teamName,
    COALESCE(raw.teams_fourfactors.teamCity, raw.teams_advanced.teamCity, raw.teams_misc.teamCity, raw.teams_scoring.teamCity, raw.teams_traditional_totals.teamCity) AS teamCity,
    raw.log_table.MATCHUP as matchup,

    -- 2. Outcome Stats
    raw.log_table.WL as winLoss,
    raw.log_table.PLUS_MINUS AS plusMinus,
    base.lines_table.LINE as line,
    base.lines_table.OU as overUnder,

    -- 3. Core Box Score Stats (Numeric)
    COALESCE(CAST(raw.log_table.MIN AS VARCHAR),CAST(raw.teams_fourfactors.minutes AS VARCHAR),CAST(raw.teams_advanced.minutes AS VARCHAR),CAST(raw.teams_misc.minutes AS VARCHAR),CAST(raw.teams_scoring.minutes AS VARCHAR),CAST(raw.teams_traditional_totals.minutes AS VARCHAR)) as minutes,
    COALESCE(raw.log_table.PTS, raw.teams_traditional_totals.points) AS points,
    COALESCE(raw.log_table.FGM, raw.teams_traditional_totals.fieldGoalsMade) AS fieldGoalsMade,
    COALESCE(raw.log_table.FGA, raw.teams_traditional_totals.fieldGoalsAttempted) AS fieldGoalsAttempted,
    COALESCE(raw.log_table.FG3M, raw.teams_traditional_totals.threePointersMade) AS threePointersMade,
    COALESCE(raw.log_table.FG3A, raw.teams_traditional_totals.threePointersAttempted) AS threePointersAttempted,
    COALESCE(raw.log_table.FTM, raw.teams_traditional_totals.freeThrowsMade) AS freeThrowsMade,
    COALESCE(raw.log_table.FTA, raw.teams_traditional_totals.freeThrowsAttempted) AS freeThrowsAttempted,
    COALESCE(raw.log_table.OREB, raw.teams_traditional_totals.reboundsOffensive) AS reboundsOffensive,
    COALESCE(raw.log_table.DREB, raw.teams_traditional_totals.reboundsDefensive) AS reboundsDefensive,
    COALESCE(raw.log_table.REB, raw.teams_traditional_totals.reboundsTotal) AS reboundsTotal,
    COALESCE(raw.log_table.AST, raw.teams_traditional_totals.assists) AS assists,
    COALESCE(raw.log_table.STL, raw.teams_traditional_totals.steals) AS steals,
    COALESCE(raw.log_table.BLK, raw.teams_misc.blocks, raw.teams_traditional_totals.blocks) AS blocks,
    COALESCE(raw.log_table.PF, raw.teams_misc.foulsPersonal, raw.teams_traditional_totals.foulsPersonal) AS foulsPersonal,
    COALESCE(raw.log_table.TOV,raw.teams_traditional_totals.turnovers) as turnovers,

    -- 4. Misc Scoring
    raw.teams_misc.pointsOffTurnovers,
    raw.teams_misc.pointsSecondChance,
    raw.teams_misc.pointsFastBreak,
    raw.teams_misc.pointsPaint,
    raw.teams_misc.oppPointsOffTurnovers,
    raw.teams_misc.oppPointsSecondChance,
    raw.teams_misc.oppPointsFastBreak,
    raw.teams_misc.oppPointsPaint,
    raw.teams_misc.blocksAgainst,
    raw.teams_misc.foulsDrawn,

    -- 5. Percentages / Advanced
    COALESCE(raw.log_table.FG_PCT, raw.teams_traditional_totals.fieldGoalsPercentage) AS fieldGoalsPercentage,
    COALESCE(raw.log_table.FG3_PCT, raw.teams_traditional_totals.threePointersPercentage) AS threePointersPercentage,
    COALESCE(raw.log_table.FT_PCT, raw.teams_traditional_totals.freeThrowsPercentage) AS freeThrowsPercentage,
    raw.teams_advanced.estimatedOffensiveRating,
    raw.teams_advanced.offensiveRating,
    raw.teams_advanced.estimatedDefensiveRating,
    raw.teams_advanced.defensiveRating,
    raw.teams_advanced.estimatedNetRating,
    raw.teams_advanced.netRating,
    raw.teams_advanced.assistPercentage,
    raw.teams_advanced.assistToTurnover,
    raw.teams_advanced.assistRatio,
    COALESCE(raw.teams_fourfactors.offensiveReboundPercentage, raw.teams_advanced.offensiveReboundPercentage) AS offensiveReboundPercentage,
    raw.teams_advanced.defensiveReboundPercentage,
    raw.teams_advanced.reboundPercentage,
    raw.teams_advanced.estimatedTeamTurnoverPercentage,
    raw.teams_fourfactors.teamTurnoverPercentage,
    COALESCE(raw.teams_fourfactors.effectiveFieldGoalPercentage, raw.teams_advanced.effectiveFieldGoalPercentage) AS effectiveFieldGoalPercentage,
    raw.teams_advanced.trueShootingPercentage,
    raw.teams_advanced.usagePercentage,
    raw.teams_advanced.estimatedUsagePercentage,
    raw.teams_advanced.estimatedPace,
    raw.teams_advanced.pace,
    raw.teams_advanced.pacePer40,
    raw.teams_advanced.possessions,
    raw.teams_advanced.PIE,
    raw.teams_fourfactors.freeThrowAttemptRate,
    raw.teams_fourfactors.oppEffectiveFieldGoalPercentage,
    raw.teams_fourfactors.oppFreeThrowAttemptRate,
    raw.teams_fourfactors.oppTeamTurnoverPercentage,
    raw.teams_fourfactors.oppOffensiveReboundPercentage,

    -- 6. Scoring Percent Breakdown
    raw.teams_scoring.percentageFieldGoalsAttempted2pt,
    raw.teams_scoring.percentageFieldGoalsAttempted3pt,
    raw.teams_scoring.percentagePoints2pt,
    raw.teams_scoring.percentagePointsMidrange2pt,
    raw.teams_scoring.percentagePoints3pt,
    raw.teams_scoring.percentagePointsFastBreak,
    raw.teams_scoring.percentagePointsFreeThrow,
    raw.teams_scoring.percentagePointsOffTurnovers,
    raw.teams_scoring.percentagePointsPaint,
    raw.teams_scoring.percentageAssisted2pt,
    raw.teams_scoring.percentageUnassisted2pt,
    raw.teams_scoring.percentageAssisted3pt,
    raw.teams_scoring.percentageUnassisted3pt,
    raw.teams_scoring.percentageAssistedFGM,
    raw.teams_scoring.percentageUnassistedFGM

FROM raw.log_table
LEFT JOIN raw.teams_advanced
    ON raw.log_table.GAME_ID::int = raw.teams_advanced.gameId::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_advanced.teamTricode
LEFT JOIN raw.teams_fourfactors
    ON raw.log_table.GAME_ID::int = raw.teams_fourfactors.gameId::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_fourfactors.teamTricode
LEFT JOIN raw.teams_traditional_totals
    ON raw.log_table.GAME_ID::int = raw.teams_traditional_totals.gameId::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_traditional_totals.teamTricode
LEFT JOIN raw.teams_scoring
    ON raw.log_table.GAME_ID::int = raw.teams_scoring.gameId::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_scoring.teamTricode
LEFT JOIN raw.teams_misc
    ON raw.log_table.GAME_ID::int = raw.teams_misc.gameId::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_misc.teamTricode
LEFT JOIN base.lines_table
    ON raw.log_table.GAME_ID::int = base.lines_table.gameId::int
    AND raw.log_table.TEAM_ABBREVIATION = base.lines_table.teamTricode;
