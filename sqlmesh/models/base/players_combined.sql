MODEL (
  name base.players_combined,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column gameDate
  )
);

SELECT DISTINCT
    -- 1. Metadata (log_table → camelCase)
    raw.log_table.SEASON_ID AS seasonId,
    raw.log_table.TEAM_ID AS teamId,
    raw.log_table.TEAM_ABBREVIATION AS teamAbbreviation,
    raw.log_table.TEAM_NAME AS teamName,
    CAST(raw.log_table.GAME_ID AS VARCHAR) AS gameId,
    raw.log_table.GAME_DATE AS gameDate,
    raw.log_table.MATCHUP AS matchup,
    raw.log_table.WL AS winLoss,

    -- 2. Player identity
    COALESCE(
        raw.players_fourfactors.personId,
        raw.players_advanced.personId,
        raw.players_misc.personId,
        raw.players_scoring.personId,
        raw.players_traditional.personId
    ) AS personId,

    COALESCE(
        raw.players_fourfactors.firstName,
        raw.players_advanced.firstName,
        raw.players_misc.firstName,
        raw.players_scoring.firstName,
        raw.players_traditional.firstName
    ) AS firstName,

    COALESCE(
        raw.players_fourfactors.familyName,
        raw.players_advanced.familyName,
        raw.players_misc.familyName,
        raw.players_scoring.familyName,
        raw.players_traditional.familyName
    ) AS familyName,

    COALESCE(
        raw.players_fourfactors.playerSlug,
        raw.players_advanced.playerSlug,
        raw.players_misc.playerSlug,
        raw.players_scoring.playerSlug,
        raw.players_traditional.playerSlug
    ) AS playerSlug,

    COALESCE(
        raw.players_fourfactors.position,
        raw.players_advanced.position,
        raw.players_misc.position,
        raw.players_scoring.position,
        raw.players_traditional.position
    ) AS position,

    COALESCE(
        raw.players_fourfactors.comment,
        raw.players_advanced.comment,
        raw.players_misc.comment,
        raw.players_scoring.comment,
        raw.players_traditional.comment
    ) AS comment,

    COALESCE(
        raw.players_fourfactors.teamCity,
        raw.players_advanced.teamCity,
        raw.players_misc.teamCity,
        raw.players_scoring.teamCity,
        raw.players_traditional.teamCity
    ) AS teamCity,

    -- 3. Core Box Score Stats (NO renaming)
    COALESCE(
        raw.players_fourfactors.minutes,
        raw.players_advanced.minutes,
        raw.players_misc.minutes,
        raw.players_scoring.minutes,
        raw.players_traditional.minutes
    ) AS minutes,

    raw.players_traditional.points,
    raw.players_traditional.fieldGoalsMade,
    raw.players_traditional.fieldGoalsAttempted,
    raw.players_traditional.fieldGoalsPercentage,
    raw.players_traditional.threePointersMade,
    raw.players_traditional.threePointersAttempted,
    raw.players_traditional.threePointersPercentage,
    raw.players_traditional.freeThrowsMade,
    raw.players_traditional.freeThrowsAttempted,
    raw.players_traditional.freeThrowsPercentage,
    raw.players_traditional.reboundsOffensive,
    raw.players_traditional.reboundsDefensive,
    raw.players_traditional.reboundsTotal,
    raw.players_traditional.assists,
    raw.players_traditional.steals,

    COALESCE(raw.players_misc.blocks, raw.players_traditional.blocks) AS blocks,
    COALESCE(raw.players_misc.foulsPersonal, raw.players_traditional.foulsPersonal) AS foulsPersonal,

    raw.players_traditional.turnovers,
    raw.players_traditional.plusMinusPoints,

    -- 4. Advanced Metrics (NO renaming)
    raw.players_advanced.estimatedOffensiveRating,
    raw.players_advanced.offensiveRating,
    raw.players_advanced.estimatedDefensiveRating,
    raw.players_advanced.defensiveRating,
    raw.players_advanced.estimatedNetRating,
    raw.players_advanced.netRating,
    raw.players_advanced.assistPercentage,
    raw.players_advanced.assistToTurnover,
    raw.players_advanced.assistRatio,

    COALESCE(raw.players_fourfactors.offensiveReboundPercentage, raw.players_advanced.offensiveReboundPercentage) AS offensiveReboundPercentage,
    raw.players_advanced.defensiveReboundPercentage,
    raw.players_advanced.reboundPercentage,

    raw.players_fourfactors.teamTurnoverPercentage,
    COALESCE(raw.players_fourfactors.effectiveFieldGoalPercentage, raw.players_advanced.effectiveFieldGoalPercentage) AS effectiveFieldGoalPercentage,

    raw.players_advanced.trueShootingPercentage,
    raw.players_advanced.usagePercentage,
    raw.players_advanced.estimatedUsagePercentage,
    raw.players_advanced.estimatedPace,
    raw.players_advanced.pace,
    raw.players_advanced.pacePer40,
    raw.players_advanced.possessions,
    raw.players_advanced.PIE,

    -- 5. Misc / Derived
    raw.players_fourfactors.freeThrowAttemptRate,
    raw.players_fourfactors.oppEffectiveFieldGoalPercentage,
    raw.players_fourfactors.oppFreeThrowAttemptRate,
    raw.players_fourfactors.oppTeamTurnoverPercentage,
    raw.players_fourfactors.oppOffensiveReboundPercentage,

    raw.players_misc.pointsOffTurnovers,
    raw.players_misc.pointsSecondChance,
    raw.players_misc.pointsFastBreak,
    raw.players_misc.pointsPaint,
    raw.players_misc.oppPointsOffTurnovers,
    raw.players_misc.oppPointsSecondChance,
    raw.players_misc.oppPointsFastBreak,
    raw.players_misc.oppPointsPaint,
    raw.players_misc.blocksAgainst,
    raw.players_misc.foulsDrawn,

    -- 6. Scoring Percentages
    raw.players_scoring.percentageFieldGoalsAttempted2pt,
    raw.players_scoring.percentageFieldGoalsAttempted3pt,
    raw.players_scoring.percentagePoints2pt,
    raw.players_scoring.percentagePointsMidrange2pt,
    raw.players_scoring.percentagePoints3pt,
    raw.players_scoring.percentagePointsFastBreak,
    raw.players_scoring.percentagePointsFreeThrow,
    raw.players_scoring.percentagePointsOffTurnovers,
    raw.players_scoring.percentagePointsPaint,
    raw.players_scoring.percentageAssisted2pt,
    raw.players_scoring.percentageUnassisted2pt,
    raw.players_scoring.percentageAssisted3pt,
    raw.players_scoring.percentageUnassisted3pt,
    raw.players_scoring.percentageAssistedFGM,
    raw.players_scoring.percentageUnassistedFGM

FROM raw.log_table
LEFT JOIN raw.players_traditional
    ON raw.log_table.GAME_ID::INT = raw.players_traditional.gameId::INT
    AND raw.log_table.TEAM_ABBREVIATION = raw.players_traditional.teamTricode
LEFT JOIN raw.players_advanced
    ON raw.players_traditional.gameId::INT = raw.players_advanced.gameId::INT
    AND raw.players_traditional.personId = raw.players_advanced.personId
LEFT JOIN raw.players_fourfactors
    ON raw.players_advanced.gameId::INT = raw.players_fourfactors.gameId::INT
    AND raw.players_advanced.personId = raw.players_fourfactors.personId
LEFT JOIN raw.players_scoring
    ON raw.players_fourfactors.gameId::INT = raw.players_scoring.gameId::INT
    AND raw.players_fourfactors.personId = raw.players_scoring.personId
LEFT JOIN raw.players_misc
    ON raw.players_scoring.gameId::INT = raw.players_misc.gameId::INT
    AND raw.players_scoring.personId = raw.players_misc.personId;
