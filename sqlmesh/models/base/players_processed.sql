MODEL (
    name base.players_processed,
    kind FULL
);

WITH minute_table AS (
    SELECT
        pc.*,
        CASE
            WHEN pc.minutes IS NULL OR pc.minutes = '' THEN '00:00'
            ELSE pc.minutes
        END AS minutesStr
    FROM base.players_combined pc
),
player_games AS (
    SELECT
        *,
        CASE
            WHEN
                TRY_CAST(SPLIT_PART(minutesStr, ':', 1) AS DOUBLE) > 0
                OR TRY_CAST(SPLIT_PART(minutesStr, ':', 2) AS DOUBLE) > 0
            THEN 1
            ELSE 0
        END AS playedFlag,
        LEAD(gameId) OVER (
            PARTITION BY personId, seasonId
            ORDER BY gameDate
        ) AS nextGameId
    FROM minute_table
),
ranked_games AS (
    SELECT
        *,
        -- Sequential game count (includes DNPs)
        ROW_NUMBER() OVER (
            PARTITION BY personId, seasonId
            ORDER BY gameDate
        ) AS gameCount,

        -- Cumulative count of games played
        SUM(
            CASE
                WHEN playedFlag = 1 THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY personId, seasonId
            ORDER BY gameDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS gamesPlayed,

        -- Row number for most recent *played* game
        ROW_NUMBER() OVER (
            PARTITION BY personId, seasonId
            ORDER BY CASE WHEN playedFlag = 1 THEN gameDate ELSE NULL END DESC
        ) AS rnPlayed
    FROM player_games
)

SELECT
    -- base player metadata from players_combined
    rg.seasonId,
    rg.teamId,
    rg.teamAbbreviation,
    rg.teamName,
    rg.gameId,
    rg.nextGameId,
    rg.gameDate,
    rg.teamCity,
    rg.personId,
    rg.firstName,
    rg.familyName,
    rg.playerSlug,
    rg.position,
    rg.comment,

    -- sequence / flags
    rg.gameCount,
    rg.playedFlag,
    rg.gamesPlayed,
    CASE
        WHEN rg.rnPlayed = 1 AND rg.playedFlag = 1 THEN 1
        ELSE 0
    END AS isLastGame,

    -- game context from log
    rg.matchup,
    rg.winLoss,

    -- minutes as numeric (hours)
    COALESCE(
        TRY_CAST(SPLIT_PART(rg.minutesStr, ':', 1) AS DOUBLE)
        + TRY_CAST(SPLIT_PART(rg.minutesStr, ':', 2) AS DOUBLE) / 60,
        0
    ) AS minutes,

    -- all player stats exactly as in players_combined (no renaming)
    rg.points,
    rg.fieldGoalsMade,
    rg.fieldGoalsAttempted,
    rg.fieldGoalsPercentage,
    rg.threePointersMade,
    rg.threePointersAttempted,
    rg.threePointersPercentage,
    rg.freeThrowsMade,
    rg.freeThrowsAttempted,
    rg.freeThrowsPercentage,
    rg.reboundsOffensive,
    rg.reboundsDefensive,
    rg.reboundsTotal,
    rg.assists,
    rg.steals,
    rg.blocks,
    rg.foulsPersonal,
    rg.turnovers,
    rg.plusMinusPoints,

    rg.estimatedOffensiveRating,
    rg.offensiveRating,
    rg.estimatedDefensiveRating,
    rg.defensiveRating,
    rg.estimatedNetRating,
    rg.netRating,
    rg.assistPercentage,
    rg.assistToTurnover,
    rg.assistRatio,
    rg.offensiveReboundPercentage,
    rg.defensiveReboundPercentage,
    rg.reboundPercentage,
    rg.teamTurnoverPercentage,
    rg.effectiveFieldGoalPercentage,
    rg.trueShootingPercentage,
    rg.usagePercentage,
    rg.estimatedUsagePercentage,
    rg.estimatedPace,
    rg.pace,
    rg.pacePer40,
    rg.possessions,
    rg.PIE,

    rg.freeThrowAttemptRate,
    rg.oppEffectiveFieldGoalPercentage,
    rg.oppFreeThrowAttemptRate,
    rg.oppTeamTurnoverPercentage,
    rg.oppOffensiveReboundPercentage,
    rg.pointsOffTurnovers,
    rg.pointsSecondChance,
    rg.pointsFastBreak,
    rg.pointsPaint,
    rg.oppPointsOffTurnovers,
    rg.oppPointsSecondChance,
    rg.oppPointsFastBreak,
    rg.oppPointsPaint,
    rg.blocksAgainst,
    rg.foulsDrawn,

    rg.percentageFieldGoalsAttempted2pt,
    rg.percentageFieldGoalsAttempted3pt,
    rg.percentagePoints2pt,
    rg.percentagePointsMidrange2pt,
    rg.percentagePoints3pt,
    rg.percentagePointsFastBreak,
    rg.percentagePointsFreeThrow,
    rg.percentagePointsOffTurnovers,
    rg.percentagePointsPaint,
    rg.percentageAssisted2pt,
    rg.percentageUnassisted2pt,
    rg.percentageAssisted3pt,
    rg.percentageUnassisted3pt,
    rg.percentageAssistedFGM,
    rg.percentageUnassistedFGM,

    -- context from teams_processed (aliased to camelCase, no conflicts)
    tp.homeGame,
    tp.isBackToBack,
    tp.is3In4,
    tp.is4In6,
    tp.daysSinceLastGame,
    tp.isLastTeamGame,
    -- tp.line,
    -- tp.overUnder,
    tp.winsSoFar,
    tp.lossesSoFar,
    tp.last10WinPercentage,
    tp.winsVsOpponent,
    tp.lossesVsOpponent

FROM ranked_games rg
LEFT JOIN base.teams_processed tp
    ON rg.gameId = tp.gameId
   AND rg.teamId = tp.teamId
WHERE rg.personId IS NOT NULL;
