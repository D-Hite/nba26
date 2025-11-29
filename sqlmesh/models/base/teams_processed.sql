MODEL (
    name base.teams_processed,
    kind FULL
);

WITH base_data AS (
    SELECT
        *,
        CASE 
            WHEN seasonId::VARCHAR ILIKE '4%' THEN 'PLAYOFFS'
            WHEN seasonId::VARCHAR ILIKE '6%' THEN 'NBA_CUP'
            ELSE 'REGULAR'
        END AS seasonType,
        COALESCE(plusMinus, 0) AS scoreDiff,
        CASE WHEN MATCHUP ILIKE '% vs. %' THEN TRUE ELSE FALSE END AS homeGame,
        TRIM(
          CASE WHEN MATCHUP ILIKE '%vs.%' THEN SPLIT_PART(MATCHUP, 'vs.', 1)
               ELSE SPLIT_PART(MATCHUP, '@', 2)
          END
        ) AS homeTeam,
        TRIM(
          CASE WHEN MATCHUP ILIKE '%vs.%' THEN SPLIT_PART(MATCHUP, 'vs.', 2)
               ELSE SPLIT_PART(MATCHUP, '@', 1)
          END
        ) AS awayTeam
    FROM base.teams_combined
),
with_opponent AS (
    SELECT
        t1.*,
        t2.points AS opponentPoints
    FROM base_data t1
    JOIN base_data t2
        ON t1.gameId = t2.gameId
        AND t1.teamId != t2.teamId
),

outcomes AS (
    SELECT
        *,
        CASE 
            WHEN LINE is NULL then NULL
            WHEN homeGame AND scoreDiff > -LINE THEN 'Cover'
            WHEN NOT homeGame AND scoreDiff > LINE THEN 'Cover'
            ELSE 'No Cover'
        END AS coverResult,
        CASE
            WHEN overUnder IS NULL THEN NULL
            WHEN points + opponentPoints > overUnder THEN 'Over'
            WHEN points + opponentPoints < overUnder THEN 'Under'
            ELSE 'Push'
        END AS overUnderResult
    FROM with_opponent
),

ranked_games AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY teamId, seasonId 
            ORDER BY gameDate
        ) AS gameNumber,
        MAX(gameDate) OVER (
            PARTITION BY teamId, seasonId
        ) AS lastTeamGameDate,

        -- Days since last game
        DATEDIFF(
            'day',
            CAST(LAG(gameDate) OVER (
                PARTITION BY teamId, seasonId
                ORDER BY gameDate
            ) AS DATE),
            CAST(gameDate AS DATE)
        ) AS daysSinceLastGame,


        -- Back-to-back flag
        CASE 
            WHEN DATEDIFF(
                'day',
                CAST(LAG(gameDate) OVER (
                    PARTITION BY teamId, seasonId
                    ORDER BY gameDate
                ) AS DATE),
                CAST(gameDate AS DATE)
            ) = 1 THEN 1 ELSE 0
        END AS isBackToBack,


        -- 3 games in 4 nights
        (
            SELECT COUNT(*)
            FROM outcomes o2
            WHERE o2.teamId = o1.teamId
            AND o2.seasonId = o1.seasonId
            AND CAST(o2.gameDate AS DATE) 
                BETWEEN (CAST(o1.gameDate AS DATE) - INTERVAL 3 DAY) 
                    AND CAST(o1.gameDate AS DATE)
        ) AS gamesLast4Days,

        (
            SELECT COUNT(*)
            FROM outcomes o3
            WHERE o3.teamId = o1.teamId
            AND o3.seasonId = o1.seasonId
            AND CAST(o3.gameDate AS DATE) 
                BETWEEN (CAST(o1.gameDate AS DATE) - INTERVAL 5 DAY) 
                    AND CAST(o1.gameDate AS DATE)
        ) AS gamesLast6Days


    FROM outcomes o1
),



record_agg AS (
    SELECT
        teamId,
        seasonId,
        seasonType,
        gameId,
        gameDate,
        SUM(CASE WHEN winLoss = 'W' THEN 1 ELSE 0 END) OVER (
            PARTITION BY teamId, seasonId, seasonType
            ORDER BY gameDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS winsSoFar,
        SUM(CASE WHEN winLoss = 'L' THEN 1 ELSE 0 END) OVER (
            PARTITION BY teamId, seasonId, seasonType
            ORDER BY gameDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS lossesSoFar,
        ROUND(
            SUM(CASE WHEN winLoss = 'W' THEN 1 ELSE 0 END) OVER (
                PARTITION BY teamId, seasonId, seasonType
                ORDER BY gameDate
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ) * 1.0
            /
            LEAST(10, ROW_NUMBER() OVER (
                PARTITION BY teamId, seasonId, seasonType
                ORDER BY gameDate
            )),
            3
        ) AS last10WinPercentage
    FROM ranked_games
),
-- THIS MAY NEED WORK:: TODO
vs_opponent_record AS (
    SELECT
        r.teamId,
        r.seasonId,
        r.seasonType,
        r.gameId,
        REGEXP_REPLACE(r.MATCHUP, '.*(vs\\.|@) ', '') AS opponentTricode,
        SUM(CASE WHEN r.winLoss = 'W' THEN 1 ELSE 0 END) OVER (
            PARTITION BY r.teamId, opponentTricode, r.seasonId, r.seasonType
            ORDER BY r.gameDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS winsVsOpponent,
        SUM(CASE WHEN r.winLoss = 'L' THEN 1 ELSE 0 END) OVER (
            PARTITION BY r.teamId, opponentTricode, r.seasonId, r.seasonType
            ORDER BY r.gameDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS lossesVsOpponent
    FROM ranked_games AS r
)
SELECT
    -- 1. Metadata / Identifiers
    t1.gameId,
    LEAD(t1.gameId) OVER (
      PARTITION BY t1.seasonId, t1.teamId
      ORDER BY t1.gameDate
    ) AS nextGameId,
    t1.gameDate,
    t1.seasonId,
    t1.seasonType,
    t1.teamId,
    t1.teamTricode,
    t1.teamName,
    t1.teamCity,
    t1.homeTeam,
    t1.awayTeam,
    t1.homeGame,
    t1.MATCHUP,
    t1.gameNumber,
    t1.daysSinceLastGame,
    t1.isBackToBack,
    CASE WHEN t1.gamesLast4Days >= 3 THEN 1 ELSE 0 END AS is3In4,
    CASE WHEN t1.gamesLast6Days >= 4 THEN 1 ELSE 0 END AS is4In6,
    CASE WHEN t1.gameDate = t1.lastTeamGameDate THEN 1 ELSE 0 END AS isLastTeamGame,
    t1.line,
    t1.overUnder,

    -- 2. Outcome Stats
    t1.winLoss,
    t1.points,
    t1.opponentPoints,
    t1.points + t1.opponentPoints as totalPoints,
    t1.scoreDiff,
    t1.coverResult,
    t1.overUnderResult,

    -- 3. Betting Record & Rolling Performance
    r.winsSoFar,
    r.lossesSoFar,
    r.last10WinPercentage,
    v.winsVsOpponent,
    v.lossesVsOpponent,

    -- 4. Core Box Score Stats

    t1.fieldGoalsMade,
    t1.fieldGoalsAttempted,
    t1.fieldGoalsPercentage,
    t1.threePointersMade,
    t1.threePointersAttempted,
    t1.threePointersPercentage,
    t1.freeThrowsMade,
    t1.freeThrowsAttempted,
    t1.freeThrowsPercentage,
    t1.reboundsOffensive,
    t1.reboundsDefensive,
    t1.reboundsTotal,
    t1.assists,
    t1.steals,
    t1.blocks,
    t1.turnovers,
    t1.foulsPersonal,

    -- 5. Advanced Metrics
    t1.estimatedOffensiveRating,
    t1.offensiveRating,
    t1.estimatedDefensiveRating,
    t1.defensiveRating,
    t1.estimatedNetRating,
    t1.netRating,
    t1.assistPercentage,
    t1.assistToTurnover,
    t1.assistRatio,
    t1.offensiveReboundPercentage,
    t1.defensiveReboundPercentage,
    t1.reboundPercentage,
    t1.teamTurnoverPercentage,
    t1.effectiveFieldGoalPercentage,
    t1.trueShootingPercentage,
    t1.usagePercentage,
    t1.estimatedUsagePercentage,
    t1.estimatedPace,
    t1.pace,
    t1.pacePer40,
    t1.possessions,
    t1.PIE,

    -- 6. Miscellaneous Stats
    t1.freeThrowAttemptRate,
    t1.oppEffectiveFieldGoalPercentage,
    t1.oppFreeThrowAttemptRate,
    t1.oppTeamTurnoverPercentage,
    t1.oppOffensiveReboundPercentage,
    t1.pointsOffTurnovers,
    t1.pointsSecondChance,
    t1.pointsFastBreak,
    t1.pointsPaint,
    t1.oppPointsOffTurnovers,
    t1.oppPointsSecondChance,
    t1.oppPointsFastBreak,
    t1.oppPointsPaint,
    t1.blocksAgainst,
    t1.foulsDrawn,

    -- 7. Scoring Breakdown Percentages
    t1.percentageFieldGoalsAttempted2pt,
    t1.percentageFieldGoalsAttempted3pt,
    t1.percentagePoints2pt,
    t1.percentagePointsMidrange2pt,
    t1.percentagePoints3pt,
    t1.percentagePointsFastBreak,
    t1.percentagePointsFreeThrow,
    t1.percentagePointsOffTurnovers,
    t1.percentagePointsPaint,
    t1.percentageAssisted2pt,
    t1.percentageUnassisted2pt,
    t1.percentageAssisted3pt,
    t1.percentageUnassisted3pt,
    t1.percentageAssistedFGM,
    t1.percentageUnassistedFGM

FROM ranked_games t1
LEFT JOIN record_agg r
    ON t1.teamId = r.teamId AND t1.gameId = r.gameId
LEFT JOIN vs_opponent_record v
    ON t1.teamId = v.teamId AND t1.gameId = v.gameId;