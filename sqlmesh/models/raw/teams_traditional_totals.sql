MODEL (
  name raw.teams_traditional_totals,
  kind VIEW,
  dialect snowflake
);

WITH agg AS (
    SELECT
        gameId,
        teamId,

        -- static attributes (identical on starters/bench rows)
        MAX(teamCity)      AS teamCity,
        MAX(teamName)      AS teamName,
        MAX(teamTricode)   AS teamTricode,
        MAX(teamSlug)      AS teamSlug,

        -- minutes are strings in the API, but summing makes no sense.
        -- Starters + bench minutes will always equal 240 (team minutes),
        -- but you generally don't want the sum. So we MAX() to keep the
        -- team-level minutes field consistent with old dataset.
        MAX(minutes)       AS minutes,

        -- additive numeric stats (sum starters + bench)
        SUM(fieldGoalsMade)        AS fieldGoalsMade,
        SUM(fieldGoalsAttempted)   AS fieldGoalsAttempted,
        SUM(threePointersMade)     AS threePointersMade,
        SUM(threePointersAttempted)AS threePointersAttempted,
        SUM(freeThrowsMade)        AS freeThrowsMade,
        SUM(freeThrowsAttempted)   AS freeThrowsAttempted,
        SUM(reboundsOffensive)     AS reboundsOffensive,
        SUM(reboundsDefensive)     AS reboundsDefensive,
        SUM(reboundsTotal)         AS reboundsTotal,
        SUM(assists)               AS assists,
        SUM(steals)                AS steals,
        SUM(blocks)                AS blocks,
        SUM(turnovers)             AS turnovers,
        SUM(foulsPersonal)         AS foulsPersonal,
        SUM(points)                AS points

    FROM NBA.RAW.TEAMS_TRADITIONAL
    GROUP BY gameId, teamId
)

SELECT
    gameId,
    teamId,
    teamCity,
    teamName,
    teamTricode,
    teamSlug,
    minutes,

    -- totals
    fieldGoalsMade,
    fieldGoalsAttempted,
    threePointersMade,
    threePointersAttempted,
    freeThrowsMade,
    freeThrowsAttempted,
    reboundsOffensive,
    reboundsDefensive,
    reboundsTotal,
    assists,
    steals,
    blocks,
    turnovers,
    foulsPersonal,
    points,

    -- === Recalculated Percentages (correct way) ===

    CASE 
        WHEN fieldGoalsAttempted = 0 THEN NULL
        ELSE fieldGoalsMade * 1.0 / fieldGoalsAttempted
    END AS fieldGoalsPercentage,

    CASE 
        WHEN threePointersAttempted = 0 THEN NULL
        ELSE threePointersMade * 1.0 / threePointersAttempted
    END AS threePointersPercentage,

    CASE 
        WHEN freeThrowsAttempted = 0 THEN NULL
        ELSE freeThrowsMade * 1.0 / freeThrowsAttempted
    END AS freeThrowsPercentage

FROM agg;
