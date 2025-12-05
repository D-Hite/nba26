MODEL (
  name fantasy.performance_delta,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column gameDate
  ),
  description "Signals players who are outperforming their season averages; ideal for fantasy pickups."
);

WITH base AS (
    SELECT
        bs.seasonId,
        bs.gameId,
        bs.gameDate,
        bs.personId,
        bs.firstName,
        bs.familyName,
        bs.playedFlag,
        bs.minutes,
        bs.points,
        bs.assists,
        bs.steals,
        bs.blocks,
        bs.reboundsTotal,
        bs.threePointersMade
    FROM fantasy.base_stats bs
),

avg AS (
    SELECT
        seasonId,
        gameId,
        gameDate,
        personId,
        average_minutes,
        average_points,
        average_assists,
        average_steals,
        average_blocks,
        average_total_rebounds,
        average_three_pointers_made
    FROM fantasy.base_stats_averages
),

joined AS (
    SELECT
        b.*,
        a.average_minutes,
        a.average_points,
        a.average_assists,
        a.average_steals,
        a.average_blocks,
        a.average_total_rebounds,
        a.average_three_pointers_made,

        -----------------------------------------------------------------
        -- Last 3-game rolling averages (fantasy-relevant stats)
        -----------------------------------------------------------------
        AVG(b.minutes) OVER (
            PARTITION BY b.personId
            ORDER BY b.gameDate
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS minutes_last3,

        AVG(b.points) OVER (
            PARTITION BY b.personId
            ORDER BY b.gameDate
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS points_last3,

        AVG(b.assists) OVER (
            PARTITION BY b.personId
            ORDER BY b.gameDate
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS assists_last3,

        AVG(b.steals) OVER (
            PARTITION BY b.personId
            ORDER BY b.gameDate
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS steals_last3,

        AVG(b.blocks) OVER (
            PARTITION BY b.personId
            ORDER BY b.gameDate
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS blocks_last3,

        AVG(b.reboundsTotal) OVER (
            PARTITION BY b.personId
            ORDER BY b.gameDate
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rebounds_last3,

        AVG(b.threePointersMade) OVER (
            PARTITION BY b.personId
            ORDER BY b.gameDate
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS threes_last3
    FROM base b
    LEFT JOIN avg a
           ON b.personId = a.personId
          AND b.gameId = a.gameId
)

SELECT
    *,
    -----------------------------------------------------------------
    -- DELTAS (Recent games – season averages)
    -----------------------------------------------------------------
    minutes_last3 - average_minutes AS minutes_delta,
    points_last3 - average_points AS points_delta,
    assists_last3 - average_assists AS assists_delta,
    steals_last3 - average_steals AS steals_delta,
    blocks_last3 - average_blocks AS blocks_delta,
    rebounds_last3 - average_total_rebounds AS rebounds_delta,
    threes_last3 - average_three_pointers_made AS threes_delta,

    -----------------------------------------------------------------
    -- Pickup Signal Score (tunable)
    -----------------------------------------------------------------
    (
       0.40 * (minutes_last3 - average_minutes) +
       0.30 * (points_last3 - average_points) +
       0.15 * (assists_last3 - average_assists) +
       0.10 * (rebounds_last3 - average_total_rebounds) +
       0.05 * ((steals_last3 - average_steals) + (blocks_last3 - average_blocks))
    ) AS pickup_score

FROM joined
WHERE playedFlag = 1;
