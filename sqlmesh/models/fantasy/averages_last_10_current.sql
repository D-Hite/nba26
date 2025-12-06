MODEL (
  name fantasy.averages_last_10_current,
  kind VIEW,
  description 'One row per player/season with last-10-game averages as of their last game.'
);

SELECT *
FROM fantasy.averages_last_10
WHERE isLastGame = 1
  AND games_in_window >= 10;
