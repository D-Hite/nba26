MODEL (
  name fantasy.averages_current,
  kind VIEW,
  description 'One row per player/season with season-to-date averages as of their last game.'
);

SELECT *
FROM fantasy.averages
WHERE isLastGame = 1;
