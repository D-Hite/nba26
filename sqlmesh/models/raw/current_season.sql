MODEL (
  name raw.current_season,
  kind VIEW,
  dialect snowflake
);
select
max(season_id) as seasonId
from nba.raw.log_table
where season_id ilike '2%'