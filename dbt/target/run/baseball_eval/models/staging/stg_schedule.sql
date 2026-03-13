
  create view "airflow"."public"."stg_schedule__dbt_tmp"
    
    
  as (
    

select
    game_pk,
    game_date::date as game_date,
    extract(year from game_date::date)::int as season,
    game_type,
    venue_id,
    home_team_id,
    away_team_id,
    home_name,
    away_name,
    winning_team_id
from "airflow"."public"."staging_schedule"
  );