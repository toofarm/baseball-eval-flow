
  
    

  create  table "airflow"."public"."dim_game__dbt_tmp"
  
  
    as
  
  (
    

select
    game_pk,
    game_date,
    season,
    game_type,
    venue_id,
    home_team_id,
    away_team_id,
    winning_team_id
from "airflow"."public"."stg_schedule"
order by game_pk


  );
  