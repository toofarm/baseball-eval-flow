
  create view "airflow"."public"."stg_player_stats__dbt_tmp"
    
    
  as (
    

select
    game_pk,
    player_id,
    team_id,
    full_name,
    position_type,
    position_code,
    position_name,
    batting,
    pitching,
    fielding
from "airflow"."public"."staging_player_stats"
  );