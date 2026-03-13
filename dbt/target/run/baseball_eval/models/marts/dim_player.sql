
  
    

  create  table "airflow"."public"."dim_player__dbt_tmp"
  
  
    as
  
  (
    

select distinct
    player_id,
    full_name,
    position_type
from "airflow"."public"."stg_player_stats"
where player_id is not null
order by player_id
  );
  