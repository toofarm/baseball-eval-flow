
  
    

  create  table "airflow"."public"."dim_team__dbt_tmp"
  
  
    as
  
  (
    

with teams_from_home as (
    select distinct home_team_id as team_id, home_name as name
    from "airflow"."public"."stg_schedule"
    where home_team_id is not null
),
teams_from_away as (
    select distinct away_team_id as team_id, away_name as name
    from "airflow"."public"."stg_schedule"
    where away_team_id is not null
),
unioned as (
    select * from teams_from_home
    union
    select * from teams_from_away
)
select
    team_id,
    max(name) as name,
    null::varchar(16) as abbreviation
from unioned
group by team_id
order by team_id
  );
  