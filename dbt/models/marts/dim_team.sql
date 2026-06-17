{{
    config(
        materialized='table',
        incremental_strategy='merge',
        unique_key=['team_id'],
    )
}}

with teams_from_home as (
    select distinct home_team_id as team_id, home_name as name
    from {{ ref('stg_schedule') }}
    where home_team_id is not null
),
teams_from_away as (
    select distinct away_team_id as team_id, away_name as name
    from {{ ref('stg_schedule') }}
    where away_team_id is not null
),
unioned as (
    select * from teams_from_home
    union
    select * from teams_from_away
),
teams as (
    select
        team_id,
        max(name) as name
    from unioned
    group by team_id
)
select
    t.team_id,
    t.name,
    a.abbreviation
from teams t
left join {{ ref('team_abbreviation') }} a on a.team_id = t.team_id
order by t.team_id