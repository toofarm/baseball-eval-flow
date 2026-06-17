{{
    config(
        materialized='view',
    )
}}

-- Consumer-facing search index: every player and team active in the current
-- season, normalized into one polymorphic shape so the web app can search
-- players and teams from a single Supabase table (trigram similarity).
--
-- "Active" = appeared in a game in the latest season present in dim_game
-- (players via fact_game_state, teams via dim_game home/away). search_text is a
-- lowercased blob of the name + team tokens; Supabase builds a pg_trgm GIN index
-- on it (and on display_name) in the offload step.
--
-- Grain: one row per (entity_type, uid). Dual-dialect: no Snowflake/Postgres-
-- only syntax (see .cursor/rules/dbt-dual-dialect-sql.mdc).

with current_season as (
    select max(season) as season
    from {{ ref('dim_game') }}
),

-- Teams that played a game (home or away) in the current season.
active_team_ids as (
    select g.home_team_id as team_id
    from {{ ref('dim_game') }} g
    cross join current_season cs
    where g.season = cs.season
      and g.home_team_id is not null
    union
    select g.away_team_id as team_id
    from {{ ref('dim_game') }} g
    cross join current_season cs
    where g.season = cs.season
      and g.away_team_id is not null
),

-- Every current-season player appearance, ranked so rn = 1 is the player's
-- most recent game (used to pick their current team + position).
player_appearances as (
    select
        f.player_id,
        f.team_id,
        f.position_name,
        row_number() over (
            partition by f.player_id
            order by g.game_date desc, g.game_pk desc
        ) as rn
    from {{ ref('fact_game_state') }} f
    join {{ ref('dim_game') }} g on g.game_pk = f.game_pk
    cross join current_season cs
    where g.season = cs.season
),

active_players as (
    select player_id, team_id, position_name
    from player_appearances
    where rn = 1
),

players as (
    select
        'player'              as entity_type,
        ap.player_id          as uid,
        dp.full_name          as display_name,
        ap.position_name      as position_name,
        ap.team_id            as team_id,
        dt.name               as team_name,
        dt.abbreviation       as team_abbreviation
    from active_players ap
    join {{ ref('dim_player') }} dp on dp.player_id = ap.player_id
    left join {{ ref('dim_team') }} dt on dt.team_id = ap.team_id
),

teams as (
    select
        'team'                as entity_type,
        dt.team_id            as uid,
        dt.name               as display_name,
        cast(null as varchar) as position_name,
        dt.team_id            as team_id,
        dt.name               as team_name,
        dt.abbreviation       as team_abbreviation
    from active_team_ids t
    join {{ ref('dim_team') }} dt on dt.team_id = t.team_id
),

unioned as (
    select * from players
    union all
    select * from teams
)

select
    entity_type,
    uid,
    display_name,
    position_name,
    team_id,
    team_name,
    team_abbreviation,
    lower(
        coalesce(display_name, '') || ' ' ||
        coalesce(team_abbreviation, '') || ' ' ||
        coalesce(team_name, '')
    ) as search_text
from unioned
