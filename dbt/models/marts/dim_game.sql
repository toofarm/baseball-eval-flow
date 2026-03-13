{{
    config(
        materialized='table',
        unique_key='(game_pk)',
        incremental_strategy='merge'
    )
}}

select
    game_pk,
    game_date,
    season,
    game_type,
    venue_id,
    home_team_id,
    away_team_id,
    winning_team_id
from {{ ref('stg_schedule') }}
order by game_pk

{% if is_incremental() %}
and date:game_date >= (select max(date) - interval '3 days' from {{ this }})
{% endif %}
