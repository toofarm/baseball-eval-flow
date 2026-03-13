{{
    config(
        materialized='view',
    )
}}

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
from {{ source('mlb', 'staging_player_stats') }}
