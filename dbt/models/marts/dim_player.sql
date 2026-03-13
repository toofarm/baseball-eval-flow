{{
    config(
        materialized='table',
        incremental_strategy='merge',
        unique_key=['player_id'],
    )
}}

select distinct
    player_id,
    full_name,
    position_type
from {{ ref('stg_player_stats') }}
where player_id is not null
order by player_id