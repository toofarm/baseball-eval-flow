{{
    config(
        materialized='table',
        incremental_strategy='merge',
        unique_key=['player_id'],
    )
}}

-- One row per player_id. A player can appear with more than one position_type
-- (e.g. a two-way player logged as both Pitcher and Hitter, or a position
-- player who also served as DH) or with a changed display name, so take the
-- values from their most recent game rather than every distinct combination.
with appearances as (
    select
        p.player_id,
        p.full_name,
        p.position_type,
        row_number() over (
            partition by p.player_id
            order by s.game_date desc nulls last, p.game_pk desc
        ) as rn
    from {{ ref('stg_player_stats') }} p
    left join {{ ref('stg_schedule') }} s on s.game_pk = p.game_pk
    where p.player_id is not null
)
select
    player_id,
    full_name,
    position_type
from appearances
where rn = 1
order by player_id
