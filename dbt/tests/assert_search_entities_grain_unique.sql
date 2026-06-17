-- app_search_entities grain contract: one row per (entity_type, uid).
-- A duplicate means a join fanned out (e.g. dim_player/dim_team returned more
-- than one row per id, or a player resolved to multiple "current" teams).

select
    entity_type,
    uid,
    count(*) as n_rows
from {{ ref('app_search_entities') }}
group by entity_type, uid
having count(*) > 1
