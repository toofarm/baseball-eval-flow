-- app_player_batting_percentiles grain contract: one row per (player_id, season).
-- A duplicate means the dim_player join fanned out (e.g. a non-unique player_id).

select
    player_id,
    season,
    count(*) as n_rows
from {{ ref('app_player_batting_percentiles') }}
group by player_id, season
having count(*) > 1
