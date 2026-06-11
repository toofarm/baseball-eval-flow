-- app_player_batting_percentiles contract: every percentile column is a
-- non-null value within [0, 100]. percent_rank() can only emit values in that
-- range, so a row outside it signals a join fan-out or a cast/rounding bug.

select
    player_id,
    season,
    bat_avg_pctl,
    bat_obp_pctl,
    bat_slg_pctl,
    bat_ops_pctl,
    bat_k_pct_pctl
from {{ ref('app_player_batting_percentiles') }}
where bat_avg_pctl    not between 0 and 100
   or bat_obp_pctl    not between 0 and 100
   or bat_slg_pctl    not between 0 and 100
   or bat_ops_pctl    not between 0 and 100
   or bat_k_pct_pctl  not between 0 and 100
   or bat_avg_pctl    is null
   or bat_obp_pctl    is null
   or bat_slg_pctl    is null
   or bat_ops_pctl    is null
   or bat_k_pct_pctl  is null
