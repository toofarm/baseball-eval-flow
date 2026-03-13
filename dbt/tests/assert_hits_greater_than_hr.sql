select * from {{ ref('int_player_stats_enriched') }}
where bat_hits < bat_hr
and bat_hits is not null
and bat_hr is not null