select * from {{ ref('int_player_stats_enriched') }}
where bat_ab < bat_hits
and bat_ab is not null
and bat_hits is not null