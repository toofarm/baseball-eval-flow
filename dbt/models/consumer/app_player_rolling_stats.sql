{{
    config(
        materialized='view',
    )
}}

-- App-facing denormalization of player_rolling_stats. Adds player_name from
-- dim_player so the web app doesn't need to join on every read. Grain unchanged:
-- (player_id, as_of_date, window_days).

select
    r.player_id,
    p.full_name as player_name,
    r.as_of_date,
    r.window_days,
    r.bat_games_played,
    r.bat_plate_appearances,
    r.bat_at_bats,
    r.bat_runs,
    r.bat_hits,
    r.bat_doubles,
    r.bat_triples,
    r.bat_home_runs,
    r.bat_rbi,
    r.bat_strike_outs,
    r.bat_base_on_balls,
    r.bat_stolen_bases,
    r.bat_caught_stealing,
    r.bat_avg,
    r.bat_ops,
    r.bat_woba,
    r.bat_wrc_plus,
    r.pit_games_played,
    r.pit_innings_pitched,
    r.pit_wins,
    r.pit_losses,
    r.pit_saves,
    r.pit_hits,
    r.pit_earned_runs,
    r.pit_strike_outs,
    r.pit_base_on_balls,
    r.pit_era,
    r.pit_fip,
    r.pit_whip,
    r.fld_assists,
    r.fld_put_outs,
    r.fld_errors,
    r.fld_chances
from {{ ref('player_rolling_stats') }} r
left join {{ ref('dim_player') }} p on p.player_id = r.player_id
