{{
    config(
        materialized='view',
    )
}}

-- App-facing denormalization of pitcher_pitch_arsenal. Adds pitcher_name from
-- dim_player and pitch_type_name + pitch_family from dim_pitch_type so the web
-- app can render a row without joining. Grain unchanged: (season, pitcher_id, pitch_type_code).

select
    a.pitcher_id,
    p.full_name                       as pitcher_name,
    a.season,
    a.pitch_type_code,
    pt.pitch_type_name,
    pt.pitch_family,
    a.n_pitches,
    a.usage_pct,
    a.avg_start_speed,
    a.avg_spin_rate,
    a.avg_break_vertical_induced,
    a.avg_break_horizontal,
    a.pct_swinging_strike,
    a.pct_called_strike,
    a.pct_in_play,
    a.pct_home_run
from {{ ref('pitcher_pitch_arsenal') }} a
left join {{ ref('dim_player') }} p     on p.player_id = a.pitcher_id
left join {{ ref('dim_pitch_type') }} pt on pt.pitch_type_code = a.pitch_type_code
