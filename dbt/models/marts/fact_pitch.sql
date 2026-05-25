{{
    config(
        materialized='table',
        unique_key=['game_pk', 'at_bat_index', 'pitch_number'],
        incremental_strategy='merge',
    )
}}

-- Fact at pitch grain. Pre-pitch counts are derived from the API's post-pitch
-- counts via window functions:
--   * balls_before / strikes_before = LAG(post_*) within the at-bat, defaulting
--     to 0 for the first pitch.
--   * outs_before = MIN(post_outs) within the at-bat. Outs only change on the
--     final pitch of an at-bat, so the minimum across the AB is the outs entering
--     it. Falls back to 0 when the API omits outs entirely.

with pitches as (
    select * from {{ ref('stg_play_by_play') }}
),
with_pre_counts as (
    select
        *,
        coalesce(
            lag(post_balls, 1) over (
                partition by game_pk, at_bat_index order by pitch_number
            ),
            0
        ) as balls_before,
        coalesce(
            lag(post_strikes, 1) over (
                partition by game_pk, at_bat_index order by pitch_number
            ),
            0
        ) as strikes_before,
        coalesce(
            min(post_outs) over (partition by game_pk, at_bat_index),
            0
        ) as outs_before
    from pitches
)
select
    game_pk,
    at_bat_index,
    pitch_number,
    pitcher_id,
    batter_id,
    pitch_type_code,
    start_speed,
    end_speed,
    spin_rate,
    spin_direction,
    break_angle,
    break_length,
    break_vertical,
    break_vertical_induced,
    break_horizontal,
    plate_x,
    plate_z,
    zone,
    inning,
    half_inning,
    balls_before,
    strikes_before,
    outs_before,
    bat_side,
    pitch_hand,
    is_strike,
    is_ball,
    is_in_play,
    call_code,
    at_bat_event
from with_pre_counts
order by game_pk, at_bat_index, pitch_number
