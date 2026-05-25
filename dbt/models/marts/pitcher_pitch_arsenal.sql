{{
    config(
        materialized='view',
    )
}}

-- Per-pitcher pitch-arsenal roll-up. Grain: (season, pitcher_id, pitch_type_code).
--
-- usage_pct is computed against the pitcher's CLASSIFIED pitches only (pitches
-- where pitch_type_code is null are excluded from both numerator and denominator).
-- This matches the standard convention — "of the pitches we can classify, what
-- fraction was this type?" — and keeps per-pitcher percentages summing to 100.

with pitches as (
    select
        f.pitcher_id,
        f.pitch_type_code,
        f.start_speed,
        f.spin_rate,
        g.season
    from {{ ref('fact_pitch') }} f
    join {{ ref('dim_game') }} g on g.game_pk = f.game_pk
    where f.pitch_type_code is not null
),
by_type as (
    select
        pitcher_id,
        season,
        pitch_type_code,
        count(*)                                   as n_pitches,
        avg(start_speed)::numeric(5, 2)            as avg_start_speed,
        avg(spin_rate)::numeric(7, 1)              as avg_spin_rate
    from pitches
    group by pitcher_id, season, pitch_type_code
),
totals as (
    select
        pitcher_id,
        season,
        sum(n_pitches) as total_pitches
    from by_type
    group by pitcher_id, season
)
select
    b.pitcher_id,
    b.season,
    b.pitch_type_code,
    b.n_pitches,
    round(100.0 * b.n_pitches / t.total_pitches, 2)::numeric(5, 2) as usage_pct,
    b.avg_start_speed,
    b.avg_spin_rate
from by_type b
join totals t on t.pitcher_id = b.pitcher_id and t.season = b.season
order by b.pitcher_id, b.season, b.pitch_type_code
