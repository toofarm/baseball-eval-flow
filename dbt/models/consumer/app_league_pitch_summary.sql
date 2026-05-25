{{
    config(
        materialized='view',
    )
}}

-- League-wide rollup of pitch metrics. Grain: (season, pitch_type_code).
--
-- Aggregates fact_pitch directly (rather than pitcher_pitch_arsenal) so the
-- league averages are pitch-weighted: a pitcher who threw 1,000 fastballs
-- contributes proportionally more to the league average velocity than a
-- pitcher who threw 50. The alternative (average-of-pitcher-averages) would
-- weight every pitcher equally and skew toward small-sample relievers.
--
-- pct_of_league_mix is each pitch type's share of total league pitches in the
-- season. Classified pitches only (null pitch_type_code excluded), so the
-- percentages sum to 100 within each season.

with pitches as (
    select
        f.pitcher_id,
        f.pitch_type_code,
        f.start_speed,
        f.spin_rate,
        f.break_vertical_induced,
        f.break_horizontal,
        f.call_code,
        f.is_in_play,
        f.at_bat_event,
        g.season
    from {{ ref('fact_pitch') }} f
    join {{ ref('dim_game') }} g on g.game_pk = f.game_pk
    where f.pitch_type_code is not null
),
by_type as (
    select
        season,
        pitch_type_code,
        count(*)                                                                   as n_pitches,
        count(distinct pitcher_id)                                                 as n_pitchers,
        avg(start_speed)::numeric(5, 2)                                            as avg_start_speed,
        avg(spin_rate)::numeric(7, 1)                                              as avg_spin_rate,
        avg(break_vertical_induced)::numeric(5, 2)                                 as avg_break_vertical_induced,
        avg(break_horizontal)::numeric(5, 2)                                       as avg_break_horizontal,
        sum(case when call_code in ('S', 'W', 'T') then 1 else 0 end)              as n_swinging_strike,
        sum(case when call_code = 'C' then 1 else 0 end)                           as n_called_strike,
        sum(case when is_in_play then 1 else 0 end)                                as n_in_play,
        sum(case when is_in_play and at_bat_event = 'Home Run' then 1 else 0 end)  as n_home_run
    from pitches
    group by season, pitch_type_code
),
season_totals as (
    select
        season,
        sum(n_pitches) as total_pitches
    from by_type
    group by season
)
select
    b.season,
    b.pitch_type_code,
    pt.pitch_type_name,
    pt.pitch_family,
    b.n_pitches,
    b.n_pitchers,
    round(100.0 * b.n_pitches / st.total_pitches, 2)::numeric(5, 2)                as pct_of_league_mix,
    b.avg_start_speed,
    b.avg_spin_rate,
    b.avg_break_vertical_induced,
    b.avg_break_horizontal,
    round(100.0 * b.n_swinging_strike / b.n_pitches, 2)::numeric(5, 2)             as pct_swinging_strike,
    round(100.0 * b.n_called_strike / b.n_pitches, 2)::numeric(5, 2)               as pct_called_strike,
    round(100.0 * b.n_in_play / b.n_pitches, 2)::numeric(5, 2)                     as pct_in_play,
    round(100.0 * b.n_home_run / b.n_pitches, 4)::numeric(7, 4)                    as pct_home_run
from by_type b
join season_totals st on st.season = b.season
left join {{ ref('dim_pitch_type') }} pt on pt.pitch_type_code = b.pitch_type_code
order by b.season, b.pitch_type_code
