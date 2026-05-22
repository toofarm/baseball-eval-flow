{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['player_id', 'as_of_date', 'window_days'],
    )
}}

{% set as_of_date = var('as_of_date') %}

with fact as (
    select * from {{ ref('fact_game_state') }}
),
dim_game as (
    select * from {{ ref('dim_game') }}
),
constants as (
    select * from {{ ref('dim_stat_constants') }}
),
players_to_update as (
    select distinct f.player_id
    from fact f
    join dim_game g on f.game_pk = g.game_pk
    where g.game_date = '{{ as_of_date }}'::date
),
agg_7 as (
    select
        f.player_id,
        7 as window_days,
        max(g.season) as season,
        sum(coalesce(f.bat_games_played, 0)) as bat_games_played,
        sum(coalesce(f.bat_plate_appearances, 0)) as bat_plate_appearances,
        sum(coalesce(f.bat_at_bats, 0)) as bat_at_bats,
        sum(coalesce(f.bat_runs, 0)) as bat_runs,
        sum(coalesce(f.bat_hits, 0)) as bat_hits,
        sum(coalesce(f.bat_doubles, 0)) as bat_doubles,
        sum(coalesce(f.bat_triples, 0)) as bat_triples,
        sum(coalesce(f.bat_home_runs, 0)) as bat_home_runs,
        sum(coalesce(f.bat_rbi, 0)) as bat_rbi,
        sum(coalesce(f.bat_strike_outs, 0)) as bat_strike_outs,
        sum(coalesce(f.bat_base_on_balls, 0)) as bat_base_on_balls,
        sum(coalesce(f.bat_stolen_bases, 0)) as bat_stolen_bases,
        sum(coalesce(f.bat_caught_stealing, 0)) as bat_caught_stealing,
        sum(coalesce(f.bat_intentional_walks, 0)) as bat_ibb,
        sum(coalesce(f.bat_hit_by_pitch, 0)) as bat_hbp,
        sum(coalesce(f.bat_sac_flies, 0)) as bat_sf,
        sum(coalesce(f.bat_total_bases, 0)) as bat_total_bases,
        sum(coalesce(f.pit_games_played, 0)) as pit_games_played,
        sum(coalesce(f.pit_innings_pitched, 0)) as pit_innings_pitched,
        sum(coalesce(f.pit_wins, 0)) as pit_wins,
        sum(coalesce(f.pit_losses, 0)) as pit_losses,
        sum(coalesce(f.pit_saves, 0)) as pit_saves,
        sum(coalesce(f.pit_hits, 0)) as pit_hits,
        sum(coalesce(f.pit_earned_runs, 0)) as pit_earned_runs,
        sum(coalesce(f.pit_strike_outs, 0)) as pit_strike_outs,
        sum(coalesce(f.pit_base_on_balls, 0)) as pit_bb,
        sum(coalesce(f.pit_fip, 0) * coalesce(f.pit_innings_pitched, 0)) as pit_fip_times_ip,
        sum(coalesce(f.fld_assists, 0)) as fld_assists,
        sum(coalesce(f.fld_put_outs, 0)) as fld_put_outs,
        sum(coalesce(f.fld_errors, 0)) as fld_errors,
        sum(coalesce(f.fld_chances, 0)) as fld_chances
    from fact f
    join dim_game g on f.game_pk = g.game_pk
    where g.game_date > dateadd('day', -7, '{{ as_of_date }}'::date)
      and g.game_date <= '{{ as_of_date }}'::date
      and f.player_id in (select player_id from players_to_update)
    group by f.player_id
    having (sum(coalesce(f.bat_games_played, 0)) + sum(coalesce(f.pit_games_played, 0)) + sum(coalesce(f.fld_chances, 0))) > 0
),
agg_30 as (
    select
        f.player_id,
        30 as window_days,
        max(g.season) as season,
        sum(coalesce(f.bat_games_played, 0)) as bat_games_played,
        sum(coalesce(f.bat_plate_appearances, 0)) as bat_plate_appearances,
        sum(coalesce(f.bat_at_bats, 0)) as bat_at_bats,
        sum(coalesce(f.bat_runs, 0)) as bat_runs,
        sum(coalesce(f.bat_hits, 0)) as bat_hits,
        sum(coalesce(f.bat_doubles, 0)) as bat_doubles,
        sum(coalesce(f.bat_triples, 0)) as bat_triples,
        sum(coalesce(f.bat_home_runs, 0)) as bat_home_runs,
        sum(coalesce(f.bat_rbi, 0)) as bat_rbi,
        sum(coalesce(f.bat_strike_outs, 0)) as bat_strike_outs,
        sum(coalesce(f.bat_base_on_balls, 0)) as bat_base_on_balls,
        sum(coalesce(f.bat_stolen_bases, 0)) as bat_stolen_bases,
        sum(coalesce(f.bat_caught_stealing, 0)) as bat_caught_stealing,
        sum(coalesce(f.bat_intentional_walks, 0)) as bat_ibb,
        sum(coalesce(f.bat_hit_by_pitch, 0)) as bat_hbp,
        sum(coalesce(f.bat_sac_flies, 0)) as bat_sf,
        sum(coalesce(f.bat_total_bases, 0)) as bat_total_bases,
        sum(coalesce(f.pit_games_played, 0)) as pit_games_played,
        sum(coalesce(f.pit_innings_pitched, 0)) as pit_innings_pitched,
        sum(coalesce(f.pit_wins, 0)) as pit_wins,
        sum(coalesce(f.pit_losses, 0)) as pit_losses,
        sum(coalesce(f.pit_saves, 0)) as pit_saves,
        sum(coalesce(f.pit_hits, 0)) as pit_hits,
        sum(coalesce(f.pit_earned_runs, 0)) as pit_earned_runs,
        sum(coalesce(f.pit_strike_outs, 0)) as pit_strike_outs,
        sum(coalesce(f.pit_base_on_balls, 0)) as pit_bb,
        sum(coalesce(f.pit_fip, 0) * coalesce(f.pit_innings_pitched, 0)) as pit_fip_times_ip,
        sum(coalesce(f.fld_assists, 0)) as fld_assists,
        sum(coalesce(f.fld_put_outs, 0)) as fld_put_outs,
        sum(coalesce(f.fld_errors, 0)) as fld_errors,
        sum(coalesce(f.fld_chances, 0)) as fld_chances
    from fact f
    join dim_game g on f.game_pk = g.game_pk
    where g.game_date > dateadd('day', -30, '{{ as_of_date }}'::date)
      and g.game_date <= '{{ as_of_date }}'::date
      and f.player_id in (select player_id from players_to_update)
    group by f.player_id
    having (sum(coalesce(f.bat_games_played, 0)) + sum(coalesce(f.pit_games_played, 0)) + sum(coalesce(f.fld_chances, 0))) > 0
),
combined as (
    select * from agg_7
    union all
    select * from agg_30
),
-- Closest-season lookup. Pre-rank constants once per distinct season
-- (Snowflake can't decorrelate `lateral ... order by ... limit 1`).
seasons_in_use as (
    select distinct season from combined
),
ranked_constants as (
    select
        s.season as p_season,
        c.woba,
        c.woba_scale,
        c.w_bb,
        c.w_hbp,
        c.w_1b,
        c.w_2b,
        c.w_3b,
        c.w_hr,
        c.r_per_pa,
        row_number() over (partition by s.season order by abs(c.season - s.season)) as rn
    from seasons_in_use s
    cross join constants c
),
closest_constant as (
    select * from ranked_constants where rn = 1
),
with_constants as (
    select
        a.*,
        c.woba as c_woba,
        c.woba_scale as c_woba_scale,
        c.w_bb as c_w_bb,
        c.w_hbp as c_w_hbp,
        c.w_1b as c_w_1b,
        c.w_2b as c_w_2b,
        c.w_3b as c_w_3b,
        c.w_hr as c_w_hr,
        c.r_per_pa as c_r_per_pa
    from combined a
    join closest_constant c on c.p_season = a.season
)
select
    w.player_id,
    '{{ as_of_date }}'::date as as_of_date,
    w.window_days,
    nullif(w.bat_games_played, 0) as bat_games_played,
    nullif(w.bat_plate_appearances, 0) as bat_plate_appearances,
    nullif(w.bat_at_bats, 0) as bat_at_bats,
    nullif(w.bat_runs, 0) as bat_runs,
    nullif(w.bat_hits, 0) as bat_hits,
    nullif(w.bat_doubles, 0) as bat_doubles,
    nullif(w.bat_triples, 0) as bat_triples,
    nullif(w.bat_home_runs, 0) as bat_home_runs,
    nullif(w.bat_rbi, 0) as bat_rbi,
    nullif(w.bat_strike_outs, 0) as bat_strike_outs,
    nullif(w.bat_base_on_balls, 0) as bat_base_on_balls,
    nullif(w.bat_stolen_bases, 0) as bat_stolen_bases,
    nullif(w.bat_caught_stealing, 0) as bat_caught_stealing,
    case when w.bat_at_bats > 0 then round(w.bat_hits::numeric / w.bat_at_bats, 4) end as bat_avg,
    case
        when w.bat_at_bats > 0 and (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp) > 0
        then round(
            (w.bat_base_on_balls + w.bat_hbp + w.bat_hits)::numeric / (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp)
            + w.bat_total_bases::numeric / w.bat_at_bats,
            4
        )
        else null
    end as bat_ops,
    case
        when (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp) > 0
        then round(
            (w.c_w_bb * w.bat_base_on_balls + w.c_w_hbp * w.bat_hbp
             + w.c_w_1b * (w.bat_hits - w.bat_home_runs - w.bat_doubles - w.bat_triples)
             + w.c_w_2b * w.bat_doubles + w.c_w_3b * w.bat_triples + w.c_w_hr * w.bat_home_runs)
            ::numeric / (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp),
            4
        )
        else null
    end as bat_woba,
    case
        when w.bat_plate_appearances > 0 and (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp) > 0
        then round(
            ((w.c_w_bb * w.bat_base_on_balls + w.c_w_hbp * w.bat_hbp
              + w.c_w_1b * (w.bat_hits - w.bat_home_runs - w.bat_doubles - w.bat_triples)
              + w.c_w_2b * w.bat_doubles + w.c_w_3b * w.bat_triples + w.c_w_hr * w.bat_home_runs)
             ::numeric / (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp) - w.c_woba)
            / w.c_woba_scale + w.c_r_per_pa * w.bat_plate_appearances,
            2
        )
        else null
    end as bat_wrc_plus,
    nullif(w.pit_games_played, 0) as pit_games_played,
    case when w.pit_innings_pitched > 0 then round(w.pit_innings_pitched::numeric, 2) else null end as pit_innings_pitched,
    nullif(w.pit_wins, 0) as pit_wins,
    nullif(w.pit_losses, 0) as pit_losses,
    nullif(w.pit_saves, 0) as pit_saves,
    nullif(w.pit_hits, 0) as pit_hits,
    nullif(w.pit_earned_runs, 0) as pit_earned_runs,
    nullif(w.pit_strike_outs, 0) as pit_strike_outs,
    nullif(w.pit_bb, 0) as pit_base_on_balls,
    case when w.pit_innings_pitched > 0 then round(9.0 * w.pit_earned_runs / w.pit_innings_pitched, 2) else null end as pit_era,
    case when w.pit_innings_pitched > 0 then round(w.pit_fip_times_ip / w.pit_innings_pitched, 2) else null end as pit_fip,
    case when w.pit_innings_pitched > 0 then round((w.pit_hits + w.pit_bb)::numeric / w.pit_innings_pitched, 2) else null end as pit_whip,
    nullif(w.fld_assists, 0) as fld_assists,
    nullif(w.fld_put_outs, 0) as fld_put_outs,
    nullif(w.fld_errors, 0) as fld_errors,
    nullif(w.fld_chances, 0) as fld_chances
from with_constants w
