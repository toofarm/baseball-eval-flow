{{
    config(
        materialized='view',
    )
}}

-- League-wide batting summary. Grain: (season). One row per season showing
-- aggregate counting stats, slash-line rates, and league-average derived
-- sabermetrics. The web app uses this as the baseline batters are compared
-- against.
--
-- Aggregates fact_game_state directly so the rate metrics are PA-weighted
-- (sum the counts, then compute the rate from the sums). Average-of-player-
-- averages would mis-weight short-stint or platoon hitters.
--
-- Scope: regular-season games only (dim_game.game_type = 'R'). Post-season,
-- spring training, exhibition, and all-star games are excluded so the league
-- baseline matches the conventional "MLB regular-season average" reference.
--
-- Note on wRC+: computed as the true normalized index per FanGraphs (without
-- park-factor adjustment, which we don't have):
--   wRC+ = ((wOBA - lgwOBA) / wOBAScale + lgR/PA) / lgR/PA * 100
-- At this grain the result is tautologically ~100 (calibration check, not an
-- analytic value at season-league grain). The same formula is used by
-- int_player_stats_enriched and player_rolling_stats, so per-player wRC+ on
-- fact_game_state and rolling windows are comparable to this baseline.

with games as (
    select game_pk, season, game_type from {{ ref('dim_game') }}
),
constants as (
    select * from {{ ref('dim_stat_constants') }}
),
per_player_game as (
    select
        g.season,
        f.player_id,
        coalesce(f.bat_plate_appearances, 0)    as pa,
        coalesce(f.bat_at_bats, 0)              as ab,
        coalesce(f.bat_hits, 0)                 as hits,
        coalesce(f.bat_doubles, 0)              as doubles,
        coalesce(f.bat_triples, 0)              as triples,
        coalesce(f.bat_home_runs, 0)            as home_runs,
        coalesce(f.bat_strike_outs, 0)          as strike_outs,
        coalesce(f.bat_base_on_balls, 0)        as base_on_balls,
        coalesce(f.bat_intentional_walks, 0)    as intentional_walks,
        coalesce(f.bat_hit_by_pitch, 0)         as hit_by_pitch,
        coalesce(f.bat_sac_flies, 0)            as sac_flies,
        coalesce(f.bat_sac_bunts, 0)            as sac_bunts,
        coalesce(f.bat_runs, 0)                 as runs,
        coalesce(f.bat_rbi, 0)                  as rbi,
        coalesce(f.bat_stolen_bases, 0)         as stolen_bases,
        coalesce(f.bat_caught_stealing, 0)      as caught_stealing,
        coalesce(f.bat_total_bases, 0)          as total_bases,
        coalesce(f.bat_fly_outs, 0)             as fly_outs
    from {{ ref('fact_game_state') }} f
    join games g on g.game_pk = f.game_pk
    where coalesce(f.bat_plate_appearances, 0) > 0
      and g.game_type = 'R'
),
by_season as (
    select
        season,
        count(distinct player_id) as n_batters,
        sum(pa)                   as bat_plate_appearances,
        sum(ab)                   as bat_at_bats,
        sum(hits)                 as bat_hits,
        sum(doubles)              as bat_doubles,
        sum(triples)              as bat_triples,
        sum(home_runs)            as bat_home_runs,
        sum(strike_outs)          as bat_strike_outs,
        sum(base_on_balls)        as bat_base_on_balls,
        sum(intentional_walks)    as bat_intentional_walks,
        sum(hit_by_pitch)         as bat_hit_by_pitch,
        sum(sac_flies)            as bat_sac_flies,
        sum(sac_bunts)            as bat_sac_bunts,
        sum(runs)                 as bat_runs,
        sum(rbi)                  as bat_rbi,
        sum(stolen_bases)         as bat_stolen_bases,
        sum(caught_stealing)      as bat_caught_stealing,
        sum(total_bases)          as bat_total_bases,
        sum(fly_outs)             as bat_fly_outs
    from per_player_game
    group by season
),
-- Closest-season constant lookup, same pattern as int_player_stats_enriched.
seasons_in_use as (
    select distinct season from by_season
),
ranked_constants as (
    select
        s.season as p_season,
        c.woba, c.woba_scale,
        c.w_bb, c.w_hbp, c.w_1b, c.w_2b, c.w_3b, c.w_hr,
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
        b.*,
        c.woba         as c_woba,
        c.woba_scale   as c_woba_scale,
        c.w_bb         as c_w_bb,
        c.w_hbp        as c_w_hbp,
        c.w_1b         as c_w_1b,
        c.w_2b         as c_w_2b,
        c.w_3b         as c_w_3b,
        c.w_hr         as c_w_hr,
        c.r_per_pa     as c_r_per_pa
    from by_season b
    left join closest_constant c on c.p_season = b.season
)
select
    season,
    n_batters,
    -- Counting (league totals)
    bat_plate_appearances,
    bat_at_bats,
    bat_hits,
    bat_doubles,
    bat_triples,
    bat_home_runs,
    bat_strike_outs,
    bat_base_on_balls,
    bat_intentional_walks,
    bat_hit_by_pitch,
    bat_sac_flies,
    bat_sac_bunts,
    bat_runs,
    bat_rbi,
    bat_stolen_bases,
    bat_caught_stealing,
    bat_total_bases,
    -- Slash line + standard rate metrics (computed from summed counts).
    case
        when bat_at_bats > 0
        then round(bat_hits::numeric / bat_at_bats, 4)
    end::numeric(5, 4) as bat_avg,
    case
        when (bat_at_bats + bat_base_on_balls + bat_hit_by_pitch + bat_sac_flies) > 0
        then round(
            (bat_hits + bat_base_on_balls + bat_hit_by_pitch)::numeric
            / (bat_at_bats + bat_base_on_balls + bat_hit_by_pitch + bat_sac_flies),
            4
        )
    end::numeric(5, 4) as bat_obp,
    case
        when bat_at_bats > 0
        then round(bat_total_bases::numeric / bat_at_bats, 4)
    end::numeric(5, 4) as bat_slg,
    case
        when bat_at_bats > 0
         and (bat_at_bats + bat_base_on_balls + bat_hit_by_pitch + bat_sac_flies) > 0
        then round(
            (bat_hits + bat_base_on_balls + bat_hit_by_pitch)::numeric
              / (bat_at_bats + bat_base_on_balls + bat_hit_by_pitch + bat_sac_flies)
            + bat_total_bases::numeric / bat_at_bats,
            4
        )
    end::numeric(5, 4) as bat_ops,
    case
        when (bat_at_bats - bat_strike_outs - bat_sac_flies - bat_home_runs) > 0
        then round(
            (bat_hits - bat_home_runs)::numeric
            / (bat_at_bats - bat_strike_outs - bat_sac_flies - bat_home_runs),
            4
        )
    end::numeric(5, 4) as bat_babip,
    case
        when bat_plate_appearances > 0
        then round(100.0 * bat_strike_outs / bat_plate_appearances, 2)
    end::numeric(5, 2) as bat_k_pct,
    case
        when bat_plate_appearances > 0
        then round(100.0 * bat_base_on_balls / bat_plate_appearances, 2)
    end::numeric(5, 2) as bat_bb_pct,
    case
        when (bat_fly_outs + bat_sac_flies + bat_home_runs) > 0
        then round(
            bat_home_runs::numeric / (bat_fly_outs + bat_sac_flies + bat_home_runs),
            4
        )
    end::numeric(8, 4) as bat_home_run_rate,
    -- Sabermetric derived metrics (same formulas as int_player_stats_enriched).
    case
        when (bat_at_bats + bat_base_on_balls - bat_intentional_walks + bat_sac_flies + bat_hit_by_pitch) > 0
        then round(
            (c_w_bb * bat_base_on_balls
             + c_w_hbp * bat_hit_by_pitch
             + c_w_1b * (bat_hits - bat_home_runs - bat_doubles - bat_triples)
             + c_w_2b * bat_doubles
             + c_w_3b * bat_triples
             + c_w_hr * bat_home_runs
            )::numeric
            / (bat_at_bats + bat_base_on_balls - bat_intentional_walks + bat_sac_flies + bat_hit_by_pitch),
            4
        )
    end::numeric(5, 4) as bat_woba,
    -- wRC+ = ((wOBA - lgwOBA) / wOBAScale + lgR/PA) / lgR/PA * 100
    -- League grain → wOBA == lgwOBA (modulo data-coverage drift), so this
    -- collapses to ~100 by construction.
    case
        when bat_plate_appearances > 0
         and (bat_at_bats + bat_base_on_balls - bat_intentional_walks + bat_sac_flies + bat_hit_by_pitch) > 0
         and c_r_per_pa > 0
        then round(
            (
              ((c_w_bb * bat_base_on_balls
                + c_w_hbp * bat_hit_by_pitch
                + c_w_1b * (bat_hits - bat_home_runs - bat_doubles - bat_triples)
                + c_w_2b * bat_doubles
                + c_w_3b * bat_triples
                + c_w_hr * bat_home_runs
               )::numeric
               / (bat_at_bats + bat_base_on_balls - bat_intentional_walks + bat_sac_flies + bat_hit_by_pitch)
               - c_woba
              ) / c_woba_scale
              + c_r_per_pa
            ) / c_r_per_pa * 100,
            2
        )
    end::numeric(6, 2) as bat_wrc_plus
from with_constants
order by season
