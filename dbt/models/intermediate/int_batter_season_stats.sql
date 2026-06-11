{{
    config(
        materialized='view',
    )
}}

-- Per-batter season totals for the qualified batter pool. Grain:
-- (season, player_id). One row per batter who cleared the season PA cutoff.
--
-- This is the shared source of truth for the "qualified batter" population.
-- Both app_league_batting_summary (which sums these counts into a PA-weighted
-- league baseline) and app_player_batting_percentiles (which ranks each batter
-- against the pool) build on it, so the league average and the percentile
-- ranks always describe the same set of hitters.
--
-- Scope: regular-season games only (dim_game.game_type = 'R'). Post-season,
-- spring training, exhibition, and all-star games are excluded so the pool
-- matches the conventional "MLB regular-season" reference.
--
-- Low-sample filter: per season, batters whose total PA falls below
-- mean(PA) - 0.5 * stddev(PA) across all batters in that season are excluded.
-- This drops pinch-hit pitcher cameos and other 1-2 PA appearances that would
-- otherwise pull rate stats toward the noise floor. The 0.5 multiplier is
-- tuned for the right-skewed PA distribution typical of MLB seasons; larger
-- multipliers (e.g. 2.0) effectively disable the filter mid-season because the
-- threshold goes <= 0.
--
-- The per-batter slash rates (bat_avg/obp/slg/ops/k_pct) are computed here from
-- each batter's own season totals. app_league_batting_summary does NOT use
-- these rate columns — it re-derives league rates from summed counts so the
-- baseline stays PA-weighted rather than an average-of-player-averages.

with games as (
    select game_pk, season, game_type from {{ ref('dim_game') }}
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
-- Roll per-game rows up to season totals per player. Threshold evaluation
-- happens at this grain so each batter's full-season PA count is what's
-- compared to the league cutoff.
per_player_season as (
    select
        season,
        player_id,
        sum(pa)                   as pa,
        sum(ab)                   as ab,
        sum(hits)                 as hits,
        sum(doubles)              as doubles,
        sum(triples)              as triples,
        sum(home_runs)            as home_runs,
        sum(strike_outs)          as strike_outs,
        sum(base_on_balls)        as base_on_balls,
        sum(intentional_walks)    as intentional_walks,
        sum(hit_by_pitch)         as hit_by_pitch,
        sum(sac_flies)            as sac_flies,
        sum(sac_bunts)            as sac_bunts,
        sum(runs)                 as runs,
        sum(rbi)                  as rbi,
        sum(stolen_bases)         as stolen_bases,
        sum(caught_stealing)      as caught_stealing,
        sum(total_bases)          as total_bases,
        sum(fly_outs)             as fly_outs
    from per_player_game
    group by season, player_id
),
-- Per-season PA cutoff: mean - 0.5 * stddev. stddev defaults to sample (n-1)
-- in both Postgres and Snowflake, which is what we want for inference.
pa_thresholds as (
    select
        season,
        avg(pa)                            as mean_pa,
        stddev(pa)                         as stddev_pa,
        avg(pa) - 0.5 * stddev(pa)         as min_pa_for_inclusion
    from per_player_season
    group by season
),
qualified_player_season as (
    select pps.*
    from per_player_season pps
    join pa_thresholds t on t.season = pps.season
    where pps.pa >= t.min_pa_for_inclusion
)
select
    season,
    player_id,
    -- Counting (per-batter season totals; summed by app_league_batting_summary)
    pa                 as bat_plate_appearances,
    ab                 as bat_at_bats,
    hits               as bat_hits,
    doubles            as bat_doubles,
    triples            as bat_triples,
    home_runs          as bat_home_runs,
    strike_outs        as bat_strike_outs,
    base_on_balls      as bat_base_on_balls,
    intentional_walks  as bat_intentional_walks,
    hit_by_pitch       as bat_hit_by_pitch,
    sac_flies          as bat_sac_flies,
    sac_bunts          as bat_sac_bunts,
    runs               as bat_runs,
    rbi                as bat_rbi,
    stolen_bases       as bat_stolen_bases,
    caught_stealing    as bat_caught_stealing,
    total_bases        as bat_total_bases,
    fly_outs           as bat_fly_outs,
    -- Per-batter slash line + K rate (computed from this batter's own totals).
    case
        when ab > 0
        then round(hits::numeric / ab, 4)
    end::numeric(5, 4) as bat_avg,
    case
        when (ab + base_on_balls + hit_by_pitch + sac_flies) > 0
        then round(
            (hits + base_on_balls + hit_by_pitch)::numeric
            / (ab + base_on_balls + hit_by_pitch + sac_flies),
            4
        )
    end::numeric(5, 4) as bat_obp,
    case
        when ab > 0
        then round(total_bases::numeric / ab, 4)
    end::numeric(5, 4) as bat_slg,
    case
        when ab > 0
         and (ab + base_on_balls + hit_by_pitch + sac_flies) > 0
        then round(
            (hits + base_on_balls + hit_by_pitch)::numeric
              / (ab + base_on_balls + hit_by_pitch + sac_flies)
            + total_bases::numeric / ab,
            4
        )
    end::numeric(5, 4) as bat_ops,
    case
        when pa > 0
        then round(100.0 * strike_outs / pa, 2)
    end::numeric(5, 2) as bat_k_pct
from qualified_player_season
