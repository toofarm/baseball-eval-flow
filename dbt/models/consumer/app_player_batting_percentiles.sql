{{
    config(
        materialized='view',
    )
}}

-- Per-batter percentile ranks vs. the league. Grain: (player_id, season).
-- For each qualified batter, shows the raw stat plus the percentile (0-100) at
-- which it ranks among all qualified batters in the same season, for:
--   batting average, OBP, SLG, OPS, strikeout rate, walk rate, BABIP, and wRC+.
--
-- The web app reads this to render a "how does this hitter rank?" table.
--
-- Population: int_batter_season_stats (regular-season qualified batters — the
-- exact pool app_league_batting_summary aggregates), restricted to batters with
-- a complete slash line (at_bats > 0) so every ranked metric is defined. Each
-- batter is ranked only against others in the same season.
--
-- Percentile = round(100 * cume_dist() over season, 1) — the percentage of
-- qualified batters that season at or below this batter on the metric. Higher
-- percentile is always better. Strikeout rate is the one inversion: K% is ranked
-- DESC, so the LOWEST strikeout rate earns the HIGHEST percentile. BB%, BABIP,
-- and wRC+ are ranked ASC like the slash-line stats (higher is better).
--
-- BB%, BABIP, wOBA, and wRC+ are derived here from each batter's own season
-- counts, using the same formulas and closest-season constant lookup as
-- app_league_batting_summary (see that model's header). wRC+ pulls the league
-- wOBA / wOBA-scale / R-per-PA constants from dim_stat_constants; unlike the
-- league-grain summary (where wRC+ is ~100 by construction) these are genuine
-- per-batter index values.
--
-- cume_dist returns (0, 100]: the worst qualified batter is still above 0
-- (they're "at or below" themselves) and the best is exactly 100. With a single
-- qualified batter in a season it returns 100. Ties share the same percentile.

with batters as (
    select
        player_id,
        season,
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
        bat_avg,
        bat_obp,
        bat_slg,
        bat_ops,
        bat_k_pct
    from {{ ref('int_batter_season_stats') }}
    -- at_bats > 0 guarantees avg/slg/ops/obp are non-null; pa > 0 (always true
    -- for the qualified pool) guarantees k_pct and bb_pct are non-null.
    where bat_avg is not null
),
-- Closest-season constant lookup, same pattern as app_league_batting_summary.
constants as (
    select * from {{ ref('dim_stat_constants') }}
),
seasons_in_use as (
    select distinct season from batters
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
-- Derive BB%, BABIP, and wOBA from each batter's own counts.
with_rates as (
    select
        b.*,
        c.woba       as c_woba,
        c.woba_scale as c_woba_scale,
        c.r_per_pa   as c_r_per_pa,
        case
            when b.bat_plate_appearances > 0
            then round(100.0 * b.bat_base_on_balls / b.bat_plate_appearances, 2)
        end::numeric(5, 2) as bat_bb_pct,
        case
            when (b.bat_at_bats - b.bat_strike_outs - b.bat_sac_flies - b.bat_home_runs) > 0
            then round(
                (b.bat_hits - b.bat_home_runs)::numeric
                / (b.bat_at_bats - b.bat_strike_outs - b.bat_sac_flies - b.bat_home_runs),
                4
            )
        end::numeric(5, 4) as bat_babip,
        case
            when (b.bat_at_bats + b.bat_base_on_balls - b.bat_intentional_walks + b.bat_sac_flies + b.bat_hit_by_pitch) > 0
            then round(
                (c.w_bb * b.bat_base_on_balls
                 + c.w_hbp * b.bat_hit_by_pitch
                 + c.w_1b * (b.bat_hits - b.bat_home_runs - b.bat_doubles - b.bat_triples)
                 + c.w_2b * b.bat_doubles
                 + c.w_3b * b.bat_triples
                 + c.w_hr * b.bat_home_runs
                )::numeric
                / (b.bat_at_bats + b.bat_base_on_balls - b.bat_intentional_walks + b.bat_sac_flies + b.bat_hit_by_pitch),
                4
            )
        end::numeric(5, 4) as bat_woba
    from batters b
    left join closest_constant c on c.p_season = b.season
),
-- wRC+ from wOBA: ((wOBA - lgwOBA) / wOBAScale + lgR/PA) / lgR/PA * 100.
with_wrc as (
    select
        w.*,
        case
            when w.bat_woba is not null
             and w.c_woba_scale > 0
             and w.c_r_per_pa > 0
            then round(
                ((w.bat_woba - w.c_woba) / w.c_woba_scale + w.c_r_per_pa)
                / w.c_r_per_pa * 100,
                2
            )
        end::numeric(6, 2) as bat_wrc_plus
    from with_rates w
),
ranked as (
    select
        player_id,
        season,
        bat_plate_appearances,
        bat_avg,
        bat_obp,
        bat_slg,
        bat_ops,
        bat_k_pct,
        bat_bb_pct,
        bat_babip,
        bat_wrc_plus,
        cume_dist() over (partition by season order by bat_avg) as avg_pr,
        cume_dist() over (partition by season order by bat_obp) as obp_pr,
        cume_dist() over (partition by season order by bat_slg) as slg_pr,
        cume_dist() over (partition by season order by bat_ops) as ops_pr,
        -- DESC: lowest K% (best) ranks at the top, earning the highest percentile.
        cume_dist() over (partition by season order by bat_k_pct desc) as k_pct_pr,
        cume_dist() over (partition by season order by bat_bb_pct) as bb_pct_pr,
        cume_dist() over (partition by season order by bat_babip) as babip_pr,
        cume_dist() over (partition by season order by bat_wrc_plus) as wrc_plus_pr
    from with_wrc
)
select
    r.player_id,
    p.full_name as player_name,
    r.season,
    r.bat_plate_appearances,
    r.bat_avg,
    round(100.0 * r.avg_pr, 1)::numeric(5, 1)      as bat_avg_pctl,
    r.bat_obp,
    round(100.0 * r.obp_pr, 1)::numeric(5, 1)      as bat_obp_pctl,
    r.bat_slg,
    round(100.0 * r.slg_pr, 1)::numeric(5, 1)      as bat_slg_pctl,
    r.bat_ops,
    round(100.0 * r.ops_pr, 1)::numeric(5, 1)      as bat_ops_pctl,
    r.bat_k_pct,
    round(100.0 * r.k_pct_pr, 1)::numeric(5, 1)    as bat_k_pct_pctl,
    r.bat_bb_pct,
    round(100.0 * r.bb_pct_pr, 1)::numeric(5, 1)   as bat_bb_pct_pctl,
    r.bat_babip,
    round(100.0 * r.babip_pr, 1)::numeric(5, 1)    as bat_babip_pctl,
    r.bat_wrc_plus,
    round(100.0 * r.wrc_plus_pr, 1)::numeric(5, 1) as bat_wrc_plus_pctl
from ranked r
left join {{ ref('dim_player') }} p on p.player_id = r.player_id
order by r.season, r.player_id
