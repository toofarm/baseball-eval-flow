{{
    config(
        materialized='view',
    )
}}

-- Per-batter percentile ranks vs. the league. Grain: (player_id, season).
-- For each qualified batter, shows the raw stat plus the percentile (0-100) at
-- which it ranks among all qualified batters in the same season, for:
--   batting average, OBP, SLG, OPS, and strikeout rate.
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
-- percentile is always better, including for strikeout rate: K% is ranked
-- descending (DESC), so the LOWEST strikeout rate earns the HIGHEST percentile.
-- A batter at the 90th percentile in any column is among the best in that
-- category.
--
-- cume_dist returns (0, 100]: the worst qualified batter is still above 0
-- (they're "at or below" themselves) and the best is exactly 100. With a single
-- qualified batter in a season it returns 100. Ties share the same percentile.

with batters as (
    select
        player_id,
        season,
        bat_plate_appearances,
        bat_avg,
        bat_obp,
        bat_slg,
        bat_ops,
        bat_k_pct
    from {{ ref('int_batter_season_stats') }}
    -- at_bats > 0 guarantees avg/slg/ops/obp are non-null; pa > 0 (always true
    -- for the qualified pool) guarantees k_pct is non-null.
    where bat_avg is not null
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
        cume_dist() over (partition by season order by bat_avg) as avg_pr,
        cume_dist() over (partition by season order by bat_obp) as obp_pr,
        cume_dist() over (partition by season order by bat_slg) as slg_pr,
        cume_dist() over (partition by season order by bat_ops) as ops_pr,
        -- DESC: lowest K% (best) ranks at the top, earning the highest percentile.
        cume_dist() over (partition by season order by bat_k_pct desc) as k_pct_pr
    from batters
)
select
    r.player_id,
    p.full_name as player_name,
    r.season,
    r.bat_plate_appearances,
    r.bat_avg,
    round(100.0 * r.avg_pr, 1)::numeric(5, 1)    as bat_avg_pctl,
    r.bat_obp,
    round(100.0 * r.obp_pr, 1)::numeric(5, 1)    as bat_obp_pctl,
    r.bat_slg,
    round(100.0 * r.slg_pr, 1)::numeric(5, 1)    as bat_slg_pctl,
    r.bat_ops,
    round(100.0 * r.ops_pr, 1)::numeric(5, 1)    as bat_ops_pctl,
    r.bat_k_pct,
    round(100.0 * r.k_pct_pr, 1)::numeric(5, 1)  as bat_k_pct_pctl
from ranked r
left join {{ ref('dim_player') }} p on p.player_id = r.player_id
order by r.season, r.player_id
