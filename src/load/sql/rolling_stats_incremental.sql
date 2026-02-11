-- Incremental update of player_rolling_stats for one as_of_date.
-- Parameter: %s = as_of_date (DATE). Only players who had a game on as_of_date are updated.
-- Windows: 7 and 30 days. Run with: cursor.execute(sql, (as_of_date,))

WITH players_to_update AS (
    SELECT DISTINCT f.player_id
    FROM fact_game_state f
    JOIN dim_game g ON f.game_pk = g.game_pk
    WHERE g.game_date = %s
),
agg_7 AS (
    SELECT
        f.player_id,
        7 AS window_days,
        MAX(g.season) AS season,
        SUM(COALESCE(f.bat_games_played, 0)) AS bat_games_played,
        SUM(COALESCE(f.bat_plate_appearances, 0)) AS bat_plate_appearances,
        SUM(COALESCE(f.bat_at_bats, 0)) AS bat_at_bats,
        SUM(COALESCE(f.bat_runs, 0)) AS bat_runs,
        SUM(COALESCE(f.bat_hits, 0)) AS bat_hits,
        SUM(COALESCE(f.bat_doubles, 0)) AS bat_doubles,
        SUM(COALESCE(f.bat_triples, 0)) AS bat_triples,
        SUM(COALESCE(f.bat_home_runs, 0)) AS bat_home_runs,
        SUM(COALESCE(f.bat_rbi, 0)) AS bat_rbi,
        SUM(COALESCE(f.bat_strike_outs, 0)) AS bat_strike_outs,
        SUM(COALESCE(f.bat_base_on_balls, 0)) AS bat_base_on_balls,
        SUM(COALESCE(f.bat_stolen_bases, 0)) AS bat_stolen_bases,
        SUM(COALESCE(f.bat_caught_stealing, 0)) AS bat_caught_stealing,
        SUM(COALESCE(f.bat_intentional_walks, 0)) AS bat_ibb,
        SUM(COALESCE(f.bat_hit_by_pitch, 0)) AS bat_hbp,
        SUM(COALESCE(f.bat_sac_flies, 0)) AS bat_sf,
        SUM(COALESCE(f.bat_total_bases, 0)) AS bat_total_bases,
        SUM(COALESCE(f.pit_games_played, 0)) AS pit_games_played,
        SUM(COALESCE(f.pit_innings_pitched, 0)) AS pit_innings_pitched,
        SUM(COALESCE(f.pit_wins, 0)) AS pit_wins,
        SUM(COALESCE(f.pit_losses, 0)) AS pit_losses,
        SUM(COALESCE(f.pit_saves, 0)) AS pit_saves,
        SUM(COALESCE(f.pit_hits, 0)) AS pit_hits,
        SUM(COALESCE(f.pit_earned_runs, 0)) AS pit_earned_runs,
        SUM(COALESCE(f.pit_strike_outs, 0)) AS pit_strike_outs,
        SUM(COALESCE(f.pit_base_on_balls, 0)) AS pit_bb,
        SUM(COALESCE(f.pit_fip, 0) * COALESCE(f.pit_innings_pitched, 0)) AS pit_fip_times_ip,
        SUM(COALESCE(f.fld_assists, 0)) AS fld_assists,
        SUM(COALESCE(f.fld_put_outs, 0)) AS fld_put_outs,
        SUM(COALESCE(f.fld_errors, 0)) AS fld_errors,
        SUM(COALESCE(f.fld_chances, 0)) AS fld_chances
    FROM fact_game_state f
    JOIN dim_game g ON f.game_pk = g.game_pk
    WHERE g.game_date > %s::date - 7
      AND g.game_date <= %s
      AND f.player_id IN (SELECT player_id FROM players_to_update)
    GROUP BY f.player_id
    HAVING (SUM(COALESCE(f.bat_games_played, 0)) + SUM(COALESCE(f.pit_games_played, 0)) + SUM(COALESCE(f.fld_chances, 0))) > 0
),
agg_30 AS (
    SELECT
        f.player_id,
        30 AS window_days,
        MAX(g.season) AS season,
        SUM(COALESCE(f.bat_games_played, 0)) AS bat_games_played,
        SUM(COALESCE(f.bat_plate_appearances, 0)) AS bat_plate_appearances,
        SUM(COALESCE(f.bat_at_bats, 0)) AS bat_at_bats,
        SUM(COALESCE(f.bat_runs, 0)) AS bat_runs,
        SUM(COALESCE(f.bat_hits, 0)) AS bat_hits,
        SUM(COALESCE(f.bat_doubles, 0)) AS bat_doubles,
        SUM(COALESCE(f.bat_triples, 0)) AS bat_triples,
        SUM(COALESCE(f.bat_home_runs, 0)) AS bat_home_runs,
        SUM(COALESCE(f.bat_rbi, 0)) AS bat_rbi,
        SUM(COALESCE(f.bat_strike_outs, 0)) AS bat_strike_outs,
        SUM(COALESCE(f.bat_base_on_balls, 0)) AS bat_base_on_balls,
        SUM(COALESCE(f.bat_stolen_bases, 0)) AS bat_stolen_bases,
        SUM(COALESCE(f.bat_caught_stealing, 0)) AS bat_caught_stealing,
        SUM(COALESCE(f.bat_intentional_walks, 0)) AS bat_ibb,
        SUM(COALESCE(f.bat_hit_by_pitch, 0)) AS bat_hbp,
        SUM(COALESCE(f.bat_sac_flies, 0)) AS bat_sf,
        SUM(COALESCE(f.bat_total_bases, 0)) AS bat_total_bases,
        SUM(COALESCE(f.pit_games_played, 0)) AS pit_games_played,
        SUM(COALESCE(f.pit_innings_pitched, 0)) AS pit_innings_pitched,
        SUM(COALESCE(f.pit_wins, 0)) AS pit_wins,
        SUM(COALESCE(f.pit_losses, 0)) AS pit_losses,
        SUM(COALESCE(f.pit_saves, 0)) AS pit_saves,
        SUM(COALESCE(f.pit_hits, 0)) AS pit_hits,
        SUM(COALESCE(f.pit_earned_runs, 0)) AS pit_earned_runs,
        SUM(COALESCE(f.pit_strike_outs, 0)) AS pit_strike_outs,
        SUM(COALESCE(f.pit_base_on_balls, 0)) AS pit_bb,
        SUM(COALESCE(f.pit_fip, 0) * COALESCE(f.pit_innings_pitched, 0)) AS pit_fip_times_ip,
        SUM(COALESCE(f.fld_assists, 0)) AS fld_assists,
        SUM(COALESCE(f.fld_put_outs, 0)) AS fld_put_outs,
        SUM(COALESCE(f.fld_errors, 0)) AS fld_errors,
        SUM(COALESCE(f.fld_chances, 0)) AS fld_chances
    FROM fact_game_state f
    JOIN dim_game g ON f.game_pk = g.game_pk
    WHERE g.game_date > %s::date - 30
      AND g.game_date <= %s
      AND f.player_id IN (SELECT player_id FROM players_to_update)
    GROUP BY f.player_id
    HAVING (SUM(COALESCE(f.bat_games_played, 0)) + SUM(COALESCE(f.pit_games_played, 0)) + SUM(COALESCE(f.fld_chances, 0))) > 0
),
combined AS (
    SELECT * FROM agg_7
    UNION ALL
    SELECT * FROM agg_30
),
with_constants AS (
    SELECT
        a.player_id,
        a.window_days,
        a.season,
        a.bat_games_played,
        a.bat_plate_appearances,
        a.bat_at_bats,
        a.bat_runs,
        a.bat_hits,
        a.bat_doubles,
        a.bat_triples,
        a.bat_home_runs,
        a.bat_rbi,
        a.bat_strike_outs,
        a.bat_base_on_balls,
        a.bat_stolen_bases,
        a.bat_caught_stealing,
        a.bat_ibb,
        a.bat_hbp,
        a.bat_sf,
        a.bat_total_bases,
        a.pit_games_played,
        a.pit_innings_pitched,
        a.pit_wins,
        a.pit_losses,
        a.pit_saves,
        a.pit_hits,
        a.pit_earned_runs,
        a.pit_strike_outs,
        a.pit_bb,
        a.pit_fip_times_ip,
        a.fld_assists,
        a.fld_put_outs,
        a.fld_errors,
        a.fld_chances,
        c.woba AS c_woba,
        c.woba_scale AS c_woba_scale,
        c.w_bb AS c_w_bb,
        c.w_hbp AS c_w_hbp,
        c.w_1b AS c_w_1b,
        c.w_2b AS c_w_2b,
        c.w_3b AS c_w_3b,
        c.w_hr AS c_w_hr,
        c.r_per_pa AS c_r_per_pa
    FROM combined a
    JOIN LATERAL (
        SELECT * FROM dim_stat_constants c0
        ORDER BY ABS(c0.season - a.season)
        LIMIT 1
    ) c ON true
)
INSERT INTO player_rolling_stats (
    player_id,
    as_of_date,
    window_days,
    bat_games_played,
    bat_plate_appearances,
    bat_at_bats,
    bat_runs,
    bat_hits,
    bat_doubles,
    bat_triples,
    bat_home_runs,
    bat_rbi,
    bat_strike_outs,
    bat_base_on_balls,
    bat_stolen_bases,
    bat_caught_stealing,
    bat_avg,
    bat_ops,
    bat_woba,
    bat_wrc_plus,
    pit_games_played,
    pit_innings_pitched,
    pit_wins,
    pit_losses,
    pit_saves,
    pit_hits,
    pit_earned_runs,
    pit_strike_outs,
    pit_base_on_balls,
    pit_era,
    pit_fip,
    pit_whip,
    fld_assists,
    fld_put_outs,
    fld_errors,
    fld_chances
)
SELECT
    w.player_id,
    %s::date AS as_of_date,
    w.window_days,
    NULLIF(w.bat_games_played, 0),
    NULLIF(w.bat_plate_appearances, 0),
    NULLIF(w.bat_at_bats, 0),
    NULLIF(w.bat_runs, 0),
    NULLIF(w.bat_hits, 0),
    NULLIF(w.bat_doubles, 0),
    NULLIF(w.bat_triples, 0),
    NULLIF(w.bat_home_runs, 0),
    NULLIF(w.bat_rbi, 0),
    NULLIF(w.bat_strike_outs, 0),
    NULLIF(w.bat_base_on_balls, 0),
    NULLIF(w.bat_stolen_bases, 0),
    NULLIF(w.bat_caught_stealing, 0),
    CASE WHEN w.bat_at_bats > 0 THEN ROUND(w.bat_hits::numeric / w.bat_at_bats, 4) END,
    CASE
        WHEN w.bat_at_bats > 0 AND (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp) > 0
        THEN ROUND(
            (w.bat_base_on_balls + w.bat_hbp + w.bat_hits)::numeric / (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp)
            + w.bat_total_bases::numeric / w.bat_at_bats,
            4
        )
        ELSE NULL
    END,
    CASE
        WHEN (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp) > 0
        THEN ROUND(
            (w.c_w_bb * w.bat_base_on_balls + w.c_w_hbp * w.bat_hbp
             + w.c_w_1b * (w.bat_hits - w.bat_home_runs - w.bat_doubles - w.bat_triples)
             + w.c_w_2b * w.bat_doubles + w.c_w_3b * w.bat_triples + w.c_w_hr * w.bat_home_runs)
            ::numeric / (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp),
            4
        )
        ELSE NULL
    END,
    CASE
        WHEN w.bat_plate_appearances > 0 AND (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp) > 0
        THEN ROUND(
            ((w.c_w_bb * w.bat_base_on_balls + w.c_w_hbp * w.bat_hbp
              + w.c_w_1b * (w.bat_hits - w.bat_home_runs - w.bat_doubles - w.bat_triples)
              + w.c_w_2b * w.bat_doubles + w.c_w_3b * w.bat_triples + w.c_w_hr * w.bat_home_runs)
             ::numeric / (w.bat_at_bats + w.bat_base_on_balls - w.bat_ibb + w.bat_sf + w.bat_hbp) - w.c_woba)
            / w.c_woba_scale + w.c_r_per_pa * w.bat_plate_appearances,
            2
        )
        ELSE NULL
    END,
    NULLIF(w.pit_games_played, 0),
    CASE WHEN w.pit_innings_pitched > 0 THEN ROUND(w.pit_innings_pitched::numeric, 2) ELSE NULL END,
    NULLIF(w.pit_wins, 0),
    NULLIF(w.pit_losses, 0),
    NULLIF(w.pit_saves, 0),
    NULLIF(w.pit_hits, 0),
    NULLIF(w.pit_earned_runs, 0),
    NULLIF(w.pit_strike_outs, 0),
    NULLIF(w.pit_bb, 0),
    CASE WHEN w.pit_innings_pitched > 0 THEN ROUND(9.0 * w.pit_earned_runs / w.pit_innings_pitched, 2) ELSE NULL END,
    CASE WHEN w.pit_innings_pitched > 0 THEN ROUND(w.pit_fip_times_ip / w.pit_innings_pitched, 2) ELSE NULL END,
    CASE WHEN w.pit_innings_pitched > 0 THEN ROUND((w.pit_hits + w.pit_bb)::numeric / w.pit_innings_pitched, 2) ELSE NULL END,
    NULLIF(w.fld_assists, 0),
    NULLIF(w.fld_put_outs, 0),
    NULLIF(w.fld_errors, 0),
    NULLIF(w.fld_chances, 0)
FROM with_constants w
ON CONFLICT (player_id, as_of_date, window_days)
DO UPDATE SET
    bat_games_played = EXCLUDED.bat_games_played,
    bat_plate_appearances = EXCLUDED.bat_plate_appearances,
    bat_at_bats = EXCLUDED.bat_at_bats,
    bat_runs = EXCLUDED.bat_runs,
    bat_hits = EXCLUDED.bat_hits,
    bat_doubles = EXCLUDED.bat_doubles,
    bat_triples = EXCLUDED.bat_triples,
    bat_home_runs = EXCLUDED.bat_home_runs,
    bat_rbi = EXCLUDED.bat_rbi,
    bat_strike_outs = EXCLUDED.bat_strike_outs,
    bat_base_on_balls = EXCLUDED.bat_base_on_balls,
    bat_stolen_bases = EXCLUDED.bat_stolen_bases,
    bat_caught_stealing = EXCLUDED.bat_caught_stealing,
    bat_avg = EXCLUDED.bat_avg,
    bat_ops = EXCLUDED.bat_ops,
    bat_woba = EXCLUDED.bat_woba,
    bat_wrc_plus = EXCLUDED.bat_wrc_plus,
    pit_games_played = EXCLUDED.pit_games_played,
    pit_innings_pitched = EXCLUDED.pit_innings_pitched,
    pit_wins = EXCLUDED.pit_wins,
    pit_losses = EXCLUDED.pit_losses,
    pit_saves = EXCLUDED.pit_saves,
    pit_hits = EXCLUDED.pit_hits,
    pit_earned_runs = EXCLUDED.pit_earned_runs,
    pit_strike_outs = EXCLUDED.pit_strike_outs,
    pit_base_on_balls = EXCLUDED.pit_base_on_balls,
    pit_era = EXCLUDED.pit_era,
    pit_fip = EXCLUDED.pit_fip,
    pit_whip = EXCLUDED.pit_whip,
    fld_assists = EXCLUDED.fld_assists,
    fld_put_outs = EXCLUDED.fld_put_outs,
    fld_errors = EXCLUDED.fld_errors,
    fld_chances = EXCLUDED.fld_chances;
