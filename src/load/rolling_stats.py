"""
Load rolling stats: fetch game rows for rolling window and upsert player_rolling_stats.
"""

from typing import Any

ROLLING_STATS_COLUMNS = [
    "player_id",
    "as_of_date",
    "window_days",
    "bat_games_played",
    "bat_plate_appearances",
    "bat_at_bats",
    "bat_runs",
    "bat_hits",
    "bat_doubles",
    "bat_triples",
    "bat_home_runs",
    "bat_rbi",
    "bat_strike_outs",
    "bat_base_on_balls",
    "bat_stolen_bases",
    "bat_caught_stealing",
    "bat_avg",
    "bat_ops",
    "bat_woba",
    "bat_wrc_plus",
    "pit_games_played",
    "pit_innings_pitched",
    "pit_wins",
    "pit_losses",
    "pit_saves",
    "pit_hits",
    "pit_earned_runs",
    "pit_strike_outs",
    "pit_base_on_balls",
    "pit_era",
    "pit_fip",
    "pit_whip",
    "fld_assists",
    "fld_put_outs",
    "fld_errors",
    "fld_chances",
]

FETCH_GAME_ROWS_QUERY = """
    SELECT
        f.player_id,
        g.game_date,
        g.season,
        f.bat_games_played, f.bat_plate_appearances, f.bat_at_bats,
        f.bat_runs, f.bat_hits, f.bat_doubles, f.bat_triples, f.bat_home_runs,
        f.bat_rbi, f.bat_strike_outs, f.bat_base_on_balls,
        f.bat_stolen_bases, f.bat_caught_stealing,
        f.bat_intentional_walks, f.bat_hit_by_pitch, f.bat_sac_flies,
        f.bat_total_bases,
        f.pit_games_played, f.pit_innings_pitched, f.pit_wins, f.pit_losses,
        f.pit_saves, f.pit_hits, f.pit_earned_runs, f.pit_strike_outs,
        f.pit_base_on_balls, f.pit_fip, f.pit_hit_batsmen,
        f.fld_assists, f.fld_put_outs, f.fld_errors, f.fld_chances
    FROM fact_game_state f
    JOIN dim_game g ON f.game_pk = g.game_pk
    WHERE g.game_date > CURRENT_DATE - %s
"""


def fetch_game_rows_for_rolling(
    conn: Any, lookback_days: int
) -> list[dict[str, Any]]:
    """Query fact_game_state + dim_game for the last lookback_days. Returns list of dicts."""
    with conn.cursor() as cur:
        cur.execute(FETCH_GAME_ROWS_QUERY, (lookback_days,))
        columns = [c.name for c in cur.description]
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]
    return rows


def upsert_rolling_stats(
    conn: Any, rolling_rows: list[dict[str, Any]]
) -> int:
    """Upsert rows into player_rolling_stats. Returns number of rows upserted."""
    if not rolling_rows:
        return 0

    cols = ROLLING_STATS_COLUMNS
    placeholders = ", ".join("%s" for _ in cols)
    col_list = ", ".join(cols)
    upsert_sql = f"""
        INSERT INTO player_rolling_stats ({col_list})
        VALUES ({placeholders})
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
            fld_chances = EXCLUDED.fld_chances
    """
    with conn.cursor() as cur:
        for r in rolling_rows:
            cur.execute(upsert_sql, [r.get(c) for c in cols])
    return len(rolling_rows)
