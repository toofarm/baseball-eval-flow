"""
Load transformed MLB pipeline data into PostgreSQL star schema.

Dispatches:
- dim_team: upsert from game context (home/away team_id + name)
- dim_player: ensure distinct player_id exist (placeholder name if new)
- dim_game: upsert from TransformedGameData
- fact_game_state: upsert from LoadReadyPlayerGame rows

Caller must get connection (e.g. via PostgresHook), commit, and close.
Accepts dicts or Pydantic models (XCom may serialize to dict).
"""

from typing import Any, Sequence

from mlb_types import LoadReadyPlayerGame, TransformedGameData


def _get(obj: Any, key: str) -> Any:
    """Get attribute or dict key for XCom-serialized or in-memory objects."""
    if hasattr(obj, key):
        return getattr(obj, key)
    if isinstance(obj, dict):
        return obj.get(key)
    return None


def _parse_game_date(game_date_str: str) -> str:
    """Return YYYY-MM-DD for PostgreSQL DATE from pipeline game_date string."""
    s = (game_date_str or "").strip()
    if len(s) < 10:
        raise ValueError(f"Invalid game_date: {game_date_str!r}")
    return s[:10]


def ensure_teams(
    conn: Any, transformed_games: Sequence[TransformedGameData | dict[str, Any]]
) -> int:
    """Upsert dim_team for all home/away teams in transformed games. Returns count of rows affected."""
    seen: set[tuple[int, str]] = set()
    rows: list[tuple[int, str]] = []
    for g in transformed_games:
        for team_id, name in [
            (_get(g, "home_team_id"), _get(g, "home_team")),
            (_get(g, "away_team_id"), _get(g, "away_team")),
        ]:
            key = (team_id, name)
            if key in seen:
                continue
            seen.add(key)
            rows.append((team_id, name))

    if not rows:
        return 0

    sql = """
        INSERT INTO dim_team (team_id, name)
        VALUES (%s, %s)
        ON CONFLICT (team_id) DO UPDATE SET name = EXCLUDED.name
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def ensure_players(conn: Any, load_ready_rows: list[LoadReadyPlayerGame]) -> int:
    """Insert distinct player_id into dim_player with placeholder name. ON CONFLICT DO NOTHING. Returns count inserted."""
    player_ids: set[int] = set()
    for r in load_ready_rows:
        pid = r.get("player_id")
        if pid is not None:
            player_ids.add(pid)
    if not player_ids:
        return 0

    sql = """
        INSERT INTO dim_player (player_id, full_name)
        VALUES (%s, %s)
        ON CONFLICT (player_id) DO NOTHING
    """
    rows = [(pid, "Unknown") for pid in sorted(player_ids)]
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def load_dim_games(
    conn: Any, transformed_games: Sequence[TransformedGameData | dict[str, Any]]
) -> int:
    """Upsert dim_game from transformed games. Returns count of rows upserted."""
    if not transformed_games:
        return 0

    rows = []
    for g in transformed_games:
        game_date = _parse_game_date(str(_get(g, "game_date") or ""))
        rows.append(
            (
                _get(g, "game_pk"),
                game_date,
                _get(g, "season"),
                _get(g, "game_type"),
                _get(g, "venue_id"),
                _get(g, "home_team_id"),
                _get(g, "away_team_id"),
                _get(g, "winning_team") or None,
            )
        )

    sql = """
        INSERT INTO dim_game (
            game_pk, game_date, season, game_type, venue_id,
            home_team_id, away_team_id, winning_team
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_pk) DO UPDATE SET
            game_date = EXCLUDED.game_date,
            season = EXCLUDED.season,
            game_type = EXCLUDED.game_type,
            venue_id = EXCLUDED.venue_id,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            winning_team = EXCLUDED.winning_team
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


FACT_COLUMNS = [
    "game_pk",
    "player_id",
    "team_id",
    "position_code",
    "position_name",
    "bat_games_played",
    "bat_runs",
    "bat_hits",
    "bat_doubles",
    "bat_triples",
    "bat_home_runs",
    "bat_strike_outs",
    "bat_base_on_balls",
    "bat_at_bats",
    "bat_plate_appearances",
    "bat_rbi",
    "bat_stolen_bases",
    "bat_caught_stealing",
    "bat_woba",
    "bat_wrc_plus",
    "bat_ops",
    "bat_babip",
    "bat_home_run_rate",
    "bat_fly_outs",
    "bat_ground_outs",
    "bat_air_outs",
    "bat_intentional_walks",
    "bat_hit_by_pitch",
    "bat_ground_into_double_play",
    "bat_total_bases",
    "bat_left_on_base",
    "bat_sac_bunts",
    "bat_sac_flies",
    "pit_games_played",
    "pit_games_started",
    "pit_innings_pitched",
    "pit_wins",
    "pit_losses",
    "pit_saves",
    "pit_hits",
    "pit_earned_runs",
    "pit_strike_outs",
    "pit_base_on_balls",
    "pit_fip",
    "pit_babip",
    "pit_home_run_rate",
    "pit_batters_faced",
    "pit_outs",
    "pit_holds",
    "pit_blown_saves",
    "pit_save_opportunities",
    "pit_pitches_thrown",
    "pit_balls",
    "pit_strikes",
    "pit_hit_batsmen",
    "pit_balks",
    "pit_wild_pitches",
    "pit_pickoffs",
    "pit_inherited_runners",
    "pit_inherited_runners_scored",
    "fld_assists",
    "fld_put_outs",
    "fld_errors",
    "fld_chances",
    "fld_fielding_runs",
    "fld_passed_ball",
    "fld_pickoffs",
]


def load_fact_game_state(conn: Any, load_ready_rows: list[LoadReadyPlayerGame]) -> int:
    """Upsert fact_game_state from load-ready rows. Returns count upserted."""
    if not load_ready_rows:
        return 0

    col_list = ", ".join(FACT_COLUMNS)
    placeholders = ", ".join("%s" for _ in FACT_COLUMNS)
    update_cols = [c for c in FACT_COLUMNS if c not in ("game_pk", "player_id")]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO fact_game_state ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (game_pk, player_id) DO UPDATE SET {set_clause}
    """

    rows = []
    for r in load_ready_rows:
        row = [r.get(c) for c in FACT_COLUMNS]
        rows.append(row)

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def load_to_postgres(
    conn: Any,
    transformed_games: Sequence[TransformedGameData | dict[str, Any]],
    load_ready_rows: list[LoadReadyPlayerGame],
) -> dict[str, int]:
    """
    Run full load: teams -> players -> dim_game -> fact_game_state.
    Caller must get connection (e.g. via PostgresHook), commit, and close.
    Returns dict with keys: teams, players, games, fact_rows.
    """
    n_teams = ensure_teams(conn, transformed_games)
    n_players = ensure_players(conn, load_ready_rows)
    n_games = load_dim_games(conn, transformed_games)
    n_fact = load_fact_game_state(conn, load_ready_rows)
    return {
        "teams": n_teams,
        "players": n_players,
        "games": n_games,
        "fact_rows": n_fact,
    }
