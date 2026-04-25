"""
Load raw MLB data into staging tables for dbt transform.

- load_staging_schedule: insert/upsert schedule rows from extract
- load_staging_player_stats: insert/upsert player stats from boxscore

Caller must get connection (e.g. via PostgresHook), commit, and close.
"""

import json
from datetime import datetime
from typing import Any, Sequence

from src.mlb_types import PlayerStatsWithContext, ScheduleGame


def _parse_game_date(game_date_str: str) -> str:
    """Return YYYY-MM-DD for consistency. Handles MM/DD/YYYY or YYYY-MM-DD."""
    s = (game_date_str or "").strip()
    if len(s) >= 10 and s[2] == "/" and s[5] == "/":
        # MM/DD/YYYY
        parts = s.split("/")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    return s[:10] if len(s) >= 10 else s


def load_staging_schedule(
    conn: Any, schedule_games: Sequence[ScheduleGame | dict[str, Any]]
) -> int:
    """Insert or upsert staging_schedule from raw schedule games. Returns count of rows affected."""
    if not schedule_games:
        return 0

    rows = []
    for g in schedule_games:
        game_id = g.get("game_id")
        if game_id is None:
            continue
        game_pk = int(game_id) if not isinstance(game_id, int) else game_id
        game_date = _parse_game_date(str(g.get("game_date", "")))
        winning_team_id = (
            g.get("home_id")
            if g.get("winning_team") == g.get("home_name")
            else g.get("away_id")
        )
        rows.append(
            (
                game_pk,
                game_date,
                str(g.get("game_type", "")),
                int(g.get("venue_id", 0)),
                int(g.get("home_id", 0)),
                int(g.get("away_id", 0)),
                str(g.get("home_name", "")),
                str(g.get("away_name", "")),
                int(winning_team_id or 0),
            )
        )

    sql = """
        INSERT INTO staging_schedule (
            game_pk, game_date, game_type, venue_id,
            home_team_id, away_team_id, home_name, away_name, winning_team_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_pk) DO UPDATE SET
            game_date = EXCLUDED.game_date,
            game_type = EXCLUDED.game_type,
            venue_id = EXCLUDED.venue_id,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            home_name = EXCLUDED.home_name,
            away_name = EXCLUDED.away_name,
            winning_team_id = EXCLUDED.winning_team_id,
            load_date = CURRENT_TIMESTAMP
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def _to_jsonb(obj: Any) -> str | None:
    """Serialize to JSON string for JSONB column. Returns None if obj is None/empty."""
    if obj is None:
        return None
    if isinstance(obj, dict) and not obj:
        return None
    return json.dumps(obj)


def load_staging_player_stats(
    conn: Any,
    stats_with_context: Sequence[PlayerStatsWithContext | dict[str, Any]],
    batch_size: int = 1000,
) -> int:
    """Insert or upsert staging_player_stats from player stats with context. Returns count of rows affected."""
    if not stats_with_context:
        return 0

    rows = []
    for item in stats_with_context:
        game_pk = item.get("game_pk")
        player_id = item.get("player_id")
        if game_pk is None or player_id is None:
            continue
        full_name = item.get("full_name")
        position_type = item.get("position_type")
        stats = item.get("stats") or {}
        batting = stats.get("batting")
        pitching = stats.get("pitching")
        fielding = stats.get("fielding")
        rows.append(
            (
                int(game_pk),
                int(player_id),
                int(item.get("team_id", 0)),
                str(full_name),
                str(position_type or ""),
                str(item.get("position_code", "")),
                str(item.get("position_name", "")),
                _to_jsonb(batting),
                _to_jsonb(pitching),
                _to_jsonb(fielding),
            )
        )

    sql = """
        INSERT INTO staging_player_stats (
            game_pk, player_id, team_id, full_name, position_type, position_code, position_name,
            batting, pitching, fielding
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
        ON CONFLICT (game_pk, player_id) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            full_name = EXCLUDED.full_name,
            position_type = EXCLUDED.position_type,
             position_code = EXCLUDED.position_code,
            position_name = EXCLUDED.position_name,
            batting = EXCLUDED.batting,
            pitching = EXCLUDED.pitching,
            fielding = EXCLUDED.fielding,
            load_date = CURRENT_TIMESTAMP
    """
    total_loaded = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cur.executemany(sql, batch)
            total_loaded += len(batch)
    return total_loaded
