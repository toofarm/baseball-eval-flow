"""
Load raw MLB data into staging tables for dbt transform.

- load_staging_schedule: merge schedule rows from extract into staging_schedule
- load_staging_player_stats: merge player stats from boxscore into staging_player_stats

SQL is Snowflake-flavored: ``MERGE INTO ... USING (SELECT ...) ... WHEN MATCHED
THEN UPDATE WHEN NOT MATCHED THEN INSERT`` and ``PARSE_JSON(%s)`` for VARIANT
columns. Caller must supply a Snowflake DB-API connection (e.g. via
``SnowflakeHook.get_conn()``) and is responsible for commit/close.
"""

import json
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


_MERGE_SCHEDULE_SQL = """
    MERGE INTO staging_schedule AS tgt
    USING (
        SELECT
            %s AS game_pk,
            %s AS game_date,
            %s AS game_type,
            %s AS venue_id,
            %s AS home_team_id,
            %s AS away_team_id,
            %s AS home_name,
            %s AS away_name,
            %s AS winning_team_id
    ) AS src
    ON tgt.game_pk = src.game_pk
    WHEN MATCHED THEN UPDATE SET
        game_date = src.game_date,
        game_type = src.game_type,
        venue_id = src.venue_id,
        home_team_id = src.home_team_id,
        away_team_id = src.away_team_id,
        home_name = src.home_name,
        away_name = src.away_name,
        winning_team_id = src.winning_team_id,
        load_date = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        game_pk, game_date, game_type, venue_id,
        home_team_id, away_team_id, home_name, away_name, winning_team_id
    ) VALUES (
        src.game_pk, src.game_date, src.game_type, src.venue_id,
        src.home_team_id, src.away_team_id, src.home_name, src.away_name,
        src.winning_team_id
    )
"""


def load_staging_schedule(
    conn: Any, schedule_games: Sequence[ScheduleGame | dict[str, Any]]
) -> int:
    """Merge schedule rows into staging_schedule. Returns number of rows submitted."""
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

    with conn.cursor() as cur:
        cur.executemany(_MERGE_SCHEDULE_SQL, rows)
    return len(rows)


def _to_json(obj: Any) -> str | None:
    """Serialize to JSON string for VARIANT column. Returns None if obj is None/empty."""
    if obj is None:
        return None
    if isinstance(obj, dict) and not obj:
        return None
    return json.dumps(obj)


# Number of bind parameters per player_stats row. Used to build the multi-row
# VALUES clause below — must stay in sync with the tuple in load_staging_player_stats.
_PLAYER_STATS_COLS_PER_ROW = 10


def _build_player_stats_merge_sql(n_rows: int) -> str:
    """Build a single MERGE statement that handles ``n_rows`` source rows.

    Snowflake's ``executemany`` is only optimized for plain ``INSERT … VALUES``;
    for ``MERGE`` it falls back to N serial round-trips, which dominated wall
    time (~900 rows × ~1s round-trip ≈ 12 min). Folding all rows into one
    ``USING (SELECT … FROM VALUES (…), (…), …)`` collapses that to one round-trip
    per chunk.
    """
    if n_rows < 1:
        raise ValueError("n_rows must be >= 1")
    placeholder_row = "(" + ", ".join(["%s"] * _PLAYER_STATS_COLS_PER_ROW) + ")"
    values_block = ", ".join([placeholder_row] * n_rows)
    return f"""
        MERGE INTO staging_player_stats AS tgt
        USING (
            SELECT
                column1 AS game_pk,
                column2 AS player_id,
                column3 AS team_id,
                column4 AS full_name,
                column5 AS position_type,
                column6 AS position_code,
                column7 AS position_name,
                PARSE_JSON(column8) AS batting,
                PARSE_JSON(column9) AS pitching,
                PARSE_JSON(column10) AS fielding
            FROM VALUES {values_block}
        ) AS src
        ON tgt.game_pk = src.game_pk AND tgt.player_id = src.player_id
        WHEN MATCHED THEN UPDATE SET
            team_id = src.team_id,
            full_name = src.full_name,
            position_type = src.position_type,
            position_code = src.position_code,
            position_name = src.position_name,
            batting = src.batting,
            pitching = src.pitching,
            fielding = src.fielding,
            load_date = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            game_pk, player_id, team_id, full_name, position_type, position_code,
            position_name, batting, pitching, fielding
        ) VALUES (
            src.game_pk, src.player_id, src.team_id, src.full_name,
            src.position_type, src.position_code, src.position_name,
            src.batting, src.pitching, src.fielding
        )
    """


def load_staging_player_stats(
    conn: Any,
    stats_with_context: Sequence[PlayerStatsWithContext | dict[str, Any]],
    chunk_size: int = 500,
) -> int:
    """Merge player stats rows into staging_player_stats. Returns rows submitted.

    Each chunk becomes one MERGE statement carrying ``chunk_size`` rows in its
    USING clause. Default 500 keeps us safely under Snowflake's bind-parameter
    limit (~16k) at 10 cols/row, and a typical daily load (~900 rows) fits in
    two round-trips.
    """
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
                _to_json(batting),
                _to_json(pitching),
                _to_json(fielding),
            )
        )

    total_loaded = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            sql = _build_player_stats_merge_sql(len(chunk))
            flat_params = [v for row in chunk for v in row]
            cur.execute(sql, flat_params)
            total_loaded += len(chunk)
    return total_loaded


_MERGE_PLAY_BY_PLAY_SQL = """
    MERGE INTO staging_play_by_play AS tgt
    USING (
        SELECT
            %s AS game_pk,
            PARSE_JSON(%s) AS all_plays
    ) AS src
    ON tgt.game_pk = src.game_pk
    WHEN MATCHED THEN UPDATE SET
        all_plays = src.all_plays,
        load_date = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        game_pk, all_plays
    ) VALUES (
        src.game_pk, src.all_plays
    )
"""


def load_staging_play_by_play(
    conn: Any,
    pbp_by_game: Sequence[tuple[int, Any]],
) -> int:
    """Merge play-by-play JSON blobs into staging_play_by_play.

    ``pbp_by_game`` is a sequence of ``(game_pk, pbp_json)`` tuples, where
    ``pbp_json`` is the full playByPlay response dict (we persist only the
    ``allPlays`` array to keep storage tight). Returns rows submitted.
    """
    if not pbp_by_game:
        return 0

    rows = []
    for game_pk, pbp in pbp_by_game:
        if pbp is None:
            continue
        all_plays = pbp.get("allPlays") if isinstance(pbp, dict) else None
        if not all_plays:
            continue
        rows.append((int(game_pk), json.dumps(all_plays)))

    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany(_MERGE_PLAY_BY_PLAY_SQL, rows)
    return len(rows)
