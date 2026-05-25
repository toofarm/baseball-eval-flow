"""Streaming play-by-play fetch + load: fetch N games, load to staging, repeat.

Mirrors streaming_boxscore.py. Each game's full allPlays array is written as
one JSONB row in staging_play_by_play; dbt flattens it to fact_pitch.
"""

from typing import Any, List

from src.mlb_types import ScheduleGame

from src.extract.boxscore import _get_game_pk
from src.extract.play_by_play import fetch_play_by_play
from src.load.staging import load_staging_play_by_play

# Default batch size (games per micro-batch)
DEFAULT_GAME_BATCH_SIZE = 5


def fetch_and_load_play_by_play_batched(
    conn: Any,
    schedule_games: List[ScheduleGame],
    batch_size: int = DEFAULT_GAME_BATCH_SIZE,
    timeout: int = 30,
) -> int:
    """
    Fetch playByPlay in batches and load each batch to staging immediately.
    Caller must commit and close conn. Returns total games loaded.
    """
    total_loaded = 0
    for i in range(0, len(schedule_games), batch_size):
        batch = schedule_games[i : i + batch_size]
        pbp_batch: List[tuple[int, dict]] = []
        for game in batch:
            game_pk = _get_game_pk(game)
            data = fetch_play_by_play(game_pk, timeout=timeout)
            pbp_batch.append((game_pk, data))
        if pbp_batch:
            total_loaded += load_staging_play_by_play(conn, pbp_batch)
        conn.commit()  # Commit after each micro-batch
    return total_loaded
