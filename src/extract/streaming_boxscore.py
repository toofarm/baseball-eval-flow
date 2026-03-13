"""Streaming boxscore fetch + load: fetch N games, load to staging, repeat."""

from typing import Any, List

from dags.mlb_types import PlayerStatsWithContext, ScheduleGame

from src.extract.boxscore import fetch_boxscore, parse_boxscore_players, _get_game_pk
from src.load.staging import load_staging_player_stats

# Default batch size (games per micro-batch)
DEFAULT_GAME_BATCH_SIZE = 5


def fetch_and_load_player_stats_batched(
    conn: Any,
    schedule_games: List[ScheduleGame],
    batch_size: int = DEFAULT_GAME_BATCH_SIZE,
    timeout: int = 30,
) -> int:
    """
    Fetch boxscores in batches and load each batch to staging immediately.
    Caller must commit and close conn. Returns total player stats rows loaded.
    """
    total_loaded = 0
    for i in range(0, len(schedule_games), batch_size):
        batch = schedule_games[i : i + batch_size]
        stats_batch: List[PlayerStatsWithContext] = []
        for game in batch:
            game_pk = _get_game_pk(game)
            data = fetch_boxscore(game_pk, timeout=timeout)
            stats_batch.extend(parse_boxscore_players(data, game_pk))
        if stats_batch:
            total_loaded += load_staging_player_stats(conn, stats_batch)
        conn.commit()  # Commit after each micro-batch
    return total_loaded
