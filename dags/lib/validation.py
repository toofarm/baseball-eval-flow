"""
Validation helpers for the MLB ETL pipeline.

Run after extract/transform steps to fail the DAG early on invalid or
inconsistent data. Each validator raises ValueError with a clear message
if validation fails.
"""

from typing import Any, List, Optional, Union

from mlb_types import (
    TransformedGameData,
    TransformedPlayerData,
    PlayerStatsWithContext,
)


# --- Schedule (after extract) ---


def validate_schedule_games(
    games: Union[List[TransformedGameData], Any], min_games: int = 1
) -> None:
    """
    Validate raw schedule data from extract_yesterdays_games.

    - Ensures list is present and length meets minimum (default 0).
    - Checks each game has required keys and valid game_pk/season.
    """
    if not isinstance(games, list):
        raise ValueError(f"schedule data must be a list, got {type(games).__name__}")

    if len(games) < min_games:
        raise ValueError(
            f"expected at least {min_games} schedule game(s), got {len(games)}"
        )

    required = {"game_id", "home_name", "away_name", "game_date"}
    for i, g in enumerate(games):
        if not isinstance(g, dict):
            raise ValueError(f"game[{i}] must be a dict, got {type(g).__name__}")
        missing = required - set(g.keys())
        if missing:
            raise ValueError(f"game[{i}] missing required keys: {missing}")
        pk = g.get("game_id")
        if not isinstance(pk, int) or pk <= 0:
            raise ValueError(f"game[{i}] invalid game_id: {pk}")


# --- Transformed games (after transform_game_data) ---


def validate_transformed_games(
    games: Union[List[TransformedGameData], Any],
    min_games: int = 0,
    expected_game_pks: Optional[List[int]] = None,
) -> None:
    """
    Validate transformed game data.

    - Ensures list length and optional match to expected game_pks.
    - Validates game_date format (YYYY-MM-DD) and season range.
    """
    if not isinstance(games, list):
        raise ValueError(
            f"transformed games must be a list, got {type(games).__name__}"
        )

    if len(games) < min_games:
        raise ValueError(
            f"expected at least {min_games} transformed game(s), got {len(games)}"
        )

    if expected_game_pks is not None:
        actual_pks = sorted(g.game_pk for g in games)
        expected_sorted = sorted(expected_game_pks)
        if actual_pks != expected_sorted:
            raise ValueError(
                f"transformed game_pks do not match extract: "
                f"expected {expected_sorted}, got {actual_pks}"
            )

    for i, g in enumerate(games):
        if not isinstance(g, TransformedGameData):
            raise ValueError(
                f"transformed game[{i}] must be TransformedGameData, got {type(g).__name__}"
            )
        if g.game_pk <= 0:
            raise ValueError(f"transformed game[{i}] invalid game_pk: {g.game_pk}")
        if not (1870 <= g.season <= 2100):
            raise ValueError(f"transformed game[{i}] invalid season: {g.season}")
        # Basic date format: YYYY-MM-DD (10 chars)
        if len(g.game_date) < 10 or g.game_date[4] != "-" or g.game_date[7] != "-":
            raise ValueError(
                f"transformed game[{i}] invalid game_date format: {g.game_date}"
            )


# --- Player stats (after fetch_player_stats) ---


def validate_player_stats_with_context_list(
    items: Union[List[PlayerStatsWithContext], Any],
    min_count: int = 0,
) -> None:
    """
    Validate list of player stats with context from fetch_player_stats.

    - Ensures list and minimum count.
    - Ensures each item has game_pk, player_id, team_id, position_code, position_name, stats.
    - Ensures each stats has at least one of batting/pitching/fielding.
    """
    if not isinstance(items, list):
        raise ValueError(
            f"player stats with context must be a list, got {type(items).__name__}"
        )

    if len(items) < min_count:
        raise ValueError(
            f"expected at least {min_count} player stat(s) with context, got {len(items)}"
        )

    required_ctx = {
        "game_pk",
        "player_id",
        "team_id",
        "position_code",
        "position_name",
        "stats",
    }
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"item[{i}] must be a dict, got {type(item).__name__}")
        missing = required_ctx - set(item.keys())
        if missing:
            raise ValueError(f"item[{i}] missing required keys: {missing}")
        stat = item.get("stats")
        if not isinstance(stat, dict):
            raise ValueError(
                f"item[{i}].stats must be a dict, got {type(stat).__name__}"
            )
        has_any = (
            stat.get("batting") is not None
            or stat.get("pitching") is not None
            or stat.get("fielding") is not None
        )
        if not has_any:
            raise ValueError(
                f"item[{i}].stats must have at least one of batting/pitching/fielding"
            )


def validate_player_stats_list(
    player_stats: Union[List[TransformedGameData], Any],
    min_count: int = 0,
) -> None:
    """
    Validate list of raw player stats from fetch_player_stats.

    - Ensures list and minimum count.
    - Ensures each item has at least one of batting/pitching/fielding.
    """
    if not isinstance(player_stats, list):
        raise ValueError(
            f"player_stats must be a list, got {type(player_stats).__name__}"
        )

    if len(player_stats) < min_count:
        raise ValueError(
            f"expected at least {min_count} player stat(s), got {len(player_stats)}"
        )

    for i, stat in enumerate(player_stats):
        if not isinstance(stat, dict):
            raise ValueError(
                f"player_stats[{i}] must be a dict, got {type(stat).__name__}"
            )
        has_any = (
            stat.get("batting") is not None
            or stat.get("pitching") is not None
            or stat.get("fielding") is not None
        )
        if not has_any:
            raise ValueError(
                f"player_stats[{i}] must have at least one of batting/pitching/fielding"
            )


# --- Transformed player data (after transform) ---


def validate_transformed_player_data(
    records: Union[List[TransformedPlayerData], Any],
    min_count: int = 1,
) -> None:
    """
    Validate transformed player data.

    - Ensures list and minimum count.
    - Ensures each record has at least one of batting/pitching/fielding.
    """
    if not isinstance(records, list):
        raise ValueError(
            f"transformed player data must be a list, got {type(records).__name__}"
        )

    if len(records) < min_count:
        raise ValueError(
            f"expected at least {min_count} transformed player record(s), got {len(records)}"
        )

    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            raise ValueError(
                f"transformed player[{i}] must be a dict, got {type(rec).__name__}"
            )
        has_any = (
            rec.get("batting") is not None
            or rec.get("pitching") is not None
            or rec.get("fielding") is not None
        )
        if not has_any:
            raise ValueError(
                f"transformed player[{i}] must have at least one of batting/pitching/fielding"
            )
