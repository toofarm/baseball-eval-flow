"""Orchestrate player stats transformation to load-ready rows."""

from typing import List

from dags.mlb_types import (
    LoadReadyPlayerGame,
    PlayerStatsWithContext,
    TransformedGameData,
    TransformedPlayerData,
)

from src.transform.batting_advanced_metrics import transform_batting_stats
from src.transform.fielding_advanced_metrics import transform_fielding_stats
from src.transform.load_ready import to_load_ready_row
from src.transform.pitching_advanced_metrics import transform_pitching_stats


def transform_player_stats_to_load_ready(
    transformed_games: List[TransformedGameData],
    stats_with_context: List[PlayerStatsWithContext],
) -> List[LoadReadyPlayerGame]:
    """
    Transform each player's stats with correct game context and flatten to
    load-ready rows (one per game_pk, player_id) for fact_game_state.
    """
    game_by_pk = {g.game_pk: g for g in transformed_games}
    load_ready: List[LoadReadyPlayerGame] = []
    for item in stats_with_context:
        game = game_by_pk.get(item["game_pk"])
        if not game:
            continue
        stat = item["stats"]
        enriched: dict = {}
        if stat.get("pitching"):
            enriched["pitching"] = transform_pitching_stats(stat, game)
        if stat.get("batting"):
            enriched["batting"] = transform_batting_stats(stat, game)
        if stat.get("fielding"):
            enriched["fielding"] = transform_fielding_stats(stat, game)
        transformed = TransformedPlayerData(**enriched)
        row = to_load_ready_row(
            game_pk=item["game_pk"],
            player_id=item["player_id"],
            team_id=item["team_id"],
            position_code=item["position_code"],
            position_name=item["position_name"],
            transformed=transformed,
        )
        load_ready.append(row)
    return load_ready
