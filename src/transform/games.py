"""Transform raw schedule games to TransformedGameData."""

from typing import List

from dags.mlb_types import ScheduleGame, TransformedGameData


def transform_games(schedule_games: List[ScheduleGame]) -> List[TransformedGameData]:
    """Clean raw schedule JSON and prepare for loading."""
    cleaned_data: List[TransformedGameData] = []
    for game in schedule_games:
        cleaned_data.append(
            TransformedGameData(
                game_pk=int(game["game_id"]),
                home_team=game["home_name"],
                away_team=game["away_name"],
                winning_team=game.get("winning_team", ""),
                season=int(game["game_date"][:4]),
                game_date=game["game_date"],
                game_type=game["game_type"],
                venue_id=game["venue_id"],
                home_team_id=game["home_id"],
                away_team_id=game["away_id"],
            )
        )
    return cleaned_data
