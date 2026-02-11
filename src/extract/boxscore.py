"""Boxscore extraction: fetch and parse MLB Stats API boxscore endpoint."""

from typing import List

import requests

from mlb_types import PlayerStatsWithContext, TransformedGameData

# MLB boxscore API: https://statsapi.mlb.com/api/{ver}/game/{gamePk}/boxscore
MLB_BOXSCORE_BASE = "https://statsapi.mlb.com/api/v1/game"


def fetch_boxscore(game_pk: int, timeout: int = 30) -> dict:
    """HTTP GET boxscore for game_pk. Returns raw JSON. Raises on HTTP error."""
    url = f"{MLB_BOXSCORE_BASE}/{game_pk}/boxscore"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def parse_boxscore_players(boxscore: dict, game_pk: int) -> List[PlayerStatsWithContext]:
    """Parse boxscore JSON teams -> players into list of PlayerStatsWithContext dicts."""
    res: List[PlayerStatsWithContext] = []
    teams_data = boxscore.get("teams") or {}
    for _team_key, team_obj in teams_data.items():
        players = team_obj.get("players")
        if not players or not isinstance(players, dict):
            continue
        for player in players.values():
            player_stats = player.get("stats")
            if not player_stats:
                continue
            person = player.get("person") or {}
            position = player.get("position") or {}
            player_id = person.get("id")
            team_id = player.get("parentTeamId")
            if player_id is None or team_id is None:
                continue
            res.append(
                {
                    "game_pk": game_pk,
                    "player_id": player_id,
                    "team_id": team_id,
                    "position_code": str(position.get("code", "")),
                    "position_name": str(position.get("name", "")),
                    "stats": player_stats,
                }
            )
    return res


def fetch_player_stats_for_games(
    games: List[TransformedGameData], timeout: int = 30
) -> List[PlayerStatsWithContext]:
    """Fetch boxscore for each game and parse player stats. Returns combined list."""
    result: List[PlayerStatsWithContext] = []
    for game in games:
        data = fetch_boxscore(game.game_pk, timeout=timeout)
        result.extend(parse_boxscore_players(data, game.game_pk))
    return result
