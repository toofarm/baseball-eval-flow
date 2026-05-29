"""Boxscore extraction: fetch and parse MLB Stats API boxscore endpoint."""

from typing import Any, List

import requests

from src.mlb_types import PlayerStatsWithContext, ScheduleGame

# MLB boxscore API: https://statsapi.mlb.com/api/{ver}/game/{gamePk}/boxscore
MLB_BOXSCORE_BASE = "https://statsapi.mlb.com/api/v1/game"

# statsapi.mlb.com sits behind a CDN that returns 406 Not Acceptable for the
# default python-requests User-Agent. Send browser-like headers so the request
# is treated like a normal client (works from curl/browser for the same reason).
MLB_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _get_game_pk(game: Any) -> int:
    """Extract game_pk from TransformedGameData, dict, or schedule game."""
    if hasattr(game, "game_pk"):
        return game.game_pk
    if isinstance(game, dict):
        return int(game.get("game_pk") or game.get("game_id", 0))
    raise TypeError(f"game must have game_pk or be dict, got {type(game)}")


def fetch_boxscore(game_pk: int, timeout: int = 30) -> dict:
    """HTTP GET boxscore for game_pk. Returns raw JSON. Raises on HTTP error."""
    url = f"{MLB_BOXSCORE_BASE}/{game_pk}/boxscore"
    resp = requests.get(url, headers=MLB_API_HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def parse_boxscore_players(
    boxscore: dict, game_pk: int
) -> List[PlayerStatsWithContext]:
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
                    "position_type": str(position.get("type", "")),
                    "full_name": str(person.get("fullName", "")),
                    "stats": player_stats,
                }
            )
    return res


def fetch_player_stats_for_games(
    games: List[ScheduleGame], timeout: int = 30
) -> List[PlayerStatsWithContext]:
    """Fetch boxscore for each game and parse player stats. Returns combined list."""
    result: List[PlayerStatsWithContext] = []
    for game in games:
        game_pk = _get_game_pk(game)
        data = fetch_boxscore(game_pk, timeout=timeout)
        result.extend(parse_boxscore_players(data, game_pk))
    return result
