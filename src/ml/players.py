"""
Resolve (game_pk, player_id) for scheduled games using DB only (Option A).

Players are those who appeared for either the home or away team in the last lookback_days.
"""

from typing import Any

from typing_extensions import TypedDict


class ScheduledGame(TypedDict):
    """Minimal game info from schedule: game_pk and team ids."""

    game_pk: int
    home_team_id: int
    away_team_id: int


def get_players_for_scheduled_games(
    conn: Any,
    games: list[ScheduledGame],
    as_of_date: Any,
    lookback_days: int = 7,
) -> list[tuple[int, int]]:
    """
    Return list of (game_pk, player_id) for all players to predict.

    For each game, selects distinct player_ids from fact_game_state join dim_game
    where game_date is in [as_of_date - lookback_days, as_of_date] and
    team_id is the game's home or away team. No MLB API calls.
    """
    if not games:
        return []

    result: list[tuple[int, int]] = []
    with conn.cursor() as cur:
        for g in games:
            game_pk = g["game_pk"]
            home_id = g["home_team_id"]
            away_id = g["away_team_id"]
            cur.execute(
                """
                SELECT DISTINCT f.player_id
                FROM fact_game_state f
                JOIN dim_game d ON f.game_pk = d.game_pk
                WHERE d.game_date >= %s::date - %s
                  AND d.game_date <= %s
                  AND f.team_id IN (%s, %s)
                """,
                (as_of_date, lookback_days, as_of_date, home_id, away_id),
            )
            for (player_id,) in cur.fetchall():
                result.append((game_pk, player_id))
    return result
