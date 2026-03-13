import pytest

from src.load.staging import load_staging_player_stats

from unittest.mock import MagicMock, patch


@patch("src.load.staging.load_staging_player_stats")
def test_load_staging_player_stats(mock_load):
    mock_load.return_value = 1
    conn = MagicMock()
    stats_with_context = [
        {
            "game_pk": 12345,
            "player_id": 1,
            "team_id": 1,
            "position_code": "1B",
            "position_name": "First Base",
            "stats": {"batting": {"hits": 1, "runs": 1, "rbi": 1, "average": 0.300}},
        }
    ]
    load_staging_player_stats(conn, stats_with_context)


def test_load_staging_player_stats_empty():
    conn = MagicMock()
    stats_with_context = []
    load_staging_player_stats(conn, stats_with_context)
    assert conn.commit.call_count == 0


def test_load_staging_player_stats_large_payload():
    conn = MagicMock()
    stats_with_context = [
        {
            "game_pk": 12345,
            "player_id": 1,
            "team_id": 1,
            "position_code": "1B",
            "position_name": "First Base",
            "stats": {
                "batting": {"hits": 1, "runs": 1, "rbi": 1, "average": 0.300},
                "pitching": {
                    "wins": 1,
                    "losses": 1,
                    "era": 3.00,
                    " innings_pitched": 10.0,
                },
            },
            "fielding": {"errors": 1, "assists": 1, "putouts": 1, "total_chances": 10},
        }
    ] * 1100
    load_staging_player_stats(conn, stats_with_context)
