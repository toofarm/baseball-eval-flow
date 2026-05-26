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


def test_load_staging_player_stats_issues_one_execute_per_chunk():
    """Regression test for the executemany-with-MERGE Snowflake anti-pattern.

    Previously this function called ``cur.executemany`` once per chunk, which
    Snowflake's connector silently degrades to N serial single-row MERGE
    round-trips. We now build a single multi-row MERGE per chunk via
    ``cur.execute``. Lock that in: 600 rows + chunk_size=500 should produce
    exactly 2 ``execute()`` calls and 0 ``executemany`` calls.
    """
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    rows = [
        {
            "game_pk": 100 + i,        # unique grain (game_pk, player_id)
            "player_id": i,
            "team_id": 1,
            "position_code": "1B",
            "position_name": "First Base",
            "stats": {"batting": {"hits": 1}},
        }
        for i in range(600)
    ]

    n = load_staging_player_stats(conn, rows, chunk_size=500)

    assert n == 600
    assert cur.execute.call_count == 2, (
        f"expected one execute() per chunk (2), got {cur.execute.call_count}"
    )
    assert cur.executemany.call_count == 0, (
        "executemany was called — that's the slow path we're escaping"
    )
    # First chunk SQL should carry 500 source rows in its USING(VALUES …) block,
    # and the bind list should have 500 * 10 = 5000 params.
    first_sql, first_params = cur.execute.call_args_list[0].args
    assert "MERGE INTO staging_player_stats" in first_sql
    assert "FROM VALUES" in first_sql
    assert "PARSE_JSON(column8)" in first_sql  # batting json column
    assert len(first_params) == 500 * 10
    # Second chunk has the remaining 100 rows × 10 cols.
    _, second_params = cur.execute.call_args_list[1].args
    assert len(second_params) == 100 * 10
