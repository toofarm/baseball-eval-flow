"""Unit tests for src.ml.features."""

import pandas as pd
import pytest

from src.ml.features import (
    BATTER_ROLLING_COLS,
    PITCHER_ROLLING_COLS,
    get_batter_feature_column_names,
    get_pitcher_feature_column_names,
    get_batter_features,
    get_pitcher_features,
    _pivot_rolling_to_wide,
)


def test_get_batter_feature_column_names() -> None:
    names = get_batter_feature_column_names()
    assert len(names) == len(BATTER_ROLLING_COLS) * 2
    assert all("_7" in n or "_30" in n for n in names)
    assert names[0].endswith("_7")
    assert names[len(BATTER_ROLLING_COLS)].endswith("_30")


def test_get_pitcher_feature_column_names() -> None:
    names = get_pitcher_feature_column_names()
    assert len(names) == len(PITCHER_ROLLING_COLS) * 2
    assert all("_7" in n or "_30" in n for n in names)


def test_pivot_rolling_to_wide_empty() -> None:
    out = _pivot_rolling_to_wide(pd.DataFrame(), BATTER_ROLLING_COLS)
    assert out.empty


def test_pivot_rolling_to_wide_two_players() -> None:
    cols = ["bat_woba", "bat_hits"]
    df = pd.DataFrame([
        {"player_id": 1, "as_of_date": "2024-01-01", "window_days": 7, "bat_woba": 0.35, "bat_hits": 5},
        {"player_id": 1, "as_of_date": "2024-01-01", "window_days": 30, "bat_woba": 0.33, "bat_hits": 20},
        {"player_id": 2, "as_of_date": "2024-01-01", "window_days": 7, "bat_woba": 0.40, "bat_hits": 8},
        {"player_id": 2, "as_of_date": "2024-01-01", "window_days": 30, "bat_woba": 0.38, "bat_hits": 25},
    ])
    out = _pivot_rolling_to_wide(df, cols)
    assert out.shape == (2, 6)
    assert "player_id" in out.columns and "as_of_date" in out.columns
    assert "bat_woba_7" in out.columns and "bat_woba_30" in out.columns
    assert "bat_hits_7" in out.columns and "bat_hits_30" in out.columns
    assert out[out["player_id"] == 1]["bat_woba_7"].iloc[0] == 0.35
    assert out[out["player_id"] == 1]["bat_woba_30"].iloc[0] == 0.33


def test_get_batter_features_empty_conn(monkeypatch: pytest.MonkeyPatch) -> None:
    """When read_sql returns empty DataFrame, get_batter_features returns empty DataFrame."""
    def mock_read_sql(*args: object, **kwargs: object) -> pd.DataFrame:
        return pd.DataFrame()
    import src.ml.features as mod
    monkeypatch.setattr(mod.pd, "read_sql", mock_read_sql)
    mock_conn = object()
    out = get_batter_features(mock_conn, "2024-01-01")
    assert out.empty


def test_get_batter_features_returns_wide_matrix(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_batter_features returns DataFrame indexed by player_id with _7 and _30 columns."""
    long_df = pd.DataFrame([
        {"player_id": 1, "as_of_date": "2024-01-01", "window_days": 7, **{c: 0.1 for c in BATTER_ROLLING_COLS}},
        {"player_id": 1, "as_of_date": "2024-01-01", "window_days": 30, **{c: 0.2 for c in BATTER_ROLLING_COLS}},
    ])
    def mock_read_sql(sql: object, conn: object, params: object = None) -> pd.DataFrame:
        return long_df.copy()
    import src.ml.features as mod
    monkeypatch.setattr(mod.pd, "read_sql", mock_read_sql)
    out = get_batter_features(object(), "2024-01-01")
    assert out.index.name == "player_id" or out.index.tolist() == [1]
    assert any("_7" in c for c in out.columns)
    assert any("_30" in c for c in out.columns)


def test_get_pitcher_features_empty_conn(monkeypatch: pytest.MonkeyPatch) -> None:
    def mock_read_sql(*args: object, **kwargs: object) -> pd.DataFrame:
        return pd.DataFrame()
    import src.ml.features as mod
    monkeypatch.setattr(mod.pd, "read_sql", mock_read_sql)
    out = get_pitcher_features(object(), "2024-01-01")
    assert out.empty
