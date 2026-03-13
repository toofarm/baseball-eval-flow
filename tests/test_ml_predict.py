"""Unit tests for src.ml.predict."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.ml.features import (
    get_batter_feature_column_names,
    get_pitcher_feature_column_names,
)
from src.ml.predict import generate_predictions
from src.ml.train import train_batter_model, train_pitcher_model


def test_generate_predictions_emits_two_rows_per_game_player(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When both Ridge and HGB pipelines exist, returns two rows per (game_pk, player_id)."""
    batter_cols = get_batter_feature_column_names()
    pitcher_cols = get_pitcher_feature_column_names()
    player_id = 100
    game_pk = 1

    with tempfile.TemporaryDirectory() as d:
        model_dir = Path(d)
        # Train both Ridge and HGB for batter and pitcher
        X_b = pd.DataFrame([[0.2] * len(batter_cols)], columns=batter_cols, index=[player_id])
        y_b = pd.Series([0.35], index=[player_id])
        train_batter_model(X_b, y_b, model_dir=model_dir)
        train_batter_model(X_b, y_b, model_dir=model_dir, model_type="hgb")

        X_p = pd.DataFrame([[2.0] * len(pitcher_cols)], columns=pitcher_cols, index=[player_id])
        y_p = pd.Series([3.50], index=[player_id])
        train_pitcher_model(X_p, y_p, model_dir=model_dir)
        train_pitcher_model(X_p, y_p, model_dir=model_dir, model_type="hgb")

        batter_feat = pd.DataFrame(
            [[0.2] * len(batter_cols)],
            columns=batter_cols,
            index=pd.Index([player_id], name="player_id"),
        )
        pitcher_feat = pd.DataFrame(
            [[2.0] * len(pitcher_cols)],
            columns=pitcher_cols,
            index=pd.Index([player_id], name="player_id"),
        )

        def mock_get_players(*args: object, **kwargs: object) -> list[tuple[int, int]]:
            return [(game_pk, player_id)]

        def mock_get_batter_features(*args: object, **kwargs: object) -> pd.DataFrame:
            return batter_feat.copy()

        def mock_get_pitcher_features(*args: object, **kwargs: object) -> pd.DataFrame:
            return pitcher_feat.copy()

        import src.ml.predict as predict_mod

        monkeypatch.setattr(
            predict_mod, "get_players_for_scheduled_games", mock_get_players
        )
        monkeypatch.setattr(predict_mod, "get_batter_features", mock_get_batter_features)
        monkeypatch.setattr(predict_mod, "get_pitcher_features", mock_get_pitcher_features)

        schedule = [{"game_pk": game_pk, "home_team_id": 1, "away_team_id": 2}]
        rows = generate_predictions(
            MagicMock(), "2024-06-01", schedule, model_dir
        )

    ridge_rows = [r for r in rows if r["model_type"] == "ridge"]
    hgb_rows = [r for r in rows if r["model_type"] == "hgb"]
    assert len(ridge_rows) == 1
    assert len(hgb_rows) == 1
    assert ridge_rows[0]["game_pk"] == game_pk
    assert ridge_rows[0]["player_id"] == player_id
    assert hgb_rows[0]["game_pk"] == game_pk
    assert hgb_rows[0]["player_id"] == player_id
    assert ridge_rows[0]["pred_bat_woba"] is not None
    assert hgb_rows[0]["pred_bat_woba"] is not None
