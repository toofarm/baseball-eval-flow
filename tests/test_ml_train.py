"""Unit tests for src.ml.train."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.ml.train import train_batter_model, train_pitcher_model


def test_train_batter_model_saves_pipeline_and_metadata() -> None:
    n_features = len(
        __import__(
            "src.ml.features", fromlist=["get_batter_feature_column_names"]
        ).get_batter_feature_column_names()
    )
    X = pd.DataFrame(
        [[0.1] * n_features, [0.2] * n_features, [0.3] * n_features],
        columns=__import__(
            "src.ml.features", fromlist=["get_batter_feature_column_names"]
        ).get_batter_feature_column_names(),
    )
    y = pd.Series([0.32, 0.35, 0.38])
    with tempfile.TemporaryDirectory() as d:
        pipe, meta = train_batter_model(X, y, model_dir=d)
        assert hasattr(pipe, "predict")
        assert pipe.predict(X).shape == (3,)
        assert meta["train_rmse"] >= 0
        assert "trained_at" in meta
        assert "feature_columns" in meta
        assert (Path(d) / "batter_pipeline.joblib").exists()
        assert (Path(d) / "batter_metadata.json").exists()


def test_train_pitcher_model_saves_pipeline_and_metadata() -> None:
    n_features = len(
        __import__(
            "src.ml.features", fromlist=["get_pitcher_feature_column_names"]
        ).get_pitcher_feature_column_names()
    )
    X = pd.DataFrame(
        [[1.0] * n_features, [2.0] * n_features],
        columns=__import__(
            "src.ml.features", fromlist=["get_pitcher_feature_column_names"]
        ).get_pitcher_feature_column_names(),
    )
    y = pd.Series([3.50, 4.00])
    with tempfile.TemporaryDirectory() as d:
        pipe, meta = train_pitcher_model(X, y, model_dir=d)
        assert hasattr(pipe, "predict")
        pred = pipe.predict(X)
        assert pred.shape == (2,)
        assert all(3.0 <= p <= 5.0 for p in pred)
        assert meta["n_samples"] == 2
        assert (Path(d) / "pitcher_pipeline.joblib").exists()
        assert (Path(d) / "pitcher_metadata.json").exists()


def test_train_batter_model_handles_imputation() -> None:
    from src.ml.features import get_batter_feature_column_names

    cols = get_batter_feature_column_names()
    X = pd.DataFrame([[float("nan")] * len(cols), [0.5] * len(cols)], columns=cols)
    y = pd.Series([0.30, 0.35])
    with tempfile.TemporaryDirectory() as d:
        pipe, _ = train_batter_model(X, y, model_dir=d)
        out = pipe.predict(X)
    assert out.shape == (2,)
    assert not any(pd.isna(out))


def test_train_batter_model_with_validation() -> None:
    """With player_dates and validation_days, metadata includes val_rmse, n_train, n_val."""
    from src.ml.features import get_batter_feature_column_names

    cols = get_batter_feature_column_names()
    n_features = len(cols)
    # 10 rows: 5 with game_date 2024-01-01..05 (train), 5 with 2024-01-06..10 (val)
    dates = pd.to_datetime(
        ["2024-01-01"] * 2
        + ["2024-01-03"] * 2
        + ["2024-01-05"] * 1
        + ["2024-01-06"] * 2
        + ["2024-01-08"] * 2
        + ["2024-01-10"] * 1
    )
    X = pd.DataFrame(
        [[0.2] * n_features for _ in range(10)],
        columns=cols,
    )
    y = pd.Series([0.32] * 10)
    player_dates = pd.DataFrame({"player_id": range(10), "game_date": dates})
    with tempfile.TemporaryDirectory() as d:
        pipe, meta = train_batter_model(
            X,
            y,
            model_dir=d,
            player_dates=player_dates,
            validation_days=5,
        )
    assert meta["val_rmse"] is not None
    assert meta["val_rmse"] >= 0
    assert meta["n_train"] + meta["n_val"] == meta["n_samples"]
    assert meta["n_val"] > 0


def test_train_with_validation_no_val_data() -> None:
    """When validation_days exceeds date range, fall back to no split; val_rmse is None."""
    from src.ml.features import get_batter_feature_column_names

    cols = get_batter_feature_column_names()
    n_features = len(cols)
    # 5 rows all within 5 days; validation_days=30 puts cutoff before our data
    dates = pd.to_datetime(
        ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
    )
    X = pd.DataFrame([[0.2] * n_features for _ in range(5)], columns=cols)
    y = pd.Series([0.32] * 5)
    player_dates = pd.DataFrame({"player_id": range(5), "game_date": dates})
    with tempfile.TemporaryDirectory() as d:
        _, meta = train_batter_model(
            X,
            y,
            model_dir=d,
            player_dates=player_dates,
            validation_days=30,
        )
    assert meta.get("val_rmse") is None
    assert meta["n_samples"] == 5


def test_train_without_player_dates_unchanged() -> None:
    """When player_dates is None, no validation; behavior unchanged."""
    from src.ml.features import get_batter_feature_column_names

    cols = get_batter_feature_column_names()
    n_features = len(cols)
    X = pd.DataFrame([[0.2] * n_features, [0.3] * n_features], columns=cols)
    y = pd.Series([0.32, 0.35])
    with tempfile.TemporaryDirectory() as d:
        _, meta = train_batter_model(
            X,
            y,
            model_dir=d,
            player_dates=None,
            validation_days=30,
        )
    assert meta.get("val_rmse") is None
    assert "n_val" not in meta or meta.get("n_val") is None
    assert meta["n_samples"] == 2


def test_train_batter_model_hgb_saves_separate_files() -> None:
    """With model_type='hgb', saves to batter_hgb_* files; metadata has model_type and val_rmse."""
    from src.ml.features import get_batter_feature_column_names

    cols = get_batter_feature_column_names()
    n_features = len(cols)
    dates = pd.to_datetime(
        ["2024-01-01"] * 3 + ["2024-01-06"] * 2 + ["2024-01-10"] * 1
    )
    X = pd.DataFrame([[0.2] * n_features for _ in range(6)], columns=cols)
    y = pd.Series([0.32] * 6)
    player_dates = pd.DataFrame({"player_id": range(6), "game_date": dates})
    with tempfile.TemporaryDirectory() as d:
        pipe, meta = train_batter_model(
            X, y, model_dir=d,
            model_type="hgb",
            player_dates=player_dates, validation_days=5,
        )
        assert (Path(d) / "batter_hgb_pipeline.joblib").exists()
        assert (Path(d) / "batter_hgb_metadata.json").exists()
        assert not (Path(d) / "batter_pipeline.joblib").exists()
    assert meta["model_type"] == "hgb"
    assert meta["val_rmse"] is not None
    assert meta["val_rmse"] >= 0


def test_train_pitcher_model_hgb() -> None:
    """With model_type='hgb', saves to pitcher_hgb_* files."""
    from src.ml.features import get_pitcher_feature_column_names

    cols = get_pitcher_feature_column_names()
    n_features = len(cols)
    X = pd.DataFrame([[1.0] * n_features, [2.0] * n_features], columns=cols)
    y = pd.Series([3.50, 4.00])
    with tempfile.TemporaryDirectory() as d:
        pipe, meta = train_pitcher_model(X, y, model_dir=d, model_type="hgb")
        assert (Path(d) / "pitcher_hgb_pipeline.joblib").exists()
        assert (Path(d) / "pitcher_hgb_metadata.json").exists()
        assert meta["model_type"] == "hgb"
        pred = pipe.predict(X)
    assert pred.shape == (2,)
    assert not any(pd.isna(pred))


def test_train_hgb_handles_nan() -> None:
    """HGB pipeline with NaN in X returns valid predictions (no NaN output)."""
    from src.ml.features import get_batter_feature_column_names

    cols = get_batter_feature_column_names()
    X = pd.DataFrame(
        [[float("nan")] * len(cols), [0.5] * len(cols)],
        columns=cols,
    )
    y = pd.Series([0.30, 0.35])
    with tempfile.TemporaryDirectory() as d:
        pipe, _ = train_batter_model(X, y, model_dir=d, model_type="hgb")
        out = pipe.predict(X)
    assert out.shape == (2,)
    assert not any(pd.isna(out))
