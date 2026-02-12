"""Unit tests for src.ml.train."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.ml.train import train_batter_model, train_pitcher_model


def test_train_batter_model_saves_pipeline_and_metadata() -> None:
    n_features = len(__import__("src.ml.features", fromlist=["get_batter_feature_column_names"]).get_batter_feature_column_names())
    X = pd.DataFrame(
        [[0.1] * n_features, [0.2] * n_features, [0.3] * n_features],
        columns=__import__("src.ml.features", fromlist=["get_batter_feature_column_names"]).get_batter_feature_column_names(),
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
    n_features = len(__import__("src.ml.features", fromlist=["get_pitcher_feature_column_names"]).get_pitcher_feature_column_names())
    X = pd.DataFrame(
        [[1.0] * n_features, [2.0] * n_features],
        columns=__import__("src.ml.features", fromlist=["get_pitcher_feature_column_names"]).get_pitcher_feature_column_names(),
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
