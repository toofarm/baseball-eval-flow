"""
Train batter (bat_woba) and pitcher (pit_fip) regression pipelines.

Each pipeline: SimpleImputer (median) -> StandardScaler -> Ridge.
Saved with joblib plus a small metadata JSON (trained_at, feature_columns, metric).
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

try:
    import joblib
except ImportError:
    joblib = None  # type: ignore[assignment]


def _ensure_model_dir(model_dir: Path) -> None:
    model_dir = Path(model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)


def train_batter_model(
    X: pd.DataFrame,
    y: pd.Series,
    model_dir: str | Path,
    random_state: int = 0,
    **ridge_kwargs: Any,
) -> tuple[Any, dict[str, Any]]:
    """
    Fit batter pipeline (impute -> scale -> Ridge), save to model_dir, return (pipeline, metrics).

    X must have the same columns as get_batter_feature_column_names() (order matters).
    Saves batter_pipeline.joblib and batter_metadata.json.
    """
    if joblib is None:
        raise ImportError("joblib is required for saving pipelines")
    model_dir = Path(model_dir)
    _ensure_model_dir(model_dir)

    pipe = make_pipeline(
        SimpleImputer(strategy="median"),
        StandardScaler(),
        Ridge(random_state=random_state, **ridge_kwargs),
    )
    pipe.fit(X, y)
    y_pred = pipe.predict(X)
    rmse = float((mean_squared_error(y, y_pred)) ** 0.5)
    trained_at = datetime.now(timezone.utc).isoformat()
    feature_columns = list(X.columns)

    joblib.dump(pipe, model_dir / "batter_pipeline.joblib")
    metadata = {
        "trained_at": trained_at,
        "feature_columns": feature_columns,
        "train_rmse": rmse,
        "n_samples": len(y),
    }
    with open(model_dir / "batter_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    return pipe, metadata


def train_pitcher_model(
    X: pd.DataFrame,
    y: pd.Series,
    model_dir: str | Path,
    random_state: int = 0,
    **ridge_kwargs: Any,
) -> tuple[Any, dict[str, Any]]:
    """
    Fit pitcher pipeline (impute -> scale -> Ridge), save to model_dir, return (pipeline, metrics).

    X must have the same columns as get_pitcher_feature_column_names() (order matters).
    Saves pitcher_pipeline.joblib and pitcher_metadata.json.
    """
    if joblib is None:
        raise ImportError("joblib is required for saving pipelines")
    model_dir = Path(model_dir)
    _ensure_model_dir(model_dir)

    pipe = make_pipeline(
        SimpleImputer(strategy="median"),
        StandardScaler(),
        Ridge(random_state=random_state, **ridge_kwargs),
    )
    pipe.fit(X, y)
    y_pred = pipe.predict(X)
    rmse = float((mean_squared_error(y, y_pred)) ** 0.5)
    trained_at = datetime.now(timezone.utc).isoformat()
    feature_columns = list(X.columns)

    joblib.dump(pipe, model_dir / "pitcher_pipeline.joblib")
    metadata = {
        "trained_at": trained_at,
        "feature_columns": feature_columns,
        "train_rmse": rmse,
        "n_samples": len(y),
    }
    with open(model_dir / "pitcher_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    return pipe, metadata
