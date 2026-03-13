"""
Train batter (bat_woba) and pitcher (pit_fip) regression pipelines.

Ridge pipeline: SimpleImputer (median) -> StandardScaler -> Ridge.
HGB pipeline: HistGradientBoostingRegressor only (handles NaN natively).
Saved with joblib plus a small metadata JSON (trained_at, feature_columns, metric).
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
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


def _split_train_val(
    X: pd.DataFrame,
    y: pd.Series,
    player_dates: pd.DataFrame | None,
    validation_days: int,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame | None, pd.Series | None, int, int | None]:
    """
    Time-based split: last validation_days = validation. Returns (X_train, y_train, X_val, y_val, n_train, n_val).
    When no split, returns (X, y, None, None, len(y), None).
    """
    if player_dates is None or validation_days <= 0:
        return X, y, None, None, len(y), None
    dates = pd.to_datetime(player_dates["game_date"])
    max_date = dates.max()
    cutoff = max_date - pd.Timedelta(days=validation_days)
    train_mask = dates <= cutoff
    if not train_mask.any():
        return X, y, None, None, len(y), None
    X_train = X.loc[train_mask]
    y_train = y.loc[train_mask]
    val_mask = ~train_mask
    n_val = int(val_mask.sum())
    X_val = X.loc[val_mask] if n_val > 0 else None
    y_val = y.loc[val_mask] if n_val > 0 else None
    return X_train, y_train, X_val, y_val, len(X_train), n_val


def _make_pipeline(
    model_type: Literal["ridge", "hgb"],
    random_state: int,
    **ridge_kwargs: Any,
) -> Any:
    """Build Ridge or HGB pipeline based on model_type."""
    if model_type == "ridge":
        return make_pipeline(
            SimpleImputer(strategy="median"),
            StandardScaler(),
            Ridge(random_state=random_state, **ridge_kwargs),
        )
    if model_type == "hgb":
        return HistGradientBoostingRegressor(
            max_iter=200,
            learning_rate=0.1,
            max_depth=5,
            random_state=random_state,
        )
    raise ValueError(f"model_type must be 'ridge' or 'hgb', got {model_type!r}")


def _get_pipeline_files(prefix: str, model_type: Literal["ridge", "hgb"]) -> tuple[str, str]:
    """Return (pipeline_filename, metadata_filename) for given prefix and model_type."""
    suffix = "_hgb" if model_type == "hgb" else ""
    return f"{prefix}{suffix}_pipeline.joblib", f"{prefix}{suffix}_metadata.json"


def train_batter_model(
    X: pd.DataFrame,
    y: pd.Series,
    model_dir: str | Path,
    model_type: Literal["ridge", "hgb"] = "ridge",
    player_dates: pd.DataFrame | None = None,
    validation_days: int = 0,
    random_state: int = 0,
    **ridge_kwargs: Any,
) -> tuple[Any, dict[str, Any]]:
    """
    Fit batter pipeline, save to model_dir, return (pipeline, metrics).

    X must have the same columns as get_batter_feature_column_names() (order matters).
    When player_dates and validation_days > 0, splits by game_date and computes val_rmse.
    model_type: "ridge" saves to batter_pipeline.joblib; "hgb" saves to batter_hgb_pipeline.joblib.
    """
    if joblib is None:
        raise ImportError("joblib is required for saving pipelines")
    model_dir = Path(model_dir)
    _ensure_model_dir(model_dir)

    X_train, y_train, X_val, y_val, n_train, n_val = _split_train_val(
        X, y, player_dates, validation_days
    )

    pipe = _make_pipeline(model_type, random_state, **ridge_kwargs)
    pipe.fit(X_train, y_train)
    train_rmse = float((mean_squared_error(y_train, pipe.predict(X_train))) ** 0.5)
    trained_at = datetime.now(timezone.utc).isoformat()
    feature_columns = list(X.columns)

    val_rmse: float | None = None
    if X_val is not None and y_val is not None and len(X_val) > 0:
        val_rmse = float((mean_squared_error(y_val, pipe.predict(X_val))) ** 0.5)

    metadata: dict[str, Any] = {
        "trained_at": trained_at,
        "feature_columns": feature_columns,
        "train_rmse": train_rmse,
        "n_samples": len(y),
        "model_type": model_type,
    }
    if n_val is not None:
        metadata["n_train"] = n_train
        metadata["n_val"] = n_val
    if val_rmse is not None:
        metadata["val_rmse"] = val_rmse

    pipe_file, meta_file = _get_pipeline_files("batter", model_type)
    joblib.dump(pipe, model_dir / pipe_file)
    with open(model_dir / meta_file, "w") as f:
        json.dump(metadata, f, indent=2)

    return pipe, metadata


def train_pitcher_model(
    X: pd.DataFrame,
    y: pd.Series,
    model_dir: str | Path,
    model_type: Literal["ridge", "hgb"] = "ridge",
    player_dates: pd.DataFrame | None = None,
    validation_days: int = 0,
    random_state: int = 0,
    **ridge_kwargs: Any,
) -> tuple[Any, dict[str, Any]]:
    """
    Fit pitcher pipeline, save to model_dir, return (pipeline, metrics).

    X must have the same columns as get_pitcher_feature_column_names() (order matters).
    When player_dates and validation_days > 0, splits by game_date and computes val_rmse.
    model_type: "ridge" saves to pitcher_pipeline.joblib; "hgb" saves to pitcher_hgb_pipeline.joblib.
    """
    if joblib is None:
        raise ImportError("joblib is required for saving pipelines")
    model_dir = Path(model_dir)
    _ensure_model_dir(model_dir)

    X_train, y_train, X_val, y_val, n_train, n_val = _split_train_val(
        X, y, player_dates, validation_days
    )

    pipe = _make_pipeline(model_type, random_state, **ridge_kwargs)
    pipe.fit(X_train, y_train)
    train_rmse = float((mean_squared_error(y_train, pipe.predict(X_train))) ** 0.5)
    trained_at = datetime.now(timezone.utc).isoformat()
    feature_columns = list(X.columns)

    val_rmse: float | None = None
    if X_val is not None and y_val is not None and len(X_val) > 0:
        val_rmse = float((mean_squared_error(y_val, pipe.predict(X_val))) ** 0.5)

    metadata: dict[str, Any] = {
        "trained_at": trained_at,
        "feature_columns": feature_columns,
        "train_rmse": train_rmse,
        "n_samples": len(y),
        "model_type": model_type,
    }
    if n_val is not None:
        metadata["n_train"] = n_train
        metadata["n_val"] = n_val
    if val_rmse is not None:
        metadata["val_rmse"] = val_rmse

    pipe_file, meta_file = _get_pipeline_files("pitcher", model_type)
    joblib.dump(pipe, model_dir / pipe_file)
    with open(model_dir / meta_file, "w") as f:
        json.dump(metadata, f, indent=2)

    return pipe, metadata
