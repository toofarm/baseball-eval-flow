"""
Generate next-game predictions using saved batter and pitcher pipelines.
"""

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import joblib
except ImportError:
    joblib = None  # type: ignore[assignment]

from src.ml.features import get_batter_features, get_pitcher_features
from src.ml.players import ScheduledGame, get_players_for_scheduled_games


def _load_pipeline_and_metadata(
    model_dir: Path, prefix: str, suffix: str = ""
) -> tuple[Any, dict[str, Any]]:
    """Load pipeline and metadata. suffix='' for Ridge, suffix='_hgb' for HGB."""
    pipe_name = f"{prefix}{suffix}_pipeline.joblib"
    meta_name = f"{prefix}{suffix}_metadata.json"
    path_pipe = model_dir / pipe_name
    path_meta = model_dir / meta_name
    if not path_pipe.exists():
        raise FileNotFoundError(f"Model not found: {path_pipe}")
    if joblib is None:
        raise ImportError("joblib is required to load pipelines")
    pipe = joblib.load(path_pipe)
    metadata: dict[str, Any] = {}
    if path_meta.exists():
        with open(path_meta) as f:
            metadata = json.load(f)
    return pipe, metadata


def generate_predictions(
    conn: Any,
    prediction_date: Any,
    schedule: list[ScheduledGame],
    model_dir: str | Path,
    as_of_date: Any | None = None,
) -> list[dict[str, Any]]:
    """
    Produce two rows per (game_pk, player_id): one for Ridge, one for HGB.

    - Resolves players via get_players_for_scheduled_games(conn, schedule, as_of_date).
    - as_of_date defaults to prediction_date - 1 day (features from day before).
    - Loads batter and pitcher pipelines (Ridge and HGB) from model_dir.
    - Returns list of dicts with keys: game_pk, player_id, model_type, as_of_date, pred_bat_woba,
      pred_pit_fip, model_version_bat, model_version_pit.
    """
    model_dir = Path(model_dir)
    if as_of_date is None:
        d = pd.Timestamp(prediction_date).date() if not isinstance(prediction_date, date) else prediction_date
        as_of_date = d - timedelta(days=1)

    player_tuples = get_players_for_scheduled_games(conn, schedule, as_of_date)
    if not player_tuples:
        return []

    batter_ridge, batter_ridge_meta = _load_pipeline_and_metadata(model_dir, "batter", "")
    pitcher_ridge, pitcher_ridge_meta = _load_pipeline_and_metadata(model_dir, "pitcher", "")
    batter_hgb, batter_hgb_meta = _load_pipeline_and_metadata(model_dir, "batter", "_hgb")
    pitcher_hgb, pitcher_hgb_meta = _load_pipeline_and_metadata(model_dir, "pitcher", "_hgb")

    batter_feat = get_batter_features(conn, as_of_date)
    pitcher_feat = get_pitcher_features(conn, as_of_date)
    batter_cols_ridge = batter_ridge_meta.get("feature_columns") or (list(batter_feat.columns) if not batter_feat.empty else [])
    pitcher_cols_ridge = pitcher_ridge_meta.get("feature_columns") or (list(pitcher_feat.columns) if not pitcher_feat.empty else [])
    batter_cols_hgb = batter_hgb_meta.get("feature_columns") or (list(batter_feat.columns) if not batter_feat.empty else [])
    pitcher_cols_hgb = pitcher_hgb_meta.get("feature_columns") or (list(pitcher_feat.columns) if not pitcher_feat.empty else [])

    rows: list[dict[str, Any]] = []
    for game_pk, player_id in player_tuples:
        for model_type, (batter_pipe, pitcher_pipe, batter_cols, pitcher_cols, b_meta, p_meta) in [
            ("ridge", (batter_ridge, pitcher_ridge, batter_cols_ridge, pitcher_cols_ridge, batter_ridge_meta, pitcher_ridge_meta)),
            ("hgb", (batter_hgb, pitcher_hgb, batter_cols_hgb, pitcher_cols_hgb, batter_hgb_meta, pitcher_hgb_meta)),
        ]:
            row: dict[str, Any] = {
                "game_pk": game_pk,
                "player_id": player_id,
                "model_type": model_type,
                "as_of_date": as_of_date,
                "pred_bat_woba": None,
                "pred_pit_fip": None,
                "model_version_bat": b_meta.get("trained_at", ""),
                "model_version_pit": p_meta.get("trained_at", ""),
            }
            if not batter_feat.empty and player_id in batter_feat.index and batter_cols:
                X_b = batter_feat.loc[[player_id]].reindex(columns=batter_cols)
                row["pred_bat_woba"] = float(batter_pipe.predict(X_b)[0])
            if not pitcher_feat.empty and player_id in pitcher_feat.index and pitcher_cols:
                X_p = pitcher_feat.loc[[player_id]].reindex(columns=pitcher_cols)
                row["pred_pit_fip"] = float(pitcher_pipe.predict(X_p)[0])
            rows.append(row)
    return rows
