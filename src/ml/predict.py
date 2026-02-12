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
    model_dir: Path, prefix: str
) -> tuple[Any, dict[str, Any]]:
    path_pipe = model_dir / f"{prefix}_pipeline.joblib"
    path_meta = model_dir / f"{prefix}_metadata.json"
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
    Produce one row per (game_pk, player_id) with pred_bat_woba and/or pred_pit_fip.

    - Resolves players via get_players_for_scheduled_games(conn, schedule, as_of_date).
    - as_of_date defaults to prediction_date - 1 day (features from day before).
    - Loads batter and pitcher pipelines from model_dir; uses metadata for version and column order.
    - Returns list of dicts with keys: game_pk, player_id, as_of_date, pred_bat_woba, pred_pit_fip,
      model_version_bat, model_version_pit.
    """
    model_dir = Path(model_dir)
    if as_of_date is None:
        d = pd.Timestamp(prediction_date).date() if not isinstance(prediction_date, date) else prediction_date
        as_of_date = d - timedelta(days=1)

    player_tuples = get_players_for_scheduled_games(conn, schedule, as_of_date)
    if not player_tuples:
        return []

    batter_pipe, batter_meta = _load_pipeline_and_metadata(model_dir, "batter")
    pitcher_pipe, pitcher_meta = _load_pipeline_and_metadata(model_dir, "pitcher")
    version_bat = batter_meta.get("trained_at", "")
    version_pit = pitcher_meta.get("trained_at", "")

    batter_feat = get_batter_features(conn, as_of_date)
    pitcher_feat = get_pitcher_features(conn, as_of_date)
    batter_cols = batter_meta.get("feature_columns") or list(batter_feat.columns) if not batter_feat.empty else []
    pitcher_cols = pitcher_meta.get("feature_columns") or list(pitcher_feat.columns) if not pitcher_feat.empty else []

    rows: list[dict[str, Any]] = []
    for game_pk, player_id in player_tuples:
        row: dict[str, Any] = {
            "game_pk": game_pk,
            "player_id": player_id,
            "as_of_date": as_of_date,
            "pred_bat_woba": None,
            "pred_pit_fip": None,
            "model_version_bat": version_bat,
            "model_version_pit": version_pit,
        }
        if not batter_feat.empty and player_id in batter_feat.index and batter_cols:
            X_b = batter_feat.loc[[player_id]].reindex(columns=batter_cols)
            row["pred_bat_woba"] = float(batter_pipe.predict(X_b)[0])
        if not pitcher_feat.empty and player_id in pitcher_feat.index and pitcher_cols:
            X_p = pitcher_feat.loc[[player_id]].reindex(columns=pitcher_cols)
            row["pred_pit_fip"] = float(pitcher_pipe.predict(X_p)[0])
        rows.append(row)
    return rows
