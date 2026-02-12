"""
ML predictions DAG: train batter (bat_woba) and pitcher (pit_fip) models, then predict for today's games.

Runs at 6am UTC (after main pipeline's rolling stats). Reads from player_rolling_stats and fact_game_state.
"""

import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-untyped]
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from typing import Any, Optional, cast
from pendulum import DateTime

from src.extract import get_schedule_for_date
from src.load.audit import check_freshness, record_load_audit
from src.load.predictions import load_predictions
from src.ml.features import build_batter_training_data, build_pitcher_training_data
from src.ml.predict import generate_predictions
from src.ml.players import ScheduledGame
from src.ml.train import train_batter_model, train_pitcher_model

FAILURE_ALERT_EMAILS = ["alerts@example.com"]
ML_MODEL_DIR = os.environ.get("ML_MODEL_DIR", "/opt/airflow/data/ml")
TRAINING_LOOKBACK_DAYS = 730  # ~2 seasons


@dag(
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mlb_analytics", "ml"],
    default_args={
        "on_failure_callback": [
            send_smtp_notification(
                from_email="airflow@example.com",
                to=FAILURE_ALERT_EMAILS,
                subject="[ML Predictions] Task {{ ti.task_id }} failed in {{ dag.dag_id }}",
                html_content=(
                    "<p>Task <strong>{{ ti.task_id }}</strong> failed.</p>"
                    "<p><strong>DAG:</strong> {{ dag.dag_id }}</p>"
                    "<p><strong>Logical date:</strong> {{ data_interval_start }}</p>"
                    "<p><strong>Log:</strong> <a href='{{ ti.log_url }}'>View log</a></p>"
                    "{% if exception %}<p><strong>Exception:</strong> <pre>{{ exception }}</pre></p>{% endif %}"
                ),
            )
        ],
    },
)
def ml_predictions_pipeline():

    @task()
    def check_upstream_freshness(
        conn_id: str = "mlb_postgres",
    ) -> None:
        """Mandatory: ensure mlb_player_stats was loaded within the last 24 hours."""
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            check_freshness(conn, "mlb_player_stats", max_age_hours=24)
        finally:
            conn.close()

    @task()
    def check_rolling_stats_ready(
        conn_id: str = "mlb_postgres", data_interval_start: Optional[DateTime] = None
    ) -> str:
        """Ensure player_rolling_stats has data for yesterday; otherwise raise."""
        if data_interval_start is not None:
            yesterday = (
                data_interval_start.in_timezone("UTC") - timedelta(days=1)
            ).date()
        else:
            raise ValueError("data_interval_start is required")
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(as_of_date) FROM player_rolling_stats")
                row = cur.fetchone()
            max_ao = row[0] if row and row[0] else None
            if max_ao is None or max_ao < yesterday:
                raise ValueError(
                    f"player_rolling_stats not ready: max as_of_date={max_ao!r}, need >= {yesterday}"
                )
            return str(yesterday)
        finally:
            conn.close()

    @task()
    def get_todays_schedule(
        data_interval_start: Optional[DateTime] = None,
    ) -> list[dict[str, Any]]:
        """Fetch schedule for prediction date (logical date) and return list of ScheduledGame dicts."""
        if data_interval_start is not None:
            pred_date = data_interval_start.in_timezone("UTC").date()
        else:
            raise ValueError("data_interval_start is required")
        date_str = pred_date.strftime("%m/%d/%Y")
        games = get_schedule_for_date(date_str)
        return [
            {
                "game_pk": int(g["game_id"]),
                "home_team_id": g["home_id"],
                "away_team_id": g["away_id"],
            }
            for g in games
        ]

    @task()
    def build_training_data(
        conn_id: str = "mlb_postgres",
    ) -> dict[str, Any]:
        """Compute training date range from DB and push for train tasks."""
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MIN(game_date), MAX(game_date) FROM dim_game WHERE game_date >= CURRENT_DATE - %s",
                    (TRAINING_LOOKBACK_DAYS,),
                )
                row = cur.fetchone()
            if not row or row[0] is None:
                return {"min_date": None, "max_date": None}
            return {"min_date": str(row[0]), "max_date": str(row[1])}
        finally:
            conn.close()

    @task()
    def train_batter_task(
        date_range: dict[str, Any],
        conn_id: str = "mlb_postgres",
    ) -> dict[str, Any]:
        """Build batter training data, fit pipeline, save to model_dir."""
        min_date = date_range.get("min_date")
        max_date = date_range.get("max_date")
        if not min_date or not max_date:
            return {"n_samples": 0, "trained_at": None}
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            X, y, _ = build_batter_training_data(conn, min_date, max_date)
            if X.empty or len(y) == 0:
                return {"n_samples": 0, "trained_at": None}
            _, meta = train_batter_model(X, y, model_dir=ML_MODEL_DIR)
            return meta
        finally:
            conn.close()

    @task()
    def train_pitcher_task(
        date_range: dict[str, Any],
        conn_id: str = "mlb_postgres",
    ) -> dict[str, Any]:
        """Build pitcher training data, fit pipeline, save to model_dir."""
        min_date = date_range.get("min_date")
        max_date = date_range.get("max_date")
        if not min_date or not max_date:
            return {"n_samples": 0, "trained_at": None}
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            X, y, _ = build_pitcher_training_data(conn, min_date, max_date)
            if X.empty or len(y) == 0:
                return {"n_samples": 0, "trained_at": None}
            _, meta = train_pitcher_model(X, y, model_dir=ML_MODEL_DIR)
            return meta
        finally:
            conn.close()

    @task()
    def generate_predictions_task(
        schedule: list[dict[str, Any]],
        conn_id: str = "mlb_postgres",
        data_interval_start: Optional[DateTime] = None,
    ) -> list[dict[str, Any]]:
        """Resolve players, load pipelines, predict bat_woba / pit_fip, return rows."""
        if data_interval_start is not None:
            prediction_date = data_interval_start.in_timezone("UTC").strftime(
                "%m/%d/%Y"
            )
        else:
            raise ValueError("data_interval_start is required")
        schedule_typed: list[ScheduledGame] = [
            {
                "game_pk": g["game_pk"],
                "home_team_id": g["home_team_id"],
                "away_team_id": g["away_team_id"],
            }
            for g in schedule
        ]
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            return generate_predictions(
                conn, prediction_date, schedule_typed, ML_MODEL_DIR
            )
        finally:
            conn.close()

    @task()
    def load_predictions_task(
        rows: list[dict[str, Any]],
        conn_id: str = "mlb_postgres",
    ) -> int:
        """Insert/upsert prediction rows into predictions table."""
        if not rows:
            return 0
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            n = load_predictions(conn, rows)
            conn.commit()
            return n
        finally:
            conn.close()

    @task()
    def record_load_audit_task(
        data_interval_start: Optional[DateTime] = None,
        conn_id: str = "mlb_postgres",
    ) -> None:
        """Record successful ml_predictions load for freshness checks."""
        if data_interval_start is None:
            raise ValueError("data_interval_start is required")
        prediction_date = data_interval_start.in_timezone("UTC").date()
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            record_load_audit(conn, "ml_predictions", prediction_date)
            conn.commit()
        finally:
            conn.close()

    # Task flow: freshness gate first, then rolling stats and schedule
    freshness = check_upstream_freshness()
    check = check_rolling_stats_ready()
    check.set_upstream(freshness)
    schedule = get_todays_schedule()
    schedule.set_upstream(check)
    date_range = build_training_data()
    date_range.set_upstream(schedule)
    train_batter = train_batter_task(cast(dict[str, Any], date_range))
    train_pitcher = train_pitcher_task(cast(dict[str, Any], date_range))
    preds = generate_predictions_task(cast(list[dict[str, Any]], schedule))
    preds.set_upstream(train_batter)
    preds.set_upstream(train_pitcher)
    load_result = load_predictions_task(cast(list[dict[str, Any]], preds))
    record_load_audit_task(conn_id="mlb_postgres").set_upstream(load_result)


ml_predictions_pipeline()
