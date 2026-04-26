"""
Offload connection helper.

DAGs run their offload writes against Snowflake in both dev and prod. The
specific Airflow connection id is selected at runtime via the
``MLB_OFFLOAD_CONN_ID`` environment variable, defaulting to
``snowflake-baseball``. Centralizing the lookup here keeps the DAG tasks free
of hardcoded conn ids and gives us a single seam for any future backend
swaps.
"""

from __future__ import annotations

import os
from typing import Optional

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # type: ignore[import-untyped]

DEFAULT_CONN_ID = "snowflake-baseball"
ENV_VAR = "MLB_OFFLOAD_CONN_ID"


def get_offload_conn_id() -> str:
    """Return the Airflow connection id used for DAG offload writes."""
    return os.environ.get(ENV_VAR, DEFAULT_CONN_ID)


def get_offload_hook(conn_id: Optional[str] = None) -> SnowflakeHook:
    """Return a SnowflakeHook for the configured offload connection."""
    return SnowflakeHook(snowflake_conn_id=conn_id or get_offload_conn_id())
