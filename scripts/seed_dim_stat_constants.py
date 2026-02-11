#!/usr/bin/env python3
"""
Populate dim_stat_constants from src/transform/constants.json.

Run after applying schema/04_stat_constants.sql. Idempotent: re-run to add
new seasons or update existing rows.

Usage:
  Set MLB_DATABASE_URL (or DATABASE_URL) to your Postgres connection string, then:
  python scripts/seed_dim_stat_constants.py

  From project root, with PYTHONPATH including the app:
  PYTHONPATH=. python scripts/seed_dim_stat_constants.py

Adding new seasons: add the year and constants to src/transform/constants.json
(see Fangraphs or the existing JSON shape), then re-run this script.
"""

import json
import os
import sys
from pathlib import Path

# Allow running from project root; constants.json is under src/transform
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONSTANTS_JSON = PROJECT_ROOT / "src" / "transform" / "constants.json"

COLUMNS = [
    "season",
    "woba",
    "woba_scale",
    "w_bb",
    "w_hbp",
    "w_1b",
    "w_2b",
    "w_3b",
    "w_hr",
    "r_per_pa",
]


def load_constants() -> list[dict]:
    """Load constants from JSON and return rows for dim_stat_constants."""
    data = json.loads(CONSTANTS_JSON.read_text())
    rows = []
    for year_str, vals in data.items():
        season = int(year_str)
        row = {
            "season": season,
            "woba": vals["wOBA"],
            "woba_scale": vals["wOBAScale"],
            "w_bb": vals["wBB"],
            "w_hbp": vals["wHBP"],
            "w_1b": vals["w1B"],
            "w_2b": vals["w2B"],
            "w_3b": vals["w3B"],
            "w_hr": vals["wHR"],
            "r_per_pa": vals["R/PA"],
        }
        rows.append(row)
    return rows


def main() -> int:
    url = os.environ.get("MLB_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        print(
            "Set MLB_DATABASE_URL or DATABASE_URL to your Postgres connection string.",
            file=sys.stderr,
        )
        return 1
    try:
        import psycopg2
    except ImportError:
        print(
            "Install psycopg2-binary (or psycopg2) to run this script.", file=sys.stderr
        )
        return 1

    rows = load_constants()
    if not rows:
        print("No rows in constants.json.", file=sys.stderr)
        return 1

    col_list = ", ".join(COLUMNS)
    placeholders = ", ".join("%s" for _ in COLUMNS)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "season")
    sql = f"""
        INSERT INTO dim_stat_constants ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (season) DO UPDATE SET {updates}
    """

    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(sql, [r[c] for c in COLUMNS])
        conn.commit()
        print(f"Upserted {len(rows)} rows into dim_stat_constants.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
