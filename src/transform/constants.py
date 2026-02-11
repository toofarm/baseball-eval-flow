"""Load and expose Fangraphs-style seasonal constants for wOBA, FIP, etc."""

import json
from pathlib import Path

_PATH = Path(__file__).parent / "constants.json"
# JSON structure: year (str) -> per-season constants dict
_DATA: dict[str, dict[str, float]] = json.loads(_PATH.read_text())


def get(season: int) -> dict[str, float]:
    """Return the constants dict for the given season.

    For seasons not in the data, uses the nearest available year (typically
    the most recent).
    """
    key = str(season)
    if key in _DATA:
        return _DATA[key]
    # Fallback: use closest available season
    years = sorted(int(y) for y in _DATA.keys())
    closest = min(years, key=lambda y: abs(y - season))
    return _DATA[str(closest)]
