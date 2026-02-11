"""
Pitching advanced metrics.

Calculates FIP, xFIP, BABIP, and home run rate.
"""

from typing import cast

from dags.mlb_types import PlayerStats, TransformedGameData, TransformedPitchingStats

from src.transform import constants


def calculate_fip(
    bb: int,
    hbp: int,
    hr: int,
    k: int,
    ip: float,
    season: int,
) -> float:
    if ip == 0:
        return 0.0
    return ((13 * hr) + (3 * (bb + hbp)) - (2 * k)) / ip + constants.get(season)["cFIP"]


def calculate_xfip(
    bb: int,
    hbp: int,
    k: int,
    ip: int,
    fb: int,
    league_avg_hr: float,
    league_avg_fb: float,
    season: int,
) -> float:
    if ip == 0:
        return 0.0
    return (
        (13 * (fb * (league_avg_hr / league_avg_fb))) + (3 * (bb + hbp)) - (2 * k)
    ) / ip + constants.get(season)["cFIP"]


def calculate_babip(h: int, hr: int, ab: int, k: int, sf: int) -> float:
    if ab == 0:
        return 0.0
    if ab - k - hr + sf == 0:
        return 0.0
    return (h - hr) / (ab - k - hr + sf)


def home_run_rate(hr: int, fb: int) -> float:
    if fb == 0:
        return 0.0
    return (hr / fb) * 100


def transform_pitching_stats(
    player_stats: PlayerStats,
    game: TransformedGameData,
) -> TransformedPitchingStats:
    """Transform pitching stats for a given player."""
    if player_stats.get("pitching"):
        enriched_stats = cast(TransformedPitchingStats, player_stats.get("pitching"))

        enriched_stats["fip"] = calculate_fip(
            enriched_stats["baseOnBalls"],
            enriched_stats["hitByPitch"],
            enriched_stats["homeRuns"],
            enriched_stats["strikeOuts"],
            float(enriched_stats["inningsPitched"]),
            int(game.season),
        )

        enriched_stats["babip"] = calculate_babip(
            enriched_stats["hits"],
            enriched_stats["homeRuns"],
            enriched_stats["atBats"],
            enriched_stats["strikeOuts"],
            enriched_stats["sacFlies"],
        )

        enriched_stats["home_run_rate"] = home_run_rate(
            enriched_stats["homeRuns"],
            (
                enriched_stats["flyOuts"]
                + enriched_stats["sacFlies"]
                + enriched_stats["homeRuns"]
            ),
        )

    return TransformedPitchingStats(enriched_stats)
