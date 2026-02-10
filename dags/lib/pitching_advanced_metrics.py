"""
Pitching advanced metrics.

Calculates FIP, xFIP, BABIP, and home run rate.

Glossary:
- bb: base on balls
- hbp: hit by pitch
- hr: home runs
- k: strikeouts
- sf: sacrifice flies
- ip: innings pitched
- fb: fly balls
- ab: at bats
- h: hits
- league_avg_hr: league average home runs
- league_avg_fb: league average fly balls
- constant: constant for FIP and xFIP
"""

import lib.constants as constants


from mlb_types import TransformedPitchingStats, PlayerStats, TransformedGameData

from typing import cast


def calculate_fip(
    bb: int,
    hbp: int,
    hr: int,
    k: int,
    ip: int,
    season: int,
) -> float:
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
    return (
        (13 * (fb * (league_avg_hr / league_avg_fb))) + (3 * (bb + hbp)) - (2 * k)
    ) / ip + constants.get(season)["cFIP"]


def calculate_babip(h: int, hr: int, ab: int, k: int, sf: int) -> float:
    return (h - hr) / (ab - k - hr + sf)


def home_run_rate(hr: int, fb: int) -> float:
    return (hr / fb) * 100


# Export to ETL DAG
def transform_pitching_stats(
    player_stats: PlayerStats,
    game: TransformedGameData,
) -> TransformedPitchingStats:
    """
    Transform pitching stats for a given player [11].
    """
    if player_stats.get("pitching"):
        enriched_stats = cast(TransformedPitchingStats, player_stats.get("pitching"))

        enriched_stats["fip"] = calculate_fip(
            enriched_stats["baseOnBalls"],
            enriched_stats["hitByPitch"],
            enriched_stats["homeRuns"],
            enriched_stats["strikeOuts"],
            int(enriched_stats["inningsPitched"]),
            int(game.season),
        )

        enriched_stats["babip"] = calculate_babip(
            enriched_stats["hits"],
            enriched_stats["homeRuns"],
            enriched_stats["atBats"],
            enriched_stats["strikeOuts"],
            enriched_stats["sacFlies"],
        )

        # We add up all the fly outs and home runs to get the total fly ball count
        enriched_stats["home_run_rate"] = home_run_rate(
            enriched_stats["homeRuns"],
            (
                enriched_stats["flyOuts"]
                + enriched_stats["sacFlies"]
                + enriched_stats["homeRuns"]
            ),
        )

    return TransformedPitchingStats(enriched_stats)
