"""
Type definitions for MLB constants.

The constants.json file is a flat mapping: year (str) -> SingleSeasonConstants.
"""

from typing import TypedDict


class SingleSeasonConstants(TypedDict, total=True):
    """Per-season constants for wOBA, FIP, etc. (Fangraphs-style).

    JSON also includes R/PA and R/W (accessed via .get()); those keys
    cannot be expressed in TypedDict due to identifier syntax.
    """

    wOBA: float
    wOBAScale: float
    wBB: float
    wHBP: float
    w1B: float
    w2B: float
    w3B: float
    wHR: float
    runSB: float
    runCS: float
    cFIP: float


# The constants.json structure: year string -> SingleSeasonConstants
SeasonConstantsByYear = dict[str, SingleSeasonConstants]
