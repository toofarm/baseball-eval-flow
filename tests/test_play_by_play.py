"""Tests for play-by-play extract: parse_play_by_play_pitches.

The parser is the trickiest piece because it (a) skips non-pitch events,
(b) reconstructs pre-pitch counts from post-pitch counts, and (c) tracks outs
across at-bats within the same half-inning. These tests exercise each.
"""

from src.extract.play_by_play import parse_play_by_play_pitches


def _pitch(pitch_number, balls_after, strikes_after, outs_after, **extra):
    """Minimal pitch event with API-shaped count + details."""
    base = {
        "isPitch": True,
        "type": "pitch",
        "pitchNumber": pitch_number,
        "details": {
            "call": {"code": "B"},
            "type": {"code": "FF"},
            "isStrike": False,
            "isBall": True,
            "isInPlay": False,
        },
        "count": {
            "balls": balls_after,
            "strikes": strikes_after,
            "outs": outs_after,
        },
        "pitchData": {
            "startSpeed": 94.5,
            "endSpeed": 85.8,
            "zone": 12,
            "coordinates": {"pX": 1.4, "pZ": 2.4},
            "breaks": {
                "spinRate": 2400,
                "spinDirection": 200,
                "breakVerticalInduced": 17.4,
                "breakHorizontal": 6.2,
                "breakAngle": 22.8,
                "breakLength": 3.6,
                "breakVertical": -13.4,
            },
        },
    }
    base.update(extra)
    return base


def _play(at_bat_index, inning, half_inning, events, event_name="Single"):
    return {
        "atBatIndex": at_bat_index,
        "about": {
            "atBatIndex": at_bat_index,
            "inning": inning,
            "halfInning": half_inning,
            "isComplete": True,
        },
        "matchup": {
            "batter": {"id": 1000 + at_bat_index, "fullName": "B"},
            "pitcher": {"id": 9000, "fullName": "P"},
            "batSide": {"code": "R"},
            "pitchHand": {"code": "R"},
        },
        "result": {"type": "atBat", "event": event_name},
        "playEvents": events,
    }


def test_empty_pbp_returns_empty_list():
    assert parse_play_by_play_pitches({"allPlays": []}, 100) == []
    assert parse_play_by_play_pitches({}, 100) == []


def test_basic_pitch_extraction():
    pbp = {"allPlays": [_play(0, 1, "top", [_pitch(1, 1, 0, 0)])]}
    rows = parse_play_by_play_pitches(pbp, 12345)
    assert len(rows) == 1
    r = rows[0]
    assert r["game_pk"] == 12345
    assert r["at_bat_index"] == 0
    assert r["pitch_number"] == 1
    assert r["pitcher_id"] == 9000
    assert r["batter_id"] == 1000
    assert r["pitch_type_code"] == "FF"
    assert r["start_speed"] == 94.5
    assert r["spin_rate"] == 2400
    assert r["break_vertical_induced"] == 17.4
    assert r["inning"] == 1
    assert r["half_inning"] == "top"
    assert r["at_bat_event"] == "Single"


def test_pre_counts_within_at_bat():
    """balls_before / strikes_before come from previous pitch's post-count."""
    pbp = {
        "allPlays": [
            _play(
                0,
                1,
                "top",
                [
                    _pitch(1, 1, 0, 0),  # ball:    pre 0-0, post 1-0
                    _pitch(2, 1, 1, 0),  # strike:  pre 1-0, post 1-1
                    _pitch(3, 2, 1, 0),  # ball:    pre 1-1, post 2-1
                    _pitch(4, 3, 1, 0),  # ball:    pre 2-1, post 3-1
                ],
            )
        ]
    }
    rows = parse_play_by_play_pitches(pbp, 1)
    pre = [(r["balls_before"], r["strikes_before"]) for r in rows]
    assert pre == [(0, 0), (1, 0), (1, 1), (2, 1)]


def test_non_pitch_events_skipped():
    """playEvents may include substitutions / action events without isPitch."""
    action = {"type": "action", "isPitch": False, "details": {"event": "Pickoff"}}
    pbp = {
        "allPlays": [
            _play(
                0,
                1,
                "top",
                [_pitch(1, 1, 0, 0), action, _pitch(2, 2, 0, 0)],
            )
        ]
    }
    rows = parse_play_by_play_pitches(pbp, 1)
    assert [r["pitch_number"] for r in rows] == [1, 2]
    # Pre-count for pitch 2 should still come from pitch 1's post-count.
    assert (rows[1]["balls_before"], rows[1]["strikes_before"]) == (1, 0)


def test_outs_carry_within_half_inning():
    """Outs from a completed at-bat should appear as outs_before in the next AB
    in the same half-inning, then reset on half-inning change."""
    pbp = {
        "allPlays": [
            # Top 1, AB 0: ends with 1 out (strikeout)
            _play(
                0,
                1,
                "top",
                [
                    _pitch(1, 0, 1, 0),
                    _pitch(2, 0, 2, 0),
                    _pitch(3, 0, 3, 1),  # K, outs goes 0 -> 1
                ],
                event_name="Strikeout",
            ),
            # Top 1, AB 1: starts with 1 out
            _play(
                1,
                1,
                "top",
                [_pitch(1, 1, 0, 1)],
                event_name="Walk",
            ),
            # Bottom 1, AB 2: half-inning changed, outs reset to 0
            _play(
                2,
                1,
                "bottom",
                [_pitch(1, 1, 0, 0)],
                event_name="Walk",
            ),
        ]
    }
    rows = parse_play_by_play_pitches(pbp, 1)
    outs = [(r["at_bat_index"], r["pitch_number"], r["outs_before"]) for r in rows]
    # AB 0: outs_before = 0 for every pitch (outs entering the play)
    assert outs[0] == (0, 1, 0)
    assert outs[2] == (0, 3, 0)
    # AB 1 in same half-inning: outs_before = 1
    assert outs[3] == (1, 1, 1)
    # AB 2 in new half-inning: outs reset
    assert outs[4] == (2, 1, 0)


def test_skips_pitch_with_missing_required_fields():
    """A pitch event missing pitcher/batter/pitch_number is dropped."""
    bad = {
        "isPitch": True,
        "type": "pitch",
        # no pitchNumber
        "details": {"call": {"code": "B"}, "type": {"code": "FF"}},
        "count": {"balls": 1, "strikes": 0, "outs": 0},
    }
    pbp = {"allPlays": [_play(0, 1, "top", [bad, _pitch(2, 2, 0, 0)])]}
    rows = parse_play_by_play_pitches(pbp, 1)
    assert len(rows) == 1
    assert rows[0]["pitch_number"] == 2


def test_handles_missing_pitch_data():
    """When pitchData/breaks are absent the row still emits with nulls."""
    sparse = {
        "isPitch": True,
        "type": "pitch",
        "pitchNumber": 1,
        "details": {"call": {"code": "B"}, "type": {"code": "FF"}},
        "count": {"balls": 1, "strikes": 0, "outs": 0},
        # no pitchData at all
    }
    pbp = {"allPlays": [_play(0, 1, "top", [sparse])]}
    rows = parse_play_by_play_pitches(pbp, 1)
    assert len(rows) == 1
    r = rows[0]
    assert r["start_speed"] is None
    assert r["spin_rate"] is None
    assert r["break_vertical_induced"] is None
    assert r["plate_x"] is None
