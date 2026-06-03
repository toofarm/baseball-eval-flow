"""Tests for the shared MLB HTTP client (src/extract/http.py).

Guards the two things the rest of extract relies on: that get_json sends the
browser headers + raises on error, and that the retry policy retries only
transient/rate-based statuses (not the deterministic 406 CDN block).
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.extract import http


def test_get_json_uses_session_and_returns_payload():
    session = MagicMock()
    resp = MagicMock()
    resp.json.return_value = {"ok": True}
    resp.raise_for_status.return_value = None
    session.get.return_value = resp

    with patch.object(http, "get_session", return_value=session):
        out = http.get_json("https://example.test/x", params={"a": 1}, timeout=7)

    assert out == {"ok": True}
    session.get.assert_called_once_with(
        "https://example.test/x", params={"a": 1}, timeout=7
    )


def test_get_json_raises_on_http_error():
    session = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.side_effect = requests.HTTPError("406")
    session.get.return_value = resp

    with patch.object(http, "get_session", return_value=session):
        with pytest.raises(requests.HTTPError):
            http.get_json("https://example.test/x")


def test_retry_policy_targets_only_rate_based_statuses():
    retry = http.get_session().get_adapter("https://statsapi.mlb.com").max_retries
    assert set(retry.status_forcelist) == {429, 500, 502, 503, 504}
    # 406 is a deterministic CDN block — retrying it would only waste time.
    assert 406 not in retry.status_forcelist
    assert retry.total == 5
    assert set(retry.allowed_methods) == {"GET"}


def test_session_is_a_reused_singleton_with_browser_headers():
    s1 = http.get_session()
    s2 = http.get_session()
    assert s1 is s2
    assert "Mozilla" in s1.headers["User-Agent"]
    assert s1.headers["Accept"] == "application/json"
