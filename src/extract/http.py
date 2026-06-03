"""Shared HTTP client for the MLB Stats API.

All three MLB endpoints (schedule, boxscore, play-by-play) go through one
``requests.Session`` so we get connection pooling/keep-alive and a single
retry+backoff policy instead of three copies of the same logic.

Scope note: backoff only helps with *rate-based* pushback — HTTP 429 and
transient 5xx. It does NOT fix a deterministic 406 from the CDN bot filter or
an IP-level block; retrying those just burns time. Those are addressed by the
browser-like headers below and, if it comes to it, an egress IP change.
"""

from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# statsapi.mlb.com sits behind a CDN that returns 406 Not Acceptable for the
# default python-requests User-Agent. Send browser-like headers so the request
# is treated like a normal client (works from curl/browser for the same reason).
MLB_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Retry only transient, rate-based failures. 406 and other 4xx are deterministic
# given our headers+IP, so they are intentionally absent — retrying wouldn't help.
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_MAX_RETRIES = 5
# Exponential backoff: ~0.5, 1, 2, 4, 8s between attempts (capped by urllib3 at
# 120s), with jitter so concurrent game fetches don't retry in lockstep.
_BACKOFF_FACTOR = 0.5
_BACKOFF_JITTER = 0.5

_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    """Return the process-wide MLB API session, building it on first use."""
    global _session
    if _session is None:
        retry = Retry(
            total=_MAX_RETRIES,
            connect=_MAX_RETRIES,
            read=_MAX_RETRIES,
            status=_MAX_RETRIES,
            status_forcelist=_RETRY_STATUSES,
            allowed_methods=frozenset({"GET"}),
            backoff_factor=_BACKOFF_FACTOR,
            backoff_jitter=_BACKOFF_JITTER,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.headers.update(MLB_API_HEADERS)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _session = session
    return _session


def get_json(
    url: str, params: Optional[dict] = None, timeout: int = 30
) -> Any:
    """GET ``url`` through the shared session and return parsed JSON.

    Raises ``requests.HTTPError`` on a non-2xx response (after retries are
    exhausted for retryable statuses).
    """
    resp = get_session().get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()
