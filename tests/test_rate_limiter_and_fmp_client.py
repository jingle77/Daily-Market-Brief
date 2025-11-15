# tests/test_rate_limiter_and_fmp_client.py
import time
from types import SimpleNamespace

import pytest

from core.fmp_client import RateLimiter, FMPClient


def test_rate_limiter_allows_burst_under_limit(monkeypatch):
    """
    If we call acquire() fewer times than the limit within the window,
    it should return immediately each time (no sleeping).
    """
    limiter = RateLimiter(limit=5, period=60.0)

    # Monkeypatch time.time to a fixed value to avoid flakiness
    t0 = 1_000_000.0
    times = [t0] * 5  # same timestamp for first 5 calls
    idx = 0

    def fake_time():
        nonlocal idx
        if idx < len(times):
            val = times[idx]
            idx += 1
            return val
        return times[-1]

    monkeypatch.setattr(time, "time", fake_time)

    # Should not block or raise for <= limit calls
    for _ in range(5):
        limiter.acquire()


def test_rate_limiter_rolls_window(monkeypatch):
    """
    If enough time passes, old calls should drop out of the window,
    allowing new calls.
    """
    limiter = RateLimiter(limit=2, period=10.0)

    # We'll simulate time jumping forward: two calls at t0, then a call at t0+11
    t0 = 1_000_000.0
    timestamps = [t0, t0, t0 + 11]
    idx = 0

    def fake_time():
        nonlocal idx
        val = timestamps[min(idx, len(timestamps) - 1)]
        idx += 1
        return val

    monkeypatch.setattr(time, "time", fake_time)

    # First two calls fill the bucket
    limiter.acquire()
    limiter.acquire()

    # Third call should be allowed because now time advanced by > period,
    # so the first calls are dropped.
    limiter.acquire()


def test_fmp_client_get_uses_session_and_rate_limiter(monkeypatch):
    """
    FMPClient._get should:
      - Call rate_limiter.acquire()
      - Call session.get() with proper URL & params (including apikey)
      - Return resp.json()
    We mock both the rate limiter and the session to avoid real HTTP.
    """
    calls = {"acquire": 0, "get": 0}

    class DummyLimiter:
        def acquire(self):
            calls["acquire"] += 1

    def fake_get(url, params=None, timeout=None):
        calls["get"] += 1
        assert "apikey" in params
        # Return a dummy response object with raise_for_status/json
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {"ok": True, "url": url, "params": params},
        )

    # Build a client with injected session + limiter.
    client = FMPClient(api_key="TEST_KEY", rate_limiter=DummyLimiter())
    client.session.get = fake_get  # monkeypatch session.get

    result = client._get("/stable/test-endpoint", params={"foo": "bar"})

    assert calls["acquire"] == 1
    assert calls["get"] == 1
    assert result["ok"] is True
    assert "test-endpoint" in result["url"]
    assert result["params"]["foo"] == "bar"
    assert result["params"]["apikey"] == "TEST_KEY"
