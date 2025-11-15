# src/core/fmp_client.py
from __future__ import annotations

import os
import time
from collections import deque
from typing import Optional, Deque

import requests
from dotenv import load_dotenv


# Load environment variables from .env if present
load_dotenv()


class RateLimiter:
    """
    Simple sliding-window rate limiter.

    limit: max number of calls
    period: in seconds (default: 60s)
    """

    def __init__(self, limit: int, period: float = 60.0) -> None:
        self.limit = limit
        self.period = period
        self._calls: Deque[float] = deque()

    def acquire(self) -> None:
        """
        Block (sleep) if we've already made `limit` calls within the last `period` seconds.
        """
        now = time.time()

        # Drop calls outside the current window
        window_start = now - self.period
        while self._calls and self._calls[0] <= window_start:
            self._calls.popleft()

        if len(self._calls) >= self.limit:
            # Oldest call in the window
            earliest = self._calls[0]
            sleep_for = earliest + self.period - now
            if sleep_for > 0:
                time.sleep(sleep_for)

            # Recompute after sleeping
            now = time.time()
            window_start = now - self.period
            while self._calls and self._calls[0] <= window_start:
                self._calls.popleft()

        # Record this call
        self._calls.append(now)


class FMPClient:
    """
    Thin HTTP client for Financial Modeling Prep.

    - Handles API key + base URL
    - Applies a sliding-window rate limiter
    - Exposes a few domain-specific helpers for this project
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        rate_limiter: Optional[RateLimiter] = None,
        timeout: float = 30.0,
    ) -> None:
        # API key resolution
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "FMP_API_KEY is not set and no api_key was provided to FMPClient."
            )

        # Base URL (allow override in env for flexibility)
        self.base_url = base_url or os.getenv(
            "FMP_BASE_URL", "https://financialmodelingprep.com"
        )

        self.timeout = timeout

        # Rate limiter: default to 750/min if nothing in env
        env_limit = os.getenv("FMP_RATE_LIMIT_PER_MINUTE")
        if rate_limiter is not None:
            self.rate_limiter = rate_limiter
        else:
            limit = int(env_limit) if env_limit is not None else 750
            self.rate_limiter = RateLimiter(limit=limit, period=60.0)

        # Requests session (monkeypatched in tests)
        self.session = requests.Session()

    # ------------------------------------------------------------------
    # Low-level request helper
    # ------------------------------------------------------------------
    def _get(self, path: str, params: Optional[dict] = None):
        """
        Internal GET helper.

        - Applies rate limiter
        - Injects apiKey
        - Raises on non-2xx
        - Returns JSON-decoded body
        """
        self.rate_limiter.acquire()

        if params is None:
            params = {}
        else:
            params = dict(params)

        # FMP expects `apikey` param
        params.setdefault("apikey", self.api_key)

        url = self.base_url.rstrip("/") + path

        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Domain-specific methods used in the project
    # ------------------------------------------------------------------
    def get_sp500_constituents(self):
        """
        Current S&P 500 universe.

        Uses the stable constituent endpoint:
          /stable/sp500-constituent
        """
        return self._get("/stable/sp500-constituent")

    def get_historical_prices_eod(self, symbol: str):
        """
        Full end-of-day historical prices for a single symbol.

        We use the stable EOD endpoint:
          /stable/historical-price-eod/full?symbol=XYZ

        NOTE: FMP currently returns a *list* of bars here, not a dict,
        which is why ingest_prices() has normalization logic that can
        handle either list or {"historical": [...]}. Do not "fix" that
        there without also updating the ingest.
        """
        params = {"symbol": symbol}
        return self._get("/stable/historical-price-eod/full", params=params)

    def get_stock_news(self, symbol: str, limit: int = 50):
        """
        Recent news for a single symbol.

        Uses the stock news endpoint:
          /stable/news/stock?symbols=XYZ&limit=N

        This typically only goes back a few days historically.
        """
        params = {"symbols": symbol, "limit": limit}
        return self._get("/stable/news/stock", params=params)

    # You can add more helpers here later as needed, e.g.:
    # - get_earnings_calendar(...)
    # - get_company_profile(...)
    # - get_quote_short(...)
