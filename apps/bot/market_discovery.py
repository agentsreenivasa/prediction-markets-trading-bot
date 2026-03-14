"""
Deterministic BTC 5m/15m market discovery via slug generation and Gamma API lookup.

Generates predictable slugs based on Unix timestamp rounding, then queries the
Polymarket Gamma API to retrieve condition IDs and token IDs for the current market.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# Supported intervals in minutes and their corresponding seconds
SUPPORTED_INTERVALS: dict[int, int] = {
    5: 300,
    15: 900,
}


@dataclass
class Market:
    """Represents a discovered Polymarket BTC Up/Down market."""

    condition_id: str
    up_token_id: str
    down_token_id: str
    slug: str
    start_ts: int
    end_ts: int
    event_id: str = ""
    market_id: str = ""

    @property
    def interval_seconds(self) -> int:
        return self.end_ts - self.start_ts

    @property
    def seconds_remaining(self) -> float:
        return max(0.0, self.end_ts - time.time())

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.end_ts


@dataclass
class _CacheEntry:
    """Internal cache entry for a discovered market."""

    market: Market
    fetched_at: float
    slug: str


class MarketDiscovery:
    """
    Discovers current BTC Up/Down prediction markets on Polymarket.

    Uses deterministic slug generation (btc-updown-{interval}m-{unix_ts_rounded})
    combined with the Gamma API to look up market details including condition IDs
    and token IDs.

    Usage:
        discovery = MarketDiscovery()
        async with discovery:
            market = await discovery.get_current_market(interval_minutes=5)
            if market:
                print(market.up_token_id, market.down_token_id)
    """

    def __init__(self, session: Optional[aiohttp.ClientSession] = None) -> None:
        self._external_session = session is not None
        self._session: Optional[aiohttp.ClientSession] = session
        self._cache: dict[int, _CacheEntry] = {}  # keyed by interval_minutes

    # ------------------------------------------------------------------
    # Context manager for owning the aiohttp session lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> MarketDiscovery:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        if not self._external_session and self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Slug generation
    # ------------------------------------------------------------------

    @staticmethod
    def generate_slug(interval_minutes: int, utc_now: Optional[float] = None) -> str:
        """
        Build a deterministic slug for the current market window.

        The timestamp is rounded *down* to the nearest interval boundary so that
        the slug matches the market that is currently active.

        Args:
            interval_minutes: 5 or 15.
            utc_now: Override current time (unix seconds) for testing.

        Returns:
            Slug string, e.g. ``btc-updown-5m-1710000000``.

        Raises:
            ValueError: If ``interval_minutes`` is not 5 or 15.
        """
        if interval_minutes not in SUPPORTED_INTERVALS:
            raise ValueError(
                f"Unsupported interval {interval_minutes}m. "
                f"Supported: {sorted(SUPPORTED_INTERVALS)}"
            )
        now = utc_now if utc_now is not None else time.time()
        interval_secs = SUPPORTED_INTERVALS[interval_minutes]
        rounded_ts = math.floor(now / interval_secs) * interval_secs
        return f"btc-updown-{interval_minutes}m-{rounded_ts}"

    @staticmethod
    def _ts_from_slug(slug: str) -> int:
        """Extract the unix timestamp from a slug string."""
        return int(slug.rsplit("-", maxsplit=1)[1])

    # ------------------------------------------------------------------
    # Gamma API lookup
    # ------------------------------------------------------------------

    async def _fetch_event(self, slug: str) -> Optional[dict]:
        """
        Query the Gamma API for an event matching *slug*.

        Returns the raw event dict or ``None`` if no match is found.
        """
        if self._session is None:
            raise RuntimeError(
                "No active aiohttp session. Use `async with MarketDiscovery()` "
                "or pass an existing session."
            )

        url = f"{GAMMA_API_BASE}/events"
        params = {"slug": slug}

        try:
            async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                events = await resp.json()
                if events and isinstance(events, list) and len(events) > 0:
                    return events[0]
                logger.debug("No event found for slug=%s", slug)
                return None
        except aiohttp.ClientError as exc:
            logger.warning("Gamma API request failed for slug=%s: %s", slug, exc)
            return None

    @staticmethod
    def _parse_market(event: dict, slug: str, interval_secs: int) -> Optional[Market]:
        """
        Extract a ``Market`` from a raw Gamma API event response.

        Returns ``None`` if the event lacks the expected market structure.
        """
        markets = event.get("markets")
        if not markets or not isinstance(markets, list):
            logger.warning("Event for slug=%s has no markets array", slug)
            return None

        mkt = markets[0]
        clob_ids = mkt.get("clobTokenIds", [])
        if len(clob_ids) < 2:
            logger.warning(
                "Event for slug=%s has fewer than 2 clobTokenIds: %s", slug, clob_ids
            )
            return None

        start_ts = int(slug.rsplit("-", maxsplit=1)[1])
        end_ts = start_ts + interval_secs

        return Market(
            condition_id=mkt.get("conditionId", ""),
            up_token_id=clob_ids[0],
            down_token_id=clob_ids[1],
            slug=slug,
            start_ts=start_ts,
            end_ts=end_ts,
            event_id=str(event.get("id", "")),
            market_id=str(mkt.get("id", "")),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_current_market(
        self, interval_minutes: int = 5, *, force_refresh: bool = False
    ) -> Optional[Market]:
        """
        Return the currently-active BTC Up/Down market for the given interval.

        Results are cached per interval so that repeated calls within the same
        market window do not hit the Gamma API again.

        Args:
            interval_minutes: 5 or 15.
            force_refresh: Bypass cache and re-query the API.

        Returns:
            A ``Market`` dataclass with all token IDs and timing info,
            or ``None`` if the market cannot be found.
        """
        slug = self.generate_slug(interval_minutes)
        interval_secs = SUPPORTED_INTERVALS[interval_minutes]

        # Check cache
        if not force_refresh:
            cached = self._cache.get(interval_minutes)
            if cached is not None and cached.slug == slug:
                logger.debug("Cache hit for %s", slug)
                return cached.market

        logger.info("Fetching market for slug=%s", slug)
        event = await self._fetch_event(slug)
        if event is None:
            return None

        market = self._parse_market(event, slug, interval_secs)
        if market is None:
            return None

        # Store in cache
        self._cache[interval_minutes] = _CacheEntry(
            market=market, fetched_at=time.time(), slug=slug
        )

        logger.info(
            "Discovered market slug=%s condition=%s up=%s down=%s remaining=%.1fs",
            market.slug,
            market.condition_id,
            market.up_token_id[:12] + "...",
            market.down_token_id[:12] + "...",
            market.seconds_remaining,
        )
        return market

    async def get_next_market(
        self, interval_minutes: int = 5
    ) -> Optional[Market]:
        """
        Pre-fetch the *next* market window (the one that hasn't started yet).

        Useful for preparing subscriptions before the current window expires.
        """
        if interval_minutes not in SUPPORTED_INTERVALS:
            raise ValueError(f"Unsupported interval: {interval_minutes}m")

        interval_secs = SUPPORTED_INTERVALS[interval_minutes]
        # Compute the start of the next window
        next_ts = math.floor(time.time() / interval_secs) * interval_secs + interval_secs
        slug = f"btc-updown-{interval_minutes}m-{next_ts}"

        logger.info("Pre-fetching next market slug=%s", slug)
        event = await self._fetch_event(slug)
        if event is None:
            return None

        return self._parse_market(event, slug, interval_secs)

    def invalidate_cache(self, interval_minutes: Optional[int] = None) -> None:
        """Clear cached market data. Pass ``None`` to clear all intervals."""
        if interval_minutes is None:
            self._cache.clear()
        else:
            self._cache.pop(interval_minutes, None)
