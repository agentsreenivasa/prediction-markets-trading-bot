"""
Async WebSocket data feed manager for real-time BTC price and Polymarket orderbook data.

Manages three concurrent feeds:
  1. Binance BTCUSDT spot trades
  2. Polymarket RTDS Chainlink oracle prices
  3. Polymarket Market channel (orderbook / best bid-ask)

All data is stored in asyncio-safe structures and exposed via read-only properties.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Optional

import aiohttp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# WebSocket endpoints
# ---------------------------------------------------------------------------
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ---------------------------------------------------------------------------
# Timing constants
# ---------------------------------------------------------------------------
RTDS_PING_INTERVAL_S = 5.0
MARKET_PING_INTERVAL_S = 10.0
RECONNECT_BASE_DELAY_S = 1.0
RECONNECT_MAX_DELAY_S = 60.0
PRICE_HISTORY_MAXLEN = 100


@dataclass
class PriceTick:
    """A single price observation with timestamp."""

    price: float
    timestamp: float  # unix seconds
    volume: float = 0.0


@dataclass
class OrderbookSnapshot:
    """Lightweight snapshot of the best bid/ask for a token."""

    best_bid: Optional[float] = None
    best_bid_size: Optional[float] = None
    best_ask: Optional[float] = None
    best_ask_size: Optional[float] = None
    timestamp: float = 0.0

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2.0
        return None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None


class DataFeedManager:
    """
    Manages concurrent WebSocket connections to Binance and Polymarket.

    Usage::

        feeds = DataFeedManager()
        feeds.subscribe_market(up_token_id="abc123", down_token_id="def456")

        async with feeds:
            await feeds.run()  # blocks, runs all feeds concurrently

    Read latest data via properties:
        feeds.spot_price      -> latest Binance BTC/USDT spot price
        feeds.oracle_price    -> latest Chainlink BTC/USD oracle price
        feeds.best_bid        -> best bid for the UP token on Polymarket
        feeds.best_ask        -> best ask for the UP token on Polymarket
        feeds.orderbook_imbalance -> bid-side imbalance ratio
    """

    def __init__(self) -> None:
        # Latest scalar values (asyncio-safe -- only mutated inside the event loop)
        self._spot_price: Optional[float] = None
        self._oracle_price: Optional[float] = None
        self._spot_ts: float = 0.0
        self._oracle_ts: float = 0.0

        # Orderbook state keyed by token_id
        self._orderbooks: dict[str, OrderbookSnapshot] = {}

        # Price history for indicator calculation
        self._spot_history: Deque[PriceTick] = deque(maxlen=PRICE_HISTORY_MAXLEN)
        self._oracle_history: Deque[PriceTick] = deque(maxlen=PRICE_HISTORY_MAXLEN)
        self._trade_history: Deque[PriceTick] = deque(maxlen=PRICE_HISTORY_MAXLEN)

        # Token subscriptions
        self._up_token_id: Optional[str] = None
        self._down_token_id: Optional[str] = None
        self._pending_subscription: bool = False

        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        self._running: bool = False
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> DataFeedManager:
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def stop(self) -> None:
        """Cancel all feed tasks and close the HTTP session."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def spot_price(self) -> Optional[float]:
        """Latest Binance BTC/USDT spot price."""
        return self._spot_price

    @property
    def oracle_price(self) -> Optional[float]:
        """Latest Chainlink BTC/USD oracle price from RTDS."""
        return self._oracle_price

    @property
    def best_bid(self) -> Optional[float]:
        """Best bid for the UP token on the Polymarket CLOB."""
        if self._up_token_id and self._up_token_id in self._orderbooks:
            return self._orderbooks[self._up_token_id].best_bid
        return None

    @property
    def best_ask(self) -> Optional[float]:
        """Best ask for the UP token on the Polymarket CLOB."""
        if self._up_token_id and self._up_token_id in self._orderbooks:
            return self._orderbooks[self._up_token_id].best_ask
        return None

    @property
    def orderbook_imbalance(self) -> Optional[float]:
        """
        Bid-side imbalance ratio for the UP token.

        Returns a value between -1.0 (all ask pressure) and +1.0 (all bid pressure).
        ``None`` if orderbook data is unavailable.
        """
        if self._up_token_id is None:
            return None
        ob = self._orderbooks.get(self._up_token_id)
        if ob is None or ob.best_bid_size is None or ob.best_ask_size is None:
            return None
        total = ob.best_bid_size + ob.best_ask_size
        if total == 0:
            return 0.0
        return (ob.best_bid_size - ob.best_ask_size) / total

    @property
    def spot_history(self) -> list[PriceTick]:
        """Copy of recent spot price ticks."""
        return list(self._spot_history)

    @property
    def oracle_history(self) -> list[PriceTick]:
        """Copy of recent oracle price ticks."""
        return list(self._oracle_history)

    @property
    def trade_history(self) -> list[PriceTick]:
        """Copy of recent Polymarket trade ticks."""
        return list(self._trade_history)

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------

    def subscribe_market(self, up_token_id: str, down_token_id: str) -> None:
        """
        Set token IDs for the current market.

        If feeds are already running the Market channel will dynamically
        subscribe to the new tokens on the next heartbeat cycle.
        """
        old_up = self._up_token_id
        old_down = self._down_token_id
        self._up_token_id = up_token_id
        self._down_token_id = down_token_id

        # Ensure orderbook entries exist
        self._orderbooks.setdefault(up_token_id, OrderbookSnapshot())
        self._orderbooks.setdefault(down_token_id, OrderbookSnapshot())

        if old_up != up_token_id or old_down != down_token_id:
            self._pending_subscription = True
            logger.info(
                "Market subscription updated: up=%s down=%s",
                up_token_id[:16] + "..." if len(up_token_id) > 16 else up_token_id,
                down_token_id[:16] + "..." if len(down_token_id) > 16 else down_token_id,
            )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Start all WebSocket feeds concurrently.

        This coroutine runs indefinitely until cancelled or ``stop()`` is called.
        Each feed auto-reconnects with exponential backoff on disconnect.
        """
        if self._session is None:
            self._session = aiohttp.ClientSession()

        self._running = True
        self._tasks = [
            asyncio.create_task(self._run_binance_feed(), name="binance_feed"),
            asyncio.create_task(self._run_rtds_feed(), name="rtds_feed"),
            asyncio.create_task(self._run_market_feed(), name="market_feed"),
        ]
        logger.info("DataFeedManager started with %d feeds", len(self._tasks))

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("DataFeedManager cancelled")

    # ------------------------------------------------------------------
    # Feed 1: Binance BTCUSDT spot trades
    # ------------------------------------------------------------------

    async def _run_binance_feed(self) -> None:
        """Connect to Binance trade stream with auto-reconnect."""
        delay = RECONNECT_BASE_DELAY_S
        while self._running:
            try:
                logger.info("Connecting to Binance WebSocket...")
                assert self._session is not None
                async with self._session.ws_connect(
                    BINANCE_WS_URL, heartbeat=30.0, timeout=15.0
                ) as ws:
                    logger.info("Binance WebSocket connected")
                    delay = RECONNECT_BASE_DELAY_S  # reset backoff on success

                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._handle_binance_message(msg.data)
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            logger.warning("Binance WS closed/error: %s", msg.data)
                            break

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning("Binance feed error: %s", exc)
            except asyncio.CancelledError:
                return

            if self._running:
                logger.info("Reconnecting Binance feed in %.1fs...", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY_S)

    def _handle_binance_message(self, raw: str) -> None:
        """Parse a Binance trade message and update spot price."""
        try:
            data = json.loads(raw)
            price = float(data.get("p", 0))
            qty = float(data.get("q", 0))
            ts = data.get("T", 0) / 1000.0  # ms -> s
            if price > 0:
                self._spot_price = price
                self._spot_ts = ts or time.time()
                self._spot_history.append(
                    PriceTick(price=price, timestamp=self._spot_ts, volume=qty)
                )
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.debug("Failed to parse Binance message: %s", exc)

    # ------------------------------------------------------------------
    # Feed 2: Polymarket RTDS -- Chainlink BTC/USD
    # ------------------------------------------------------------------

    async def _run_rtds_feed(self) -> None:
        """Connect to Polymarket RTDS for Chainlink oracle prices."""
        delay = RECONNECT_BASE_DELAY_S
        while self._running:
            try:
                logger.info("Connecting to RTDS WebSocket...")
                assert self._session is not None
                async with self._session.ws_connect(
                    RTDS_WS_URL, timeout=15.0
                ) as ws:
                    logger.info("RTDS WebSocket connected")
                    delay = RECONNECT_BASE_DELAY_S

                    # Subscribe to Chainlink BTC/USD
                    subscribe_msg = json.dumps(
                        {
                            "action": "subscribe",
                            "subscriptions": [
                                {
                                    "topic": "crypto_prices_chainlink",
                                    "type": "*",
                                    "filters": json.dumps({"symbol": "btc/usd"}),
                                }
                            ],
                        }
                    )
                    await ws.send_str(subscribe_msg)
                    logger.info("RTDS subscribed to crypto_prices_chainlink btc/usd")

                    # Concurrent heartbeat + receive loop
                    recv_task = asyncio.create_task(
                        self._rtds_recv_loop(ws), name="rtds_recv"
                    )
                    ping_task = asyncio.create_task(
                        self._rtds_ping_loop(ws), name="rtds_ping"
                    )

                    done, pending = await asyncio.wait(
                        [recv_task, ping_task], return_when=asyncio.FIRST_COMPLETED
                    )
                    for t in pending:
                        t.cancel()

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning("RTDS feed error: %s", exc)
            except asyncio.CancelledError:
                return

            if self._running:
                logger.info("Reconnecting RTDS feed in %.1fs...", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY_S)

    async def _rtds_recv_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if not self._running:
                break
            if msg.type == aiohttp.WSMsgType.TEXT:
                self._handle_rtds_message(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                logger.warning("RTDS WS closed/error: %s", msg.data)
                break

    async def _rtds_ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while self._running:
            try:
                await ws.send_str("PING")
            except (aiohttp.ClientError, ConnectionError):
                break
            await asyncio.sleep(RTDS_PING_INTERVAL_S)

    def _handle_rtds_message(self, raw: str) -> None:
        """Parse an RTDS Chainlink price message."""
        if raw == "PONG":
            return
        try:
            data = json.loads(raw)
            topic = data.get("topic", "")
            if topic == "crypto_prices_chainlink":
                payload = data.get("payload", {})
                value = payload.get("value")
                if value is not None:
                    price = float(value)
                    ts = payload.get("timestamp", 0)
                    # Timestamp may be milliseconds
                    if ts > 1e12:
                        ts = ts / 1000.0
                    self._oracle_price = price
                    self._oracle_ts = ts or time.time()
                    self._oracle_history.append(
                        PriceTick(price=price, timestamp=self._oracle_ts)
                    )
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.debug("Failed to parse RTDS message: %s", exc)

    # ------------------------------------------------------------------
    # Feed 3: Polymarket Market channel (orderbook updates)
    # ------------------------------------------------------------------

    async def _run_market_feed(self) -> None:
        """Connect to Polymarket Market WebSocket for orderbook data."""
        delay = RECONNECT_BASE_DELAY_S
        while self._running:
            try:
                logger.info("Connecting to Market WebSocket...")
                assert self._session is not None
                async with self._session.ws_connect(
                    MARKET_WS_URL, timeout=15.0
                ) as ws:
                    logger.info("Market WebSocket connected")
                    delay = RECONNECT_BASE_DELAY_S

                    # Send initial subscription if tokens are set
                    await self._send_market_subscription(ws)

                    recv_task = asyncio.create_task(
                        self._market_recv_loop(ws), name="market_recv"
                    )
                    ping_task = asyncio.create_task(
                        self._market_ping_loop(ws), name="market_ping"
                    )

                    done, pending = await asyncio.wait(
                        [recv_task, ping_task], return_when=asyncio.FIRST_COMPLETED
                    )
                    for t in pending:
                        t.cancel()

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning("Market feed error: %s", exc)
            except asyncio.CancelledError:
                return

            if self._running:
                logger.info("Reconnecting Market feed in %.1fs...", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY_S)

    async def _send_market_subscription(
        self, ws: aiohttp.ClientWebSocketResponse
    ) -> None:
        """Send a subscription message for current token IDs."""
        token_ids = self._get_active_token_ids()
        if not token_ids:
            logger.debug("No token IDs to subscribe to on Market channel")
            return

        msg = json.dumps(
            {
                "assets_ids": token_ids,
                "type": "market",
                "custom_feature_enabled": True,
            }
        )
        await ws.send_str(msg)
        self._pending_subscription = False
        logger.info("Market channel subscribed to %d tokens", len(token_ids))

    def _get_active_token_ids(self) -> list[str]:
        ids: list[str] = []
        if self._up_token_id:
            ids.append(self._up_token_id)
        if self._down_token_id:
            ids.append(self._down_token_id)
        return ids

    async def _market_recv_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if not self._running:
                break
            if msg.type == aiohttp.WSMsgType.TEXT:
                self._handle_market_message(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                logger.warning("Market WS closed/error: %s", msg.data)
                break

    async def _market_ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while self._running:
            try:
                # Handle dynamic re-subscription on token change
                if self._pending_subscription:
                    await self._send_market_subscription(ws)
                await ws.send_str("PING")
            except (aiohttp.ClientError, ConnectionError):
                break
            await asyncio.sleep(MARKET_PING_INTERVAL_S)

    def _handle_market_message(self, raw: str) -> None:
        """Parse a Polymarket Market channel message."""
        if raw == "PONG":
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        if isinstance(data, list):
            for item in data:
                self._process_market_event(item)
        elif isinstance(data, dict):
            self._process_market_event(data)

    def _process_market_event(self, event: dict[str, Any]) -> None:
        """Route a single market event to the appropriate handler."""
        event_type = event.get("event_type", event.get("type", ""))
        asset_id = event.get("asset_id", "")
        now = time.time()

        if event_type == "book":
            self._handle_book_snapshot(event, asset_id, now)
        elif event_type == "price_change":
            self._handle_price_change(event, asset_id, now)
        elif event_type == "best_bid_ask":
            self._handle_best_bid_ask(event, asset_id, now)
        elif event_type == "last_trade_price":
            self._handle_last_trade(event, asset_id, now)

    def _handle_book_snapshot(
        self, event: dict[str, Any], asset_id: str, now: float
    ) -> None:
        """Full orderbook snapshot received on subscribe or after trades."""
        bids = event.get("bids", [])
        asks = event.get("asks", [])
        ob = self._orderbooks.setdefault(asset_id, OrderbookSnapshot())
        if bids:
            ob.best_bid = float(bids[0].get("price", 0))
            ob.best_bid_size = float(bids[0].get("size", 0))
        if asks:
            ob.best_ask = float(asks[0].get("price", 0))
            ob.best_ask_size = float(asks[0].get("size", 0))
        ob.timestamp = now

    def _handle_price_change(
        self, event: dict[str, Any], asset_id: str, now: float
    ) -> None:
        """Incremental price level update."""
        changes = event.get("changes", [])
        ob = self._orderbooks.setdefault(asset_id, OrderbookSnapshot())
        for change in changes:
            side = change.get("side", "")
            price = float(change.get("price", 0))
            size = float(change.get("size", 0))
            if side == "BUY":
                if ob.best_bid is None or price >= ob.best_bid:
                    ob.best_bid = price
                    ob.best_bid_size = size
            elif side == "SELL":
                if ob.best_ask is None or price <= ob.best_ask:
                    ob.best_ask = price
                    ob.best_ask_size = size
        ob.timestamp = now

    def _handle_best_bid_ask(
        self, event: dict[str, Any], asset_id: str, now: float
    ) -> None:
        """Direct best bid/ask update (requires custom_feature_enabled)."""
        ob = self._orderbooks.setdefault(asset_id, OrderbookSnapshot())
        bid = event.get("best_bid")
        ask = event.get("best_ask")
        if bid is not None:
            ob.best_bid = float(bid)
            bid_size = event.get("best_bid_size")
            if bid_size is not None:
                ob.best_bid_size = float(bid_size)
        if ask is not None:
            ob.best_ask = float(ask)
            ask_size = event.get("best_ask_size")
            if ask_size is not None:
                ob.best_ask_size = float(ask_size)
        ob.timestamp = now

    def _handle_last_trade(
        self, event: dict[str, Any], asset_id: str, now: float
    ) -> None:
        """Trade execution on the market."""
        price = event.get("price")
        size = event.get("size")
        if price is not None:
            tick = PriceTick(
                price=float(price),
                timestamp=now,
                volume=float(size) if size else 0.0,
            )
            self._trade_history.append(tick)
