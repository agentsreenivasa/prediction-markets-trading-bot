"""Execution engine wrapping py-clob-client for real order placement.

Handles order creation, cancellation, heartbeat management, and
interaction with the Polymarket CLOB API on Polygon.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType

if TYPE_CHECKING:
    from tracker import TradeTracker

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Minimum order size enforced by the bot (USDC)
# ---------------------------------------------------------------------------
MIN_ORDER_SIZE_USD: float = 1.0

# Maximum acceptable execution latency (ms) before skipping a trade
MAX_EXECUTION_LATENCY_MS: float = 300.0

# Heartbeat interval (seconds)
HEARTBEAT_INTERVAL_S: float = 5.0


# ---------------------------------------------------------------------------
# Shared dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Position:
    """Desired position computed by the risk / sizing layer."""

    size_usd: float
    token_id: str
    side: str  # "UP" or "DOWN"
    kelly_fraction: float
    confidence: float


@dataclass
class Order:
    """Canonical order representation returned by execution engines."""

    order_id: str
    side: str
    price: float
    size_usd: float
    token_id: str
    timestamp: float
    status: str
    order_type: str = "GTC"
    execution_latency_ms: float = 0.0


# ---------------------------------------------------------------------------
# ExecutionEngine
# ---------------------------------------------------------------------------

class ExecutionEngine:
    """Places real orders on Polymarket via *py-clob-client*."""

    def __init__(self, config: Any, tracker: TradeTracker) -> None:
        self._config = config
        self._tracker = tracker

        # Initialise the CLOB client
        self._client = ClobClient(
            host=getattr(config, "clob_host", "https://clob.polymarket.com"),
            key=config.private_key,
            chain_id=getattr(config, "chain_id", 137),
            signature_type=getattr(config, "signature_type", 2),
            funder=getattr(config, "funder_address", None),
        )
        creds = self._client.create_or_derive_api_creds()
        self._client.set_api_creds(creds)

        # Heartbeat state
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_running: bool = False

        logger.info("ExecutionEngine initialised  host=%s", self._client.host)

    # -- order placement -----------------------------------------------------

    async def place_order(
        self,
        market: dict[str, Any],
        signal: Any,
        position: Position,
    ) -> Optional[Order]:
        """Place an order on the Polymarket CLOB.

        Parameters
        ----------
        market:
            Dict with at least ``up_token``, ``down_token``,
            ``condition_id``, ``slug``.
        signal:
            Object with a ``score`` attribute (positive = UP, negative = DOWN)
            and optional ``components`` dict.
        position:
            Sizing information produced by the risk layer.

        Returns
        -------
        Order or None if the trade was skipped.
        """
        t_start = time.monotonic()

        # Determine direction
        if signal.score > 0:
            token_id = market["up_token"]
            side_label = "UP"
        elif signal.score < 0:
            token_id = market["down_token"]
            side_label = "DOWN"
        else:
            logger.debug("Signal score is zero; skipping trade")
            return None

        # Fetch current mid-price for the chosen token
        mid_raw = self._client.get_midpoint(token_id=token_id)
        price = float(mid_raw) if mid_raw else 0.50

        # Calculate number of shares: size_usd / price
        if price <= 0:
            logger.warning("Price <= 0 for token %s; skipping", token_id)
            return None

        size_shares = position.size_usd / price

        # Enforce minimum order size
        if position.size_usd < MIN_ORDER_SIZE_USD:
            logger.warning(
                "Order size $%.2f below minimum $%.2f; skipping",
                position.size_usd,
                MIN_ORDER_SIZE_USD,
            )
            return None

        # Check execution latency budget
        elapsed_ms = (time.monotonic() - t_start) * 1000.0
        if elapsed_ms > MAX_EXECUTION_LATENCY_MS:
            logger.warning(
                "Pre-order latency %.1fms exceeds %sms limit; skipping",
                elapsed_ms,
                MAX_EXECUTION_LATENCY_MS,
            )
            return None

        # Choose order type based on confidence
        use_fok = position.confidence > 0.7

        try:
            if use_fok:
                # FOK market order — amount is in USD for BUY side
                market_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=position.size_usd,
                    side="BUY",
                    order_type=OrderType.FOK,
                )
                signed = self._client.create_market_order(market_args)
                resp = self._client.post_order(signed, OrderType.FOK)
            else:
                # GTC limit order at mid price
                order_args = OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=size_shares,
                    side="BUY",
                )
                signed = self._client.create_order(order_args)
                resp = self._client.post_order(signed, OrderType.GTC)

            execution_latency_ms = (time.monotonic() - t_start) * 1000.0

            # Skip if total latency exceeded threshold
            if execution_latency_ms > MAX_EXECUTION_LATENCY_MS:
                logger.warning(
                    "Total execution latency %.1fms > %sms; order placed but flagged",
                    execution_latency_ms,
                    MAX_EXECUTION_LATENCY_MS,
                )

            order_id = resp.get("orderID", str(uuid.uuid4()))
            status = resp.get("status", "live")
            order_type_label = "FOK" if use_fok else "GTC"

            order = Order(
                order_id=order_id,
                side=side_label,
                price=price,
                size_usd=position.size_usd,
                token_id=token_id,
                timestamp=time.time(),
                status=status,
                order_type=order_type_label,
                execution_latency_ms=execution_latency_ms,
            )

            # Record in tracker
            await self._tracker.record_trade(
                {
                    "market_slug": market.get("slug", ""),
                    "condition_id": market.get("condition_id", ""),
                    "side": side_label,
                    "token_id": token_id,
                    "entry_price": price,
                    "size_usd": position.size_usd,
                    "paper_mode": False,
                    "signal_score": signal.score,
                    "signal_components": getattr(signal, "components", {}),
                    "order_id": order_id,
                    "order_type": order_type_label,
                    "execution_latency_ms": execution_latency_ms,
                    "status": status,
                }
            )

            logger.info(
                "Order placed  id=%s  side=%s  price=%.4f  size=$%.2f  type=%s  latency=%.1fms",
                order_id,
                side_label,
                price,
                position.size_usd,
                order_type_label,
                execution_latency_ms,
            )
            return order

        except Exception:
            logger.exception("Failed to place order on token %s", token_id)
            return None

    # -- order management ----------------------------------------------------

    async def cancel_order(self, order_id: str) -> None:
        """Cancel a single open order."""
        try:
            self._client.cancel(order_id=order_id)
            logger.info("Cancelled order %s", order_id)
        except Exception:
            logger.exception("Failed to cancel order %s", order_id)

    async def cancel_all(self) -> None:
        """Cancel all open orders."""
        try:
            self._client.cancel_all()
            logger.info("Cancelled all open orders")
        except Exception:
            logger.exception("Failed to cancel all orders")

    # -- balance -------------------------------------------------------------

    async def get_balance(self) -> float:
        """Return available USDC balance.

        Currently returns the configured initial balance.  Replace with
        on-chain balance query when ready.
        """
        return float(getattr(self._config, "initial_balance", 10_000.0))

    # -- heartbeat -----------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Send heartbeats every 5 seconds while orders are open.

        CRITICAL: Polymarket cancels ALL open orders if no valid
        heartbeat is received within 10 seconds (5-second buffer).
        """
        logger.info("Heartbeat loop started")
        try:
            while self._heartbeat_running:
                try:
                    self._client.get_ok()
                    logger.debug("Heartbeat sent")
                except Exception:
                    logger.exception("Heartbeat failed — orders may be cancelled")
                await asyncio.sleep(HEARTBEAT_INTERVAL_S)
        except asyncio.CancelledError:
            logger.info("Heartbeat loop cancelled")

    def start_heartbeat(self) -> None:
        """Start the background heartbeat task."""
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_running = True
            self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())
            logger.info("Heartbeat started")

    def stop_heartbeat(self) -> None:
        """Stop the background heartbeat task."""
        self._heartbeat_running = False
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            logger.info("Heartbeat stopped")
        self._heartbeat_task = None
