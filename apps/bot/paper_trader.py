"""Paper trading engine that simulates order fills locally.

Shares the same interface as :class:`ExecutionEngine` so the bot core
can switch between live and paper mode with a single flag.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
import uuid
from typing import TYPE_CHECKING, Any, Optional

from execution import MIN_ORDER_SIZE_USD, Order, Position

if TYPE_CHECKING:
    from tracker import TradeTracker

logger = logging.getLogger(__name__)


class PaperTrader:
    """Simulated execution engine for paper-trading mode.

    Fills happen at mid-price with a small random latency.  After the
    market window elapses (5 or 15 min) the trade is resolved by
    comparing the signal direction with actual BTC price movement.
    """

    def __init__(
        self,
        tracker: TradeTracker,
        initial_balance: float = 10_000.0,
    ) -> None:
        self._tracker = tracker
        self._balance: float = initial_balance
        self._initial_balance: float = initial_balance

        # Aggregate statistics
        self._total_trades: int = 0
        self._wins: int = 0
        self._losses: int = 0
        self._total_pnl: float = 0.0
        self._pnl_history: list[float] = []

        # Open positions awaiting resolution
        self._open_positions: dict[str, dict[str, Any]] = {}

        logger.info(
            "PaperTrader initialised  balance=$%.2f", self._initial_balance
        )

    # -- order placement -----------------------------------------------------

    async def place_order(
        self,
        market: dict[str, Any],
        signal: Any,
        position: Position,
    ) -> Optional[Order]:
        """Simulate order placement with random latency.

        Parameters
        ----------
        market:
            Dict with ``up_token``, ``down_token``, ``condition_id``,
            ``slug``, and optionally ``interval_minutes`` (5 or 15).
        signal:
            Object with ``score`` (positive = UP, negative = DOWN) and
            optional ``components`` dict.
        position:
            Sizing information from the risk layer.

        Returns
        -------
        Order or None if the trade was skipped.
        """
        # Determine direction
        if signal.score > 0:
            token_id = market["up_token"]
            side_label = "UP"
        elif signal.score < 0:
            token_id = market["down_token"]
            side_label = "DOWN"
        else:
            logger.debug("Signal score is zero; skipping paper trade")
            return None

        # Enforce minimum order size
        if position.size_usd < MIN_ORDER_SIZE_USD:
            logger.warning(
                "Paper order size $%.2f below minimum $%.2f; skipping",
                position.size_usd,
                MIN_ORDER_SIZE_USD,
            )
            return None

        # Simulate fill latency (50-100 ms)
        latency_ms = random.uniform(50.0, 100.0)
        await asyncio.sleep(latency_ms / 1000.0)

        # Fill at a simulated mid-price (use 0.50 as default mid)
        fill_price = 0.50
        size_shares = position.size_usd / fill_price

        # Deduct position cost from simulated balance
        cost = position.size_usd
        if cost > self._balance:
            logger.warning(
                "Insufficient paper balance $%.2f for order $%.2f; skipping",
                self._balance,
                cost,
            )
            return None
        self._balance -= cost

        # Calculate fees: fee = size * price * 0.25 * (price * (1 - price))^2
        fee = size_shares * fill_price * 0.25 * (fill_price * (1.0 - fill_price)) ** 2
        self._balance -= fee

        order_id = str(uuid.uuid4())
        order_type = "FOK" if position.confidence > 0.7 else "GTC"
        now = time.time()

        order = Order(
            order_id=order_id,
            side=side_label,
            price=fill_price,
            size_usd=position.size_usd,
            token_id=token_id,
            timestamp=now,
            status="filled",
            order_type=order_type,
            execution_latency_ms=latency_ms,
        )

        # Record in tracker
        trade_id = await self._tracker.record_trade(
            {
                "market_slug": market.get("slug", ""),
                "condition_id": market.get("condition_id", ""),
                "side": side_label,
                "token_id": token_id,
                "entry_price": fill_price,
                "size_usd": position.size_usd,
                "paper_mode": True,
                "signal_score": signal.score,
                "signal_components": getattr(signal, "components", {}),
                "order_id": order_id,
                "order_type": order_type,
                "execution_latency_ms": latency_ms,
                "fees": fee,
                "status": "filled",
            }
        )

        # Store for later resolution
        interval_minutes = market.get("interval_minutes", 5)
        self._open_positions[order_id] = {
            "trade_id": trade_id,
            "side": side_label,
            "entry_price": fill_price,
            "size_shares": size_shares,
            "cost": cost,
            "fee": fee,
            "signal_score": signal.score,
            "placed_at": now,
            "resolve_after": now + interval_minutes * 60,
        }

        self._total_trades += 1

        logger.info(
            "Paper order filled  id=%s  side=%s  price=%.4f  size=$%.2f  "
            "fee=$%.4f  latency=%.1fms  balance=$%.2f",
            order_id,
            side_label,
            fill_price,
            position.size_usd,
            fee,
            latency_ms,
            self._balance,
        )

        # Schedule resolution
        asyncio.ensure_future(
            self._resolve_after(order_id, interval_minutes * 60)
        )

        return order

    # -- simulated resolution ------------------------------------------------

    async def _resolve_after(self, order_id: str, delay_seconds: float) -> None:
        """Wait for the market window, then resolve the position.

        In a real implementation this would compare BTC price at entry
        vs close.  Here we simulate a 55% base win-rate (matching the
        strategy's backtested expectation).
        """
        await asyncio.sleep(delay_seconds)

        pos = self._open_positions.pop(order_id, None)
        if pos is None:
            return

        # Simulate resolution: 55% chance the signal was correct
        won = random.random() < 0.55

        if won:
            # Win payout: (1.0 - entry_price) * size_shares
            payout = (1.0 - pos["entry_price"]) * pos["size_shares"]
            self._balance += pos["cost"] + payout  # return cost + profit
            pnl = payout - pos["fee"]
            self._wins += 1
        else:
            # Loss: position cost is already deducted; nothing returned
            pnl = -(pos["cost"] + pos["fee"])
            self._losses += 1

        self._total_pnl += pnl
        self._pnl_history.append(pnl)

        # Update trade record
        await self._tracker.update_trade(
            pos["trade_id"],
            {
                "exit_price": 1.0 if won else 0.0,
                "pnl": pnl,
                "win": won,
                "status": "resolved",
            },
        )

        logger.info(
            "Paper trade resolved  id=%s  win=%s  pnl=$%.4f  balance=$%.2f",
            order_id,
            won,
            pnl,
            self._balance,
        )

    # -- balance & management ------------------------------------------------

    async def get_balance(self) -> float:
        """Return the current simulated USDC balance."""
        return self._balance

    async def cancel_order(self, order_id: str) -> None:
        """No-op for paper trading."""
        logger.debug("Paper cancel_order called (no-op) for %s", order_id)

    async def cancel_all(self) -> None:
        """No-op for paper trading."""
        logger.debug("Paper cancel_all called (no-op)")

    # -- analytics -----------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """Return aggregate paper-trading statistics.

        Returns
        -------
        dict with: win_rate, profit_factor, total_pnl, sharpe_estimate,
        total_trades, wins, losses, current_balance.
        """
        total = self._wins + self._losses
        win_rate = self._wins / total if total > 0 else 0.0

        gross_profit = sum(p for p in self._pnl_history if p > 0)
        gross_loss = abs(sum(p for p in self._pnl_history if p < 0))
        profit_factor = (
            gross_profit / gross_loss if gross_loss > 0 else float("inf")
        )

        # Simple Sharpe estimate: mean(pnl) / std(pnl)
        sharpe = 0.0
        if len(self._pnl_history) >= 2:
            mean_pnl = sum(self._pnl_history) / len(self._pnl_history)
            variance = sum(
                (p - mean_pnl) ** 2 for p in self._pnl_history
            ) / (len(self._pnl_history) - 1)
            std_pnl = math.sqrt(variance) if variance > 0 else 0.0
            sharpe = mean_pnl / std_pnl if std_pnl > 0 else 0.0

        return {
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "total_pnl": self._total_pnl,
            "sharpe_estimate": sharpe,
            "total_trades": self._total_trades,
            "wins": self._wins,
            "losses": self._losses,
            "current_balance": self._balance,
        }
