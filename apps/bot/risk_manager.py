"""
Risk management and Kelly-criterion position sizing for the Polymarket BTC bot.

Implements:
- Fractional Kelly criterion adapted for binary prediction markets
- Confidence and autocorrelation adjustments
- Multi-tier circuit breakers (daily, hourly, consecutive-loss)
- Graduated drawdown response
- Directional-exposure checks

All thresholds are sourced from :class:`Config` so they can be tuned via
environment variables without code changes.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, Sequence

if TYPE_CHECKING:
    from config import Config

logger = logging.getLogger(__name__)


# ── Data contracts ──────────────────────────────────────────────────────────


@dataclass
class Signal:
    """A directional trading signal produced by the signal generator."""

    score: float  # -1.0 (strong DOWN) to +1.0 (strong UP)
    confidence: float  # 0.0 to 1.0


@dataclass
class Position:
    """Sized position ready for execution."""

    size_usd: float
    token_id: str
    side: str  # "YES" or "NO"
    kelly_fraction: float
    confidence: float


class TradeTracker(Protocol):
    """Minimal interface the risk manager needs from the trade tracker."""

    def get_recent_results(self, n: int = 20) -> list[float]:
        """Return recent trade PnL results (positive = win, negative = loss)."""
        ...

    @property
    def daily_pnl(self) -> float:
        """Cumulative PnL since the start of the current trading day."""
        ...

    @property
    def hourly_pnl(self) -> float:
        """Cumulative PnL in the current rolling hour."""
        ...

    @property
    def consecutive_losses(self) -> int:
        """Current streak of consecutive losing trades."""
        ...

    @property
    def daily_start_bankroll(self) -> float:
        """Bankroll at the start of the current trading day."""
        ...

    @property
    def current_bankroll(self) -> float:
        """Live bankroll (starting balance + cumulative PnL)."""
        ...


# ── Risk manager ────────────────────────────────────────────────────────────


class RiskManager:
    """
    Central gatekeeper for position sizing and risk controls.

    Every prospective trade passes through :meth:`size_position` and the
    circuit-breaker / exposure checks before reaching the execution layer.
    """

    def __init__(self, config: Config, tracker: TradeTracker) -> None:
        self._cfg = config
        self._tracker = tracker

    # ── Position sizing (Kelly) ─────────────────────────────────────────

    def size_position(
        self,
        signal: Signal,
        market_price: float,
        bankroll: float,
        token_id: str,
    ) -> Position | None:
        """
        Compute a risk-adjusted position size using fractional Kelly criterion.

        Returns ``None`` when the trade should be skipped (no edge, below
        threshold, circuit breaker active, etc.).

        Kelly formula for binary prediction markets::

            p_model = (signal.score + 1) / 2
            b       = (1 - market_price) / market_price
            q       = 1 - p_model
            f*      = (b * p_model - q) / b
            f_adj   = f* * kelly_fraction * confidence * correlation_adj

        The final dollar size is capped at ``max_risk_per_trade * bankroll``
        and floored at ``min_order_size_usd``.
        """
        # ── Pre-flight checks ───────────────────────────────────────────
        if self.circuit_breaker_active():
            logger.warning("Trade rejected: circuit breaker is active")
            return None

        if signal.confidence < self._cfg.min_signal_threshold:
            logger.info(
                "Trade skipped: confidence %.3f < threshold %.3f",
                signal.confidence,
                self._cfg.min_signal_threshold,
            )
            return None

        if not (0.01 <= market_price <= 0.99):
            logger.warning(
                "Trade skipped: market_price %.4f outside tradeable range [0.01, 0.99]",
                market_price,
            )
            return None

        # ── Convert signal score to model probability ───────────────────
        # signal.score is in [-1, +1]; map to [0, 1]
        p_model = (signal.score + 1.0) / 2.0

        # Determine side — buy YES when p_model > market_price, else buy NO
        if p_model > market_price:
            side = "YES"
            effective_p = p_model
            effective_price = market_price
        elif (1.0 - p_model) > (1.0 - market_price):
            # Edge exists on the NO side
            side = "NO"
            effective_p = 1.0 - p_model
            effective_price = 1.0 - market_price
        else:
            logger.info(
                "Trade skipped: no edge (p_model=%.4f, market_price=%.4f)",
                p_model,
                market_price,
            )
            return None

        # ── Full Kelly ──────────────────────────────────────────────────
        b = (1.0 - effective_price) / effective_price  # net odds
        q = 1.0 - effective_p
        f_star = (b * effective_p - q) / b

        if f_star <= 0.0:
            logger.info(
                "Trade skipped: negative Kelly (f*=%.4f, p=%.4f, price=%.4f)",
                f_star,
                effective_p,
                effective_price,
            )
            return None

        # ── Fractional Kelly ────────────────────────────────────────────
        f_adjusted = f_star * self._cfg.kelly_fraction

        # ── Confidence scaling ──────────────────────────────────────────
        f_adjusted *= signal.confidence

        # ── Autocorrelation adjustment ──────────────────────────────────
        recent = self._tracker.get_recent_results()
        if len(recent) >= 4:
            autocorr = self._compute_autocorrelation(recent, lag=1)
            adjustment = 1.0 / (1.0 + max(0.0, autocorr))
            f_adjusted *= adjustment
            logger.debug(
                "Autocorrelation adjustment: autocorr=%.3f, multiplier=%.3f",
                autocorr,
                adjustment,
            )

        # ── Drawdown multiplier ─────────────────────────────────────────
        dd_mult = self.get_drawdown_multiplier()
        f_adjusted *= dd_mult
        if dd_mult < 1.0:
            logger.info("Drawdown multiplier applied: %.2f", dd_mult)

        # ── Hard cap at max_risk_per_trade ──────────────────────────────
        f_adjusted = min(f_adjusted, self._cfg.max_risk_per_trade)

        # ── Dollar size ─────────────────────────────────────────────────
        size_usd = bankroll * f_adjusted

        if size_usd < self._cfg.min_order_size_usd:
            logger.info(
                "Trade skipped: sized $%.2f below minimum $%.2f",
                size_usd,
                self._cfg.min_order_size_usd,
            )
            return None

        logger.info(
            "Position sized: side=%s size=$%.2f kelly=%.4f conf=%.3f dd_mult=%.2f",
            side,
            size_usd,
            f_adjusted,
            signal.confidence,
            dd_mult,
        )

        return Position(
            size_usd=round(size_usd, 2),
            token_id=token_id,
            side=side,
            kelly_fraction=round(f_adjusted, 6),
            confidence=signal.confidence,
        )

    # ── Circuit breakers ────────────────────────────────────────────────

    def circuit_breaker_active(self) -> bool:
        """
        Return ``True`` if **any** circuit breaker is currently triggered.

        Breakers checked (in order):
        1. Daily loss >= ``max_daily_loss_pct`` of daily-start bankroll
        2. Consecutive losses >= ``max_consecutive_losses``
        3. Hourly loss >= ``max_hourly_loss_pct`` of current bankroll
        4. (Exposure breaker is checked separately via :meth:`check_exposure`)
        """
        # 1. Daily loss breaker
        daily_limit = self._cfg.max_daily_loss_pct * self._tracker.daily_start_bankroll
        if self._tracker.daily_pnl <= -daily_limit:
            logger.warning(
                "CIRCUIT BREAKER: daily loss $%.2f exceeds limit $%.2f (%.1f%%)",
                abs(self._tracker.daily_pnl),
                daily_limit,
                self._cfg.max_daily_loss_pct * 100,
            )
            return True

        # 2. Consecutive loss breaker
        if self._tracker.consecutive_losses >= self._cfg.max_consecutive_losses:
            logger.warning(
                "CIRCUIT BREAKER: %d consecutive losses (limit: %d)",
                self._tracker.consecutive_losses,
                self._cfg.max_consecutive_losses,
            )
            return True

        # 3. Hourly loss breaker
        hourly_limit = self._cfg.max_hourly_loss_pct * self._tracker.current_bankroll
        if self._tracker.hourly_pnl <= -hourly_limit:
            logger.warning(
                "CIRCUIT BREAKER: hourly loss $%.2f exceeds limit $%.2f (%.1f%%)",
                abs(self._tracker.hourly_pnl),
                hourly_limit,
                self._cfg.max_hourly_loss_pct * 100,
            )
            return True

        return False

    # ── Drawdown multiplier ─────────────────────────────────────────────

    def get_drawdown_multiplier(self) -> float:
        """
        Tiered position-size multiplier based on current daily drawdown.

        Returns:
            1.0  for 0–2 % drawdown  (normal trading)
            0.5  for 2–3 % drawdown  (reduce 50 %)
            0.25 for 3–5 % drawdown  (reduce 75 %)
            0.0  for 5 %+ drawdown   (stop trading)
        """
        start = self._tracker.daily_start_bankroll
        if start <= 0.0:
            return 0.0

        daily_dd_pct = -self._tracker.daily_pnl / start  # positive when losing

        if daily_dd_pct >= 0.05:
            return 0.0
        if daily_dd_pct >= 0.03:
            return 0.25
        if daily_dd_pct >= 0.02:
            return 0.5
        return 1.0

    # ── Exposure check ──────────────────────────────────────────────────

    def check_exposure(
        self,
        current_positions: Sequence[Position],
        new_position: Position,
    ) -> bool:
        """
        Verify that adding *new_position* keeps total directional exposure
        within ``max_concurrent_exposure * bankroll``.

        Args:
            current_positions: Already-open positions.
            new_position: Proposed new position to add.

        Returns:
            ``True`` if the trade is allowed, ``False`` otherwise.
        """
        bankroll = self._tracker.current_bankroll
        if bankroll <= 0.0:
            logger.warning("Exposure check failed: bankroll is non-positive")
            return False

        total_exposure = sum(p.size_usd for p in current_positions)
        proposed_total = total_exposure + new_position.size_usd
        max_exposure_usd = self._cfg.max_concurrent_exposure * bankroll

        if proposed_total > max_exposure_usd:
            logger.warning(
                "Exposure check FAILED: proposed $%.2f > limit $%.2f (%.1f%% of $%.2f)",
                proposed_total,
                max_exposure_usd,
                self._cfg.max_concurrent_exposure * 100,
                bankroll,
            )
            return False

        logger.debug(
            "Exposure check OK: $%.2f + $%.2f = $%.2f <= $%.2f",
            total_exposure,
            new_position.size_usd,
            proposed_total,
            max_exposure_usd,
        )
        return True

    # ── Autocorrelation ─────────────────────────────────────────────────

    @staticmethod
    def _compute_autocorrelation(results: Sequence[float], lag: int = 1) -> float:
        """
        Compute first-order autocorrelation of a win/loss sequence.

        Converts raw PnL values to +1 (win) / -1 (loss) before computing
        lag-*lag* autocorrelation.

        Returns a float in [-1, 1].  Returns 0.0 for degenerate inputs
        (too few data points or zero variance).
        """
        if len(results) <= lag:
            return 0.0

        # Map to +1 / -1
        binary = [1.0 if r > 0.0 else -1.0 for r in results]

        n = len(binary)
        mean = sum(binary) / n

        # Variance
        var = sum((x - mean) ** 2 for x in binary) / n
        if var == 0.0:
            return 0.0

        # Autocovariance at given lag
        cov = sum(
            (binary[i] - mean) * (binary[i + lag] - mean)
            for i in range(n - lag)
        ) / (n - lag)

        autocorr = cov / var
        # Clamp to [-1, 1] for numerical safety
        return max(-1.0, min(1.0, autocorr))
