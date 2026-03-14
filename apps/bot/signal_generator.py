"""
Composite 8-factor signal generator for BTC prediction market trading.

Computes a directional score in [-1.0, +1.0] from eight weighted indicators
derived from real-time Binance spot, Chainlink oracle, and Polymarket
orderbook data provided by a ``DataFeedManager`` instance.

Indicator weights (from quant.md section 3.10):

    price_vs_vwap      0.20   Price position relative to VWAP
    rsi_signal          0.10   RSI(7) overbought/oversold
    macd_signal         0.10   Fast MACD (5/13/4) histogram direction
    obv_divergence      0.10   OBV divergence from price
    momentum_5m         0.20   Raw 5-minute price momentum
    vpin_signal         0.15   Order flow imbalance (VPIN proxy)
    funding_rate        0.05   Extreme funding rate signal
    regime_adjustment   0.10   Volatility regime modifier (ATR ratio)
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence

from data_feeds import DataFeedManager, PriceTick

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INDICATOR_WEIGHTS: dict[str, float] = {
    "price_vs_vwap": 0.20,
    "rsi_signal": 0.10,
    "macd_signal": 0.10,
    "obv_divergence": 0.10,
    "momentum_5m": 0.20,
    "vpin_signal": 0.15,
    "funding_rate": 0.05,
    "regime_adjustment": 0.10,
}

# RSI parameters
RSI_PERIOD = 7

# MACD parameters (fast adaptation for 5-min charts)
MACD_FAST = 5
MACD_SLOW = 13
MACD_SIGNAL = 4

# ATR / regime
ATR_PERIOD = 14
ATR_SMA_PERIOD = 50
HIGH_VOL_THRESHOLD = 1.5
LOW_VOL_THRESHOLD = 0.7

# Oracle lead detection
ORACLE_LEAD_THRESHOLD_USD = 15.0  # absolute USD difference to trigger boost
ORACLE_LEAD_BOOST = 0.15

# Funding rate thresholds (per 8h rate)
FUNDING_EXTREME_POSITIVE = 0.03
FUNDING_EXTREME_NEGATIVE = -0.03

# Minimum data points needed for meaningful indicator calculation
MIN_HISTORY_LEN = 10


class VolatilityRegime(str, Enum):
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    LOW_VOLATILITY = "LOW_VOLATILITY"
    NORMAL = "NORMAL"


@dataclass
class Signal:
    """Output of the signal generator."""

    score: float  # -1.0 (strong DOWN) to +1.0 (strong UP)
    confidence: float  # 0.0 to 1.0 — how much data backed the signal
    components: dict[str, float]  # individual indicator values
    market_price: Optional[float]  # current UP token mid-price on Polymarket
    timestamp: float  # unix seconds when signal was computed
    regime: VolatilityRegime = VolatilityRegime.NORMAL


# ---------------------------------------------------------------------------
# Pure helper functions for technical indicators
# ---------------------------------------------------------------------------


def _ema(values: Sequence[float], period: int) -> list[float]:
    """
    Compute exponential moving average over *values*.

    Returns a list the same length as *values*; the first element is
    seeded with a simple average of the first *period* items.
    """
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    result: list[float] = []
    # Seed with SMA of first `period` values
    sma = sum(values[:period]) / period
    result.append(sma)
    for v in values[period:]:
        sma = v * k + result[-1] * (1 - k)
        result.append(sma)
    return result


def _sma(values: Sequence[float], period: int) -> list[float]:
    """Simple moving average. Returns list of length ``len(values) - period + 1``."""
    if len(values) < period:
        return []
    out: list[float] = []
    s = sum(values[:period])
    out.append(s / period)
    for i in range(period, len(values)):
        s += values[i] - values[i - period]
        out.append(s / period)
    return out


def compute_rsi(prices: Sequence[float], period: int = RSI_PERIOD) -> Optional[float]:
    """
    RSI = 100 - (100 / (1 + RS))
    RS = avg_gain / avg_loss  over *period* periods.

    Returns RSI in [0, 100] or None if insufficient data.
    """
    if len(prices) < period + 1:
        return None

    gains: list[float] = []
    losses: list[float] = []
    for i in range(len(prices) - period, len(prices)):
        delta = prices[i] - prices[i - 1]
        if delta >= 0:
            gains.append(delta)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(delta))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_macd_histogram(
    prices: Sequence[float],
    fast: int = MACD_FAST,
    slow: int = MACD_SLOW,
    signal_period: int = MACD_SIGNAL,
) -> Optional[float]:
    """
    MACD Line = EMA(fast) - EMA(slow)
    Signal Line = EMA(signal_period) of MACD Line
    Histogram = MACD Line - Signal Line

    Returns the latest histogram value, or None if insufficient data.
    """
    if len(prices) < slow + signal_period:
        return None

    ema_fast = _ema(list(prices), fast)
    ema_slow = _ema(list(prices), slow)

    # Align: ema_fast starts at index (fast-1), ema_slow at (slow-1)
    offset = slow - fast
    if offset < 0 or len(ema_fast) <= offset:
        return None

    macd_line = [
        ema_fast[offset + i] - ema_slow[i] for i in range(len(ema_slow))
    ]
    if len(macd_line) < signal_period:
        return None

    signal_line = _ema(macd_line, signal_period)
    if not signal_line:
        return None

    # Histogram: last MACD - last signal
    hist = macd_line[-1] - signal_line[-1]
    return hist


def compute_vwap(ticks: Sequence[PriceTick]) -> Optional[float]:
    """
    VWAP = cumulative(price * volume) / cumulative(volume)

    Returns None if no volume data is available.
    """
    cum_pv = 0.0
    cum_v = 0.0
    for t in ticks:
        if t.volume > 0:
            cum_pv += t.price * t.volume
            cum_v += t.volume
    if cum_v == 0:
        return None
    return cum_pv / cum_v


def compute_obv(ticks: Sequence[PriceTick]) -> list[float]:
    """
    On-Balance Volume:
      +volume if close > prev close, -volume if close < prev close.

    Returns a list of cumulative OBV values.
    """
    if len(ticks) < 2:
        return []
    obv_vals = [0.0]
    for i in range(1, len(ticks)):
        if ticks[i].price > ticks[i - 1].price:
            obv_vals.append(obv_vals[-1] + ticks[i].volume)
        elif ticks[i].price < ticks[i - 1].price:
            obv_vals.append(obv_vals[-1] - ticks[i].volume)
        else:
            obv_vals.append(obv_vals[-1])
    return obv_vals


def compute_vpin(ticks: Sequence[PriceTick]) -> Optional[float]:
    """
    VPIN proxy = |buy_volume - sell_volume| / total_volume

    Buy/sell classification: if price >= prev price -> buy; else -> sell.
    Returns value in [0, 1] or None if insufficient data.
    """
    if len(ticks) < 2:
        return None
    buy_vol = 0.0
    sell_vol = 0.0
    for i in range(1, len(ticks)):
        vol = ticks[i].volume
        if vol <= 0:
            continue
        if ticks[i].price >= ticks[i - 1].price:
            buy_vol += vol
        else:
            sell_vol += vol
    total = buy_vol + sell_vol
    if total == 0:
        return None
    return abs(buy_vol - sell_vol) / total


def compute_vpin_direction(ticks: Sequence[PriceTick]) -> Optional[float]:
    """
    Signed VPIN direction: positive if buy-dominated, negative if sell-dominated.
    Magnitude proportional to imbalance.

    Returns value in [-1, 1].
    """
    if len(ticks) < 2:
        return None
    buy_vol = 0.0
    sell_vol = 0.0
    for i in range(1, len(ticks)):
        vol = ticks[i].volume
        if vol <= 0:
            continue
        if ticks[i].price >= ticks[i - 1].price:
            buy_vol += vol
        else:
            sell_vol += vol
    total = buy_vol + sell_vol
    if total == 0:
        return None
    return (buy_vol - sell_vol) / total


def compute_atr(ticks: Sequence[PriceTick], period: int = ATR_PERIOD) -> Optional[float]:
    """
    Average True Range using price ticks (we approximate high/low from adjacent prices).

    TR = |price[i] - price[i-1]|  (simplified, since we only have trade prices).
    ATR = EMA(TR, period).
    """
    if len(ticks) < period + 1:
        return None
    trs = [abs(ticks[i].price - ticks[i - 1].price) for i in range(1, len(ticks))]
    ema_vals = _ema(trs, period)
    return ema_vals[-1] if ema_vals else None


def detect_regime(ticks: Sequence[PriceTick]) -> VolatilityRegime:
    """
    Classify volatility regime using ATR ratio:
        ATR_ratio = current_ATR / ATR_SMA(50)

        > 1.5  -> HIGH_VOLATILITY (favor momentum)
        < 0.7  -> LOW_VOLATILITY (favor mean reversion)
        else   -> NORMAL (blended)
    """
    if len(ticks) < ATR_SMA_PERIOD + ATR_PERIOD + 1:
        return VolatilityRegime.NORMAL

    # Compute ATR series
    trs = [abs(ticks[i].price - ticks[i - 1].price) for i in range(1, len(ticks))]
    atr_series = _ema(trs, ATR_PERIOD)
    if len(atr_series) < ATR_SMA_PERIOD:
        return VolatilityRegime.NORMAL

    current_atr = atr_series[-1]
    atr_sma = sum(atr_series[-ATR_SMA_PERIOD:]) / ATR_SMA_PERIOD

    if atr_sma == 0:
        return VolatilityRegime.NORMAL

    ratio = current_atr / atr_sma

    if ratio > HIGH_VOL_THRESHOLD:
        return VolatilityRegime.HIGH_VOLATILITY
    elif ratio < LOW_VOL_THRESHOLD:
        return VolatilityRegime.LOW_VOLATILITY
    return VolatilityRegime.NORMAL


# ---------------------------------------------------------------------------
# Signal Generator
# ---------------------------------------------------------------------------


class SignalGenerator:
    """
    Computes a composite 8-factor directional signal for BTC prediction markets.

    Requires a running ``DataFeedManager`` to supply real-time price, oracle,
    and orderbook data.

    Usage::

        gen = SignalGenerator(feeds)
        signal = await gen.compute(market)
        if signal.score > 0.1 and signal.confidence > 0.55:
            # go UP
            ...
    """

    def __init__(
        self,
        feeds: DataFeedManager,
        *,
        weights: Optional[dict[str, float]] = None,
        funding_rate_value: float = 0.0,
    ) -> None:
        self._feeds = feeds
        self._weights = weights or dict(INDICATOR_WEIGHTS)
        self._funding_rate: float = funding_rate_value

    # ------------------------------------------------------------------
    # External funding-rate injection (fetched from Binance REST elsewhere)
    # ------------------------------------------------------------------

    def set_funding_rate(self, rate: float) -> None:
        """
        Update the current BTC/USDT perpetual funding rate.

        This value is typically fetched from Binance REST API
        (``GET /fapi/v1/premiumIndex``) on a periodic schedule.
        """
        self._funding_rate = rate

    # ------------------------------------------------------------------
    # Core compute method
    # ------------------------------------------------------------------

    async def compute(self, market: object | None = None) -> Signal:
        """
        Compute the composite signal from all 8 indicators.

        Args:
            market: Optional market dataclass (from ``MarketDiscovery``).
                    Used only for logging / context; not required for
                    indicator calculation.

        Returns:
            A ``Signal`` dataclass with the weighted composite score,
            confidence, and all component values.
        """
        spot_ticks = self._feeds.spot_history
        prices = [t.price for t in spot_ticks]
        now = time.time()

        components: dict[str, float] = {}
        available_count = 0  # how many indicators had enough data

        # 1. Price vs VWAP -----------------------------------------------
        vwap = compute_vwap(spot_ticks)
        if vwap is not None and self._feeds.spot_price is not None:
            diff_pct = (self._feeds.spot_price - vwap) / vwap
            # Clamp to [-1, 1]; scale so 0.1% deviation = ~0.5 signal
            components["price_vs_vwap"] = _clamp(diff_pct / 0.002)
            available_count += 1
        else:
            components["price_vs_vwap"] = 0.0

        # 2. RSI(7) -------------------------------------------------------
        rsi = compute_rsi(prices, RSI_PERIOD)
        if rsi is not None:
            # Map RSI: 50 -> 0, 70 -> +1, 30 -> -1
            components["rsi_signal"] = _clamp((rsi - 50.0) / 20.0)
            available_count += 1
        else:
            components["rsi_signal"] = 0.0

        # 3. MACD histogram ------------------------------------------------
        macd_hist = compute_macd_histogram(prices, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        if macd_hist is not None:
            # Normalize: typical BTC 5-min histogram is +-$5
            avg_price = prices[-1] if prices else 1.0
            norm = macd_hist / (avg_price * 0.0005) if avg_price else 0.0
            components["macd_signal"] = _clamp(norm)
            available_count += 1
        else:
            components["macd_signal"] = 0.0

        # 4. OBV divergence ------------------------------------------------
        obv_vals = compute_obv(spot_ticks)
        if len(obv_vals) >= 5 and len(prices) >= 5:
            # Simple divergence: direction of OBV vs direction of price over last 5 ticks
            price_dir = 1.0 if prices[-1] > prices[-5] else (-1.0 if prices[-1] < prices[-5] else 0.0)
            obv_dir = 1.0 if obv_vals[-1] > obv_vals[-5] else (-1.0 if obv_vals[-1] < obv_vals[-5] else 0.0)

            if price_dir == 0.0:
                # Price flat — OBV direction hints at accumulation / distribution
                components["obv_divergence"] = _clamp(obv_dir * 0.5)
            elif price_dir == obv_dir:
                # Confirmed: OBV aligns with price direction
                components["obv_divergence"] = _clamp(obv_dir * 0.6)
            else:
                # Divergence: OBV opposes price -> reversal hint in OBV direction
                components["obv_divergence"] = _clamp(obv_dir * 0.8)
            available_count += 1
        else:
            components["obv_divergence"] = 0.0

        # 5. Raw 5-minute momentum -----------------------------------------
        if len(spot_ticks) >= 2:
            # Use oldest and newest ticks within ~5 minutes
            lookback_cutoff = now - 300.0  # 5 minutes
            old_ticks = [t for t in spot_ticks if t.timestamp <= lookback_cutoff]
            ref_price = old_ticks[-1].price if old_ticks else spot_ticks[0].price
            current_price = spot_ticks[-1].price
            if ref_price > 0:
                mom_pct = (current_price - ref_price) / ref_price
                # Scale: 0.1% move -> ~0.5 signal
                components["momentum_5m"] = _clamp(mom_pct / 0.002)
                available_count += 1
            else:
                components["momentum_5m"] = 0.0
        else:
            components["momentum_5m"] = 0.0

        # 6. VPIN direction ------------------------------------------------
        vpin_dir = compute_vpin_direction(spot_ticks)
        if vpin_dir is not None:
            components["vpin_signal"] = _clamp(vpin_dir)
            available_count += 1
        else:
            components["vpin_signal"] = 0.0

        # 7. Funding rate --------------------------------------------------
        fr = self._funding_rate
        if abs(fr) > 0:
            if fr > FUNDING_EXTREME_POSITIVE:
                # Extremely positive funding -> longs overleveraged -> bearish
                components["funding_rate"] = _clamp(-1.0 * min(fr / FUNDING_EXTREME_POSITIVE, 2.0))
            elif fr < FUNDING_EXTREME_NEGATIVE:
                # Extremely negative funding -> shorts overleveraged -> bullish
                components["funding_rate"] = _clamp(1.0 * min(abs(fr) / abs(FUNDING_EXTREME_NEGATIVE), 2.0))
            else:
                # Mild funding rate — weak contrarian signal
                components["funding_rate"] = _clamp(-fr / FUNDING_EXTREME_POSITIVE * 0.3)
            available_count += 1
        else:
            components["funding_rate"] = 0.0

        # 8. Regime adjustment ---------------------------------------------
        regime = detect_regime(spot_ticks)
        momentum_component = components.get("momentum_5m", 0.0)
        mean_rev_component = -momentum_component  # simple mean-reversion proxy

        if regime == VolatilityRegime.HIGH_VOLATILITY:
            # Favor momentum — amplify momentum signal
            components["regime_adjustment"] = _clamp(momentum_component * 1.0)
        elif regime == VolatilityRegime.LOW_VOLATILITY:
            # Favor mean reversion — dampen momentum, add reversion
            components["regime_adjustment"] = _clamp(mean_rev_component * 0.8)
        else:
            # Normal — blended
            components["regime_adjustment"] = _clamp(
                momentum_component * 0.4 + mean_rev_component * 0.2
            )

        if len(spot_ticks) >= ATR_PERIOD + 1:
            available_count += 1

        # ------------------------------------------------------------------
        # Composite score
        # ------------------------------------------------------------------
        composite = sum(
            self._weights[k] * components[k] for k in self._weights
        )
        composite = _clamp(composite)

        # ------------------------------------------------------------------
        # Oracle lead detection boost
        # ------------------------------------------------------------------
        spot = self._feeds.spot_price
        oracle = self._feeds.oracle_price
        if spot is not None and oracle is not None:
            lead = spot - oracle
            if abs(lead) > ORACLE_LEAD_THRESHOLD_USD:
                direction = 1.0 if lead > 0 else -1.0
                boost = direction * ORACLE_LEAD_BOOST
                composite = _clamp(composite + boost)
                logger.debug(
                    "Oracle lead detected: spot=%.2f oracle=%.2f lead=%.2f boost=%.3f",
                    spot,
                    oracle,
                    lead,
                    boost,
                )

        # ------------------------------------------------------------------
        # Confidence: proportion of indicators that had sufficient data
        # ------------------------------------------------------------------
        total_indicators = len(self._weights)
        confidence = available_count / total_indicators if total_indicators > 0 else 0.0

        # Reduce confidence if we have very few data points overall
        if len(spot_ticks) < MIN_HISTORY_LEN:
            confidence *= len(spot_ticks) / MIN_HISTORY_LEN

        # Get market mid-price for UP token
        market_price: Optional[float] = None
        bid = self._feeds.best_bid
        ask = self._feeds.best_ask
        if bid is not None and ask is not None:
            market_price = (bid + ask) / 2.0
        elif bid is not None:
            market_price = bid
        elif ask is not None:
            market_price = ask

        signal = Signal(
            score=composite,
            confidence=min(confidence, 1.0),
            components=components,
            market_price=market_price,
            timestamp=now,
            regime=regime,
        )

        logger.info(
            "Signal computed: score=%.4f confidence=%.2f regime=%s components=%s",
            signal.score,
            signal.confidence,
            signal.regime.value,
            {k: round(v, 4) for k, v in signal.components.items()},
        )

        return signal


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    """Clamp *value* to [lo, hi]."""
    return max(lo, min(hi, value))
