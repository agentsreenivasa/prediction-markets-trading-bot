"""
Bot configuration loaded from environment variables with safe defaults.

All risk parameters, API endpoints, and credentials are centralised here.
Paper trading is **always** the default — live trading requires explicit opt-in
via ``PAPER_TRADING=false`` *and* all credential env vars being set.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv is optional
    pass

logger = logging.getLogger(__name__)

_ENV_PREFIX = "POLY_"


def _env(name: str, default: str = "") -> str:
    """Read an environment variable, optionally prefixed."""
    return os.getenv(name, default)


def _env_bool(name: str, default: bool = True) -> bool:
    """Read an env var as a boolean (accepts true/1/yes, case-insensitive)."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes")


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%r, using default %s", name, raw, default)
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid int for %s=%r, using default %s", name, raw, default)
        return default


@dataclass
class Config:
    """Central bot configuration — every field is overridable via env var."""

    # ── Trading mode ────────────────────────────────────────────────────
    paper_trading: bool = field(
        default_factory=lambda: _env_bool("PAPER_TRADING", default=True)
    )

    # ── API endpoints ───────────────────────────────────────────────────
    clob_host: str = field(
        default_factory=lambda: _env("CLOB_HOST", "https://clob.polymarket.com")
    )
    gamma_host: str = field(
        default_factory=lambda: _env("GAMMA_HOST", "https://gamma-api.polymarket.com")
    )
    chain_id: int = field(
        default_factory=lambda: _env_int("CHAIN_ID", 137)
    )
    signature_type: int = field(
        default_factory=lambda: _env_int("SIGNATURE_TYPE", 2)
    )

    # ── Credentials (from env, never hardcoded) ─────────────────────────
    private_key: str = field(
        default_factory=lambda: _env("POLY_PRIVATE_KEY")
    )
    funder_address: str = field(
        default_factory=lambda: _env("POLY_FUNDER_ADDRESS")
    )
    api_key: str = field(
        default_factory=lambda: _env("POLY_API_KEY")
    )
    api_secret: str = field(
        default_factory=lambda: _env("POLY_API_SECRET")
    )
    api_passphrase: str = field(
        default_factory=lambda: _env("POLY_API_PASSPHRASE")
    )

    # ── Risk management ─────────────────────────────────────────────────
    max_daily_loss_pct: float = field(
        default_factory=lambda: _env_float("MAX_DAILY_LOSS_PCT", 0.05)
    )
    max_risk_per_trade: float = field(
        default_factory=lambda: _env_float("MAX_RISK_PER_TRADE", 0.02)
    )
    kelly_fraction: float = field(
        default_factory=lambda: _env_float("KELLY_FRACTION", 0.25)
    )
    min_signal_threshold: float = field(
        default_factory=lambda: _env_float("MIN_SIGNAL_THRESHOLD", 0.55)
    )
    max_concurrent_exposure: float = field(
        default_factory=lambda: _env_float("MAX_CONCURRENT_EXPOSURE", 0.10)
    )
    max_consecutive_losses: int = field(
        default_factory=lambda: _env_int("MAX_CONSECUTIVE_LOSSES", 5)
    )
    max_hourly_loss_pct: float = field(
        default_factory=lambda: _env_float("MAX_HOURLY_LOSS_PCT", 0.02)
    )

    # ── Execution ───────────────────────────────────────────────────────
    max_execution_latency_ms: float = field(
        default_factory=lambda: _env_float("MAX_EXECUTION_LATENCY_MS", 300.0)
    )
    heartbeat_interval_s: float = field(
        default_factory=lambda: _env_float("HEARTBEAT_INTERVAL_S", 5.0)
    )
    min_order_size_usd: float = field(
        default_factory=lambda: _env_float("MIN_ORDER_SIZE_USD", 1.0)
    )
    default_interval_minutes: int = field(
        default_factory=lambda: _env_int("DEFAULT_INTERVAL_MINUTES", 5)
    )

    # ── Data ────────────────────────────────────────────────────────────
    database_url: str = field(
        default_factory=lambda: _env("DATABASE_URL", "sqlite+aiosqlite:///trades.db")
    )
    initial_balance: float = field(
        default_factory=lambda: _env_float("INITIAL_BALANCE", 10000.0)
    )

    # ── WebSocket URLs ──────────────────────────────────────────────────
    ws_rtds_url: str = field(
        default_factory=lambda: _env(
            "WS_RTDS_URL", "wss://ws-live-data.polymarket.com"
        )
    )
    ws_market_url: str = field(
        default_factory=lambda: _env(
            "WS_MARKET_URL",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        )
    )
    binance_ws_url: str = field(
        default_factory=lambda: _env(
            "BINANCE_WS_URL",
            "wss://stream.binance.com:9443/ws/btcusdt@trade",
        )
    )

    # ── Validation ──────────────────────────────────────────────────────

    _REQUIRED_CREDENTIALS = (
        "private_key",
        "funder_address",
        "api_key",
        "api_secret",
        "api_passphrase",
    )

    def validate(self) -> list[str]:
        """
        Validate configuration.

        Returns a list of error messages.  An empty list means the config is
        valid.  In paper-trading mode missing credentials are logged as
        warnings but are *not* errors.
        """
        errors: list[str] = []

        # Risk-param sanity checks
        if not 0.0 < self.max_risk_per_trade <= 1.0:
            errors.append(
                f"max_risk_per_trade must be in (0, 1], got {self.max_risk_per_trade}"
            )
        if not 0.0 < self.kelly_fraction <= 1.0:
            errors.append(
                f"kelly_fraction must be in (0, 1], got {self.kelly_fraction}"
            )
        if not 0.0 < self.max_daily_loss_pct <= 1.0:
            errors.append(
                f"max_daily_loss_pct must be in (0, 1], got {self.max_daily_loss_pct}"
            )
        if not 0.0 < self.min_signal_threshold <= 1.0:
            errors.append(
                f"min_signal_threshold must be in (0, 1], got {self.min_signal_threshold}"
            )

        # Credential checks
        missing = [
            name for name in self._REQUIRED_CREDENTIALS if not getattr(self, name)
        ]
        if missing:
            if self.paper_trading:
                logger.warning(
                    "Paper-trading mode: credentials not set (%s). "
                    "Live trading will fail until they are configured.",
                    ", ".join(missing),
                )
            else:
                errors.append(
                    f"Live trading requires all credentials. Missing: {', '.join(missing)}"
                )

        return errors
