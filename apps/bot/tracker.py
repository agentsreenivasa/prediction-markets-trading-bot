"""Trade tracking with SQLAlchemy async models.

Provides persistent storage for all trades (live and paper) with
analytics queries for P&L, win rate, and consecutive-loss detection.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    JSON,
    String,
    desc,
    func,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQLAlchemy declarative base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class Trade(Base):
    """Represents a single trade record."""

    __tablename__ = "trades"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    timestamp: datetime = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    market_slug: str = Column(String, nullable=False)
    condition_id: str = Column(String, nullable=False)
    side: str = Column(String, nullable=False)  # "UP" or "DOWN"
    token_id: str = Column(String, nullable=False)
    entry_price: float = Column(Float, nullable=False)
    size_usd: float = Column(Float, nullable=False)
    exit_price: Optional[float] = Column(Float, nullable=True)
    pnl: Optional[float] = Column(Float, nullable=True)
    paper_mode: bool = Column(Boolean, default=True, nullable=False)
    signal_score: float = Column(Float, nullable=False)
    signal_components: Optional[dict] = Column(JSON, nullable=True)
    win: Optional[bool] = Column(Boolean, nullable=True)
    order_id: Optional[str] = Column(String, nullable=True)
    order_type: str = Column(String, default="GTC", nullable=False)  # GTC / FOK
    execution_latency_ms: Optional[float] = Column(Float, nullable=True)
    fees: Optional[float] = Column(Float, nullable=True)
    status: str = Column(String, default="open", nullable=False)  # open/filled/cancelled/resolved


# ---------------------------------------------------------------------------
# TradeTracker
# ---------------------------------------------------------------------------

class TradeTracker:
    """Async trade-tracking interface backed by SQLAlchemy.

    Exposes sync cached properties (daily_pnl, hourly_pnl, consecutive_losses,
    etc.) that are updated via ``refresh_cache()``.  The RiskManager reads
    these properties synchronously between async cache refreshes.
    """

    def __init__(
        self,
        database_url: str = "sqlite+aiosqlite:///trades.db",
        initial_balance: float = 10_000.0,
    ) -> None:
        self._database_url = database_url
        self._engine = None
        self._session_factory = None

        # Sync cache for RiskManager Protocol compatibility
        self._cached_daily_pnl: float = 0.0
        self._cached_hourly_pnl: float = 0.0
        self._cached_consecutive_losses: int = 0
        self._cached_recent_results: list[float] = []
        self._initial_balance: float = initial_balance

    # -- sync properties for RiskManager Protocol ----------------------------

    @property
    def daily_pnl(self) -> float:
        return self._cached_daily_pnl

    @property
    def hourly_pnl(self) -> float:
        return self._cached_hourly_pnl

    @property
    def consecutive_losses(self) -> int:
        return self._cached_consecutive_losses

    @property
    def daily_start_bankroll(self) -> float:
        return self._initial_balance

    @property
    def current_bankroll(self) -> float:
        return self._initial_balance + self._cached_daily_pnl

    def get_recent_results(self, n: int = 20) -> list[float]:
        return self._cached_recent_results[:n]

    async def refresh_cache(self) -> None:
        """Refresh all cached sync properties from the database."""
        if self._session_factory is None:
            return
        self._cached_daily_pnl = await self.get_daily_pnl()
        self._cached_hourly_pnl = await self._get_hourly_pnl()
        self._cached_consecutive_losses = await self.get_consecutive_losses()
        self._cached_recent_results = await self._get_recent_pnl_results()

    async def _get_hourly_pnl(self) -> float:
        """Sum of pnl in the last hour."""
        hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        async with self._session_factory() as session:
            result = await session.execute(
                select(func.coalesce(func.sum(Trade.pnl), 0.0)).where(
                    Trade.timestamp >= hour_ago,
                    Trade.pnl.is_not(None),
                )
            )
            return float(result.scalar())

    async def _get_recent_pnl_results(self, n: int = 20) -> list[float]:
        """Return recent PnL values for autocorrelation calculation."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(Trade.pnl)
                .where(Trade.pnl.is_not(None))
                .order_by(desc(Trade.timestamp))
                .limit(n)
            )
            return [float(row[0]) for row in result if row[0] is not None]

    # -- lifecycle -----------------------------------------------------------

    async def init(self) -> None:
        """Create the async engine, session factory, and tables."""
        self._engine = create_async_engine(
            self._database_url,
            echo=False,
            future=True,
        )
        self._session_factory = sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("TradeTracker initialised  db=%s", self._database_url)

    # -- write ---------------------------------------------------------------

    async def record_trade(self, trade_data: dict[str, Any]) -> int:
        """Insert a new trade row and return its id."""
        async with self._session_factory() as session:
            trade = Trade(**trade_data)
            session.add(trade)
            await session.commit()
            await session.refresh(trade)
            logger.info(
                "Recorded trade id=%s  market=%s  side=%s  size=$%.2f",
                trade.id,
                trade.market_slug,
                trade.side,
                trade.size_usd,
            )
            return trade.id

    async def update_trade(self, trade_id: int, updates: dict[str, Any]) -> None:
        """Update an existing trade row by id."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(Trade).where(Trade.id == trade_id)
            )
            trade = result.scalar_one_or_none()
            if trade is None:
                logger.warning("update_trade: trade id=%s not found", trade_id)
                return
            for key, value in updates.items():
                setattr(trade, key, value)
            await session.commit()
            logger.debug("Updated trade id=%s  fields=%s", trade_id, list(updates.keys()))

    # -- read ----------------------------------------------------------------

    async def get_daily_pnl(self) -> float:
        """Return the sum of pnl for today (UTC)."""
        today_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        async with self._session_factory() as session:
            result = await session.execute(
                select(func.coalesce(func.sum(Trade.pnl), 0.0)).where(
                    Trade.timestamp >= today_start,
                    Trade.pnl.is_not(None),
                )
            )
            return float(result.scalar())

    async def get_recent_trades(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return the most recent *limit* trades as dicts, newest first."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(Trade).order_by(desc(Trade.timestamp)).limit(limit)
            )
            trades = result.scalars().all()
            return [
                {
                    "id": t.id,
                    "timestamp": t.timestamp.isoformat() if t.timestamp else None,
                    "market_slug": t.market_slug,
                    "condition_id": t.condition_id,
                    "side": t.side,
                    "token_id": t.token_id,
                    "entry_price": t.entry_price,
                    "size_usd": t.size_usd,
                    "exit_price": t.exit_price,
                    "pnl": t.pnl,
                    "paper_mode": t.paper_mode,
                    "signal_score": t.signal_score,
                    "signal_components": t.signal_components,
                    "win": t.win,
                    "order_id": t.order_id,
                    "order_type": t.order_type,
                    "execution_latency_ms": t.execution_latency_ms,
                    "fees": t.fees,
                    "status": t.status,
                }
                for t in trades
            ]

    async def get_consecutive_losses(self) -> int:
        """Count consecutive losses from the most recent resolved trades."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(Trade.win)
                .where(Trade.win.is_not(None))
                .order_by(desc(Trade.timestamp))
            )
            count = 0
            for (win,) in result:
                if win:
                    break
                count += 1
            return count

    async def get_stats(self, days: int = 30) -> dict[str, Any]:
        """Aggregate statistics over the last *days* days.

        Returns dict with: win_rate, total_trades, total_pnl,
        profit_factor, avg_trade_pnl.
        """
        since = datetime.now(timezone.utc) - timedelta(days=days)
        async with self._session_factory() as session:
            result = await session.execute(
                select(Trade).where(
                    Trade.timestamp >= since,
                    Trade.win.is_not(None),
                )
            )
            trades = result.scalars().all()

            total_trades = len(trades)
            if total_trades == 0:
                return {
                    "win_rate": 0.0,
                    "total_trades": 0,
                    "total_pnl": 0.0,
                    "profit_factor": 0.0,
                    "avg_trade_pnl": 0.0,
                }

            wins = sum(1 for t in trades if t.win)
            gross_profit = sum(t.pnl for t in trades if t.pnl and t.pnl > 0)
            gross_loss = abs(sum(t.pnl for t in trades if t.pnl and t.pnl < 0))
            total_pnl = sum(t.pnl for t in trades if t.pnl is not None)

            return {
                "win_rate": wins / total_trades if total_trades else 0.0,
                "total_trades": total_trades,
                "total_pnl": total_pnl,
                "profit_factor": (
                    gross_profit / gross_loss if gross_loss > 0 else float("inf")
                ),
                "avg_trade_pnl": total_pnl / total_trades if total_trades else 0.0,
            }
