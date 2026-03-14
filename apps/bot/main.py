"""
Polymarket BTC Prediction Market Trading Bot

Target: 20-30% monthly ROI
Strategy: Oracle latency arbitrage + composite momentum signals
Mode: PAPER_TRADING=true by default — validated before live deployment
"""

import asyncio
import logging

from config import Config
from data_feeds import DataFeedManager
from market_discovery import MarketDiscovery
from signal_generator import SignalGenerator
from risk_manager import RiskManager
from execution import ExecutionEngine
from paper_trader import PaperTrader
from tracker import TradeTracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    config = Config()
    config.validate()
    logger.info(
        f"Starting bot | paper_mode={config.paper_trading} | target=20-30%% monthly ROI"
    )

    tracker = TradeTracker(config.database_url)
    await tracker.init()

    discovery = MarketDiscovery()
    feeds = DataFeedManager()
    signals = SignalGenerator(feeds)
    risk = RiskManager(config, tracker)

    if config.paper_trading:
        executor = PaperTrader(tracker, initial_balance=config.initial_balance)
        logger.info("PAPER TRADING MODE — no real funds at risk")
    else:
        executor = ExecutionEngine(config, tracker)
        logger.warning("LIVE TRADING MODE — real funds at risk")

    async with discovery:
        await asyncio.gather(
            feeds.run(),
            bot_loop(discovery, feeds, signals, risk, executor, config, tracker),
        )


async def bot_loop(discovery, feeds, signals, risk, executor, config, tracker):
    """Core trading loop: discover market -> compute signal -> size position -> execute."""
    logger.info("Bot loop started, waiting for data feeds to initialize...")
    await asyncio.sleep(5)

    while True:
        try:
            market = await discovery.get_current_market(
                interval_minutes=config.default_interval_minutes
            )
            if not market:
                await asyncio.sleep(5)
                continue

            # Subscribe to market orderbook if not already (sync call)
            if market.up_token_id and market.down_token_id:
                feeds.subscribe_market(market.up_token_id, market.down_token_id)

            # Wait for sufficient price data
            if feeds.spot_price is None:
                logger.debug("Waiting for spot price data...")
                await asyncio.sleep(1)
                continue

            # Refresh risk manager's cached data from DB
            await tracker.refresh_cache()

            signal = await signals.compute(market)

            if abs(signal.score) < config.min_signal_threshold:
                await asyncio.sleep(1)
                continue

            if risk.circuit_breaker_active():
                logger.warning("Circuit breaker active — skipping trade")
                await asyncio.sleep(60)
                continue

            bankroll = await executor.get_balance()

            # Determine token based on signal direction
            if signal.score > 0:
                token_id = market.up_token_id
            else:
                token_id = market.down_token_id

            position = risk.size_position(
                signal=signal,
                market_price=signal.market_price if signal.market_price else 0.50,
                bankroll=bankroll,
                token_id=token_id,
            )

            if position is None:
                await asyncio.sleep(1)
                continue

            if position.size_usd >= config.min_order_size_usd:
                # Convert Market dataclass to dict for execution layer
                market_dict = {
                    "up_token": market.up_token_id,
                    "down_token": market.down_token_id,
                    "condition_id": market.condition_id,
                    "slug": market.slug,
                    "interval_minutes": config.default_interval_minutes,
                }
                order = await executor.place_order(market_dict, signal, position)
                if order:
                    logger.info(
                        f"Order: {order.side} ${order.size_usd:.2f} @ {order.price:.3f} "
                        f"| signal={signal.score:.3f} | balance=${bankroll:.2f}"
                    )
            else:
                logger.debug(
                    f"Position too small (${position.size_usd:.2f}), skipping"
                )

            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Bot loop error: {e}", exc_info=True)
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
