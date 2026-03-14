# Architecture

## System Overview
The bot is built as an async Python application with these core modules:

| Module | File | Purpose |
|--------|------|---------|
| Config | `config.py` | Environment-based configuration |
| Data Feeds | `data_feeds.py` | WebSocket connections to Binance + Polymarket |
| Market Discovery | `market_discovery.py` | Deterministic slug generation + Gamma API |
| Signal Generator | `signal_generator.py` | 8-factor composite signal (-1.0 to +1.0) |
| Risk Manager | `risk_manager.py` | Kelly criterion sizing + circuit breakers |
| Execution | `execution.py` | py-clob-client order placement |
| Paper Trader | `paper_trader.py` | Simulated fills for validation |
| Tracker | `tracker.py` | SQLAlchemy async trade persistence |

## Data Flow
```
Binance WS ──┐
              ├─→ DataFeedManager ─→ SignalGenerator ─→ RiskManager ─→ Execution
RTDS WS ─────┘                                                        │
Market WS ───────→ Orderbook Monitor ──────────────────────────────────┘
```

## Key Design Decisions
1. **Async-first**: All I/O through asyncio for sub-300ms latency
2. **Paper trading default**: PAPER_TRADING=true always
3. **Deterministic market discovery**: Slug-based, no API indexing delay
4. **Fractional Kelly**: Quarter Kelly (0.25x) for conservative sizing
5. **Multi-tier circuit breakers**: Daily, hourly, consecutive-loss
