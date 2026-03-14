# Strategy Research

## Primary Edge: Oracle Latency Arbitrage
The bot exploits the 100-500ms lag between real-time CEX price feeds (Binance) and Chainlink oracle updates that resolve Polymarket markets.

## Signal Architecture
8 weighted indicators combined into a composite score:
- Price vs VWAP (20%) — Intraday fair value
- 5-min Momentum (20%) — Raw directional signal
- VPIN Order Flow (15%) — Informed trading proxy
- RSI(7) (10%) — Overbought/oversold
- Fast MACD (10%) — Trend confirmation
- OBV Divergence (10%) — Volume confirmation
- Regime Adjustment (10%) — Volatility-based strategy switch
- Funding Rate (5%) — Leverage sentiment

## Volatility Regime Detection
- HIGH (ATR ratio > 1.5): Favor momentum
- LOW (ATR ratio < 0.7): Favor mean reversion
- NORMAL: Blended approach

## Precedent
- swisstony bot: $5 → $4.7M in ~5 months using "Reality Arbitrage"
- Only 7.6% of Polymarket wallets are profitable (Dune Analytics)
