# Polymarket BTC Trading Bot

Automated trading bot for Polymarket BTC 5-minute and 15-minute Up/Down prediction markets.

**Target:** 20-30% monthly ROI
**Strategy:** Oracle latency arbitrage + composite momentum signals
**Mode:** Paper trading by default — validated before live deployment

## Quick Start (local k8s)
```bash
# 1. Set secrets in 1Password ARKARCTECH-DEV-TEST vault under "prediction-markets-bot"
# 2. Install op CLI and authenticate
op run --env-file=apps/bot/.env.op -- python apps/bot/main.py

# 3. Or via Helm (local k8s)
helm install pmbot ./helm/prediction-bot
```

## Architecture
See [Wiki: Architecture](../../wiki/Architecture)

## Research
- [Strategy Research](../../wiki/Strategy-Research) — Oracle arbitrage, momentum signals
- [Quant Research](../../wiki/Quant-Research) — Kelly criterion, risk management
