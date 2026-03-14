# Quant Research

## Kelly Criterion for Binary Markets
```
f* = (b × p - q) / b
where: b = (1 - market_price) / market_price
       p = model probability, q = 1 - p
f_adjusted = f* × 0.25 (quarter Kelly)
position = min(bankroll × f_adjusted, bankroll × 0.02)
```

## Risk Parameters
| Parameter | Value |
|-----------|-------|
| Max risk per trade | 2% of bankroll |
| Kelly fraction | 0.25 (quarter) |
| Daily loss circuit breaker | 5% |
| Hourly loss pause | 2% |
| Consecutive loss stop | 5 trades |
| Max concurrent exposure | 10% |
| Min signal threshold | 0.55 |

## ROI Projections (Conservative)
- Win rate: 55%, Position: 1% of bankroll
- 35 independent trades/day
- Monthly ROI: ~36% (before friction)

## Fee Impact
- Max taker fee: 1.56% at 50/50 pricing
- Maker rebate: 20% of taker fees
- Net edge must exceed ~1.6% per trade
