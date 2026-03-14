# API Reference

## Polymarket Endpoints
| Service | URL |
|---------|-----|
| CLOB API | `https://clob.polymarket.com` |
| Gamma API | `https://gamma-api.polymarket.com` |
| RTDS WS | `wss://ws-live-data.polymarket.com` |
| Market WS | `wss://ws-subscriptions-clob.polymarket.com/ws/market` |

## Market Discovery
```python
slug = f"btc-updown-5m-{floor(time() / 300) * 300}"
GET https://gamma-api.polymarket.com/events?slug={slug}
```

## Key Trading Endpoints
| Action | Method | Endpoint |
|--------|--------|----------|
| Get orderbook | GET | `/book?token_id={id}` |
| Place order | POST | `/order` |
| Cancel all | DELETE | `/cancel-all` |
| Get midpoint | GET | `/midpoint?token_id={id}` |

## Rate Limits
- General: 9,000 req/10s
- Order placement: 3,500 req/10s burst
- Gamma /events: 500 req/10s

## Heartbeat
Send every 5 seconds or all open orders get cancelled (10s timeout).
