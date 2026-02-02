# Backend

## Backfill on Fly
1. Open a shell in the Fly app: `fly ssh console -a congress-tracker-api`.
2. Run the backfill command: `python -m app.backfill_events_from_trades --replace`.

Note: run from the app container so it has access to the Fly volume data.

## Verify backfill
- `curl "https://congress-tracker-api.fly.dev/api/events?limit=3"`
- `curl "https://congress-tracker-api.fly.dev/api/events?trade_type=sale&limit=3"`
- `curl "https://congress-tracker-api.fly.dev/api/events?chamber=house&limit=3"`

## Production smoke checks
```bash
curl.exe -s "https://congress-tracker-api.fly.dev/api/meta"
curl.exe -s "https://congress-tracker-api.fly.dev/api/events?types=congress_trade&limit=1"
curl.exe -s "https://congress-tracker-api.fly.dev/api/signals/unusual?recent_days=365&baseline_days=365&multiple=1.5&min_amount=0&limit=5"
```

## Local smoke checks (optional)
```bash
curl -s "http://localhost:8000/api/meta"
curl -s "http://localhost:8000/api/events?types=congress_trade&limit=1"
curl -s "http://localhost:8000/api/signals/unusual?recent_days=365&baseline_days=365&multiple=1.5&min_amount=0&limit=5"
```
