# Backend

## Backfill on Fly
1. Open a shell in the Fly app: `fly ssh console -a congress-tracker-api`.
2. Run the backfill command: `python -m app.backfill_events_from_trades --replace`.

Note: run from the app container so it has access to the Fly volume data.

## Verify backfill
- `curl "https://congress-tracker-api.fly.dev/api/events?limit=3"`
- `curl "https://congress-tracker-api.fly.dev/api/events?trade_type=sale&limit=3"`
- `curl "https://congress-tracker-api.fly.dev/api/events?chamber=house&limit=3"`
