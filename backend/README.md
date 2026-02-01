# Backend

## Backfill on Fly
1. Open a shell in the Fly app: `fly ssh console -a congress-tracker-api`.
2. Run the backfill command: `python -m app.backfill_events_from_trades`.

Note: run from the app container so it has access to the Fly volume data.
