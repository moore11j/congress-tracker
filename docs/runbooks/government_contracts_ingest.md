# Government Contracts Ingest

## One-off ingest on Fly

Open a shell in the API app:

```bash
fly ssh console -a congress-tracker-api
```

Then run the targeted contractor ingest from the app root:

```bash
cd /app
python -m app.ingest.government_contracts --symbols LMT,RTX,BA,MSFT,PLTR,ORCL,AMZN,GD,NOC,HII,LDOS,CACI,BAH,SAIC --lookback-days 365 --min-award-amount 1000000 --max-pages 10
```

Broad backfill mode is also supported:

```bash
cd /app
python -m app.ingest.government_contracts --lookback-days 365 --min-award-amount 10000000 --max-pages 50
```

## Scheduler split

Run core ingest every 8 hours:

```bash
cd /app
python -m app.ingest_run --job core
```

Run the daily government contracts refresh once per day:

```bash
cd /app
python -m app.ingest_run --job government-contracts-daily
```

Run the weekly 365-day government contracts backfill once per week:

```bash
cd /app
python -m app.ingest_run --job government-contracts-weekly
```

Notes:

- The contracts ingest enforces a 12-hour guardrail using `app_settings`, so duplicate cron fires within 12 hours will skip safely.
- Daily and weekly scheduled jobs default to the seeded target symbols and use idempotent upserts keyed by `source + award_id`.

## Verification SQL

```sql
SELECT symbol, COUNT(*), SUM(award_amount)
FROM government_contracts
GROUP BY symbol
ORDER BY SUM(award_amount) DESC
LIMIT 25;
```

## Verification endpoints

- `GET /api/tickers/LMT/government-contracts?lookback_days=365&min_amount=1000000`
- `GET /api/screener?...government_contracts_active=true...`

## Expected behavior after ingest

- Ticker government contract summaries return real rows from `government_contracts`.
- Screener government contract filters become filterable when data exists.
- `gov_overlay_status` resolves to `ok` once rows are present.
- Empty result sets after filtering should behave like normal empty results, not `unavailable`.
