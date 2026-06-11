# Data Enrichment Queue Runbook

The `data_enrichment_jobs` queue refreshes market-data cache misses outside web request handlers. User-facing page loads enqueue bounded work; the Fly `cron` process drains it with `scripts/run_enrichment_queue.sh`.

## Schedule

Production scheduling lives in `backend/crontab` and runs every 5 minutes:

```cron
*/5 * * * * cd /app && sh /app/scripts/run_enrichment_queue.sh
```

Keep the Fly cron process separate from the web process:

```powershell
cd backend
fly deploy --build-only
fly scale count app=1 cron=1 -a congress-tracker-api
```

## Controls

Defaults are checked into `backend/fly.toml`:

```text
DATA_ENRICHMENT_QUEUE_ENABLED=true
DATA_ENRICHMENT_QUEUE_BATCH_SIZE=50
DATA_ENRICHMENT_QUEUE_MAX_SECONDS=45
```

Set `DATA_ENRICHMENT_QUEUE_ENABLED=false` to pause queue draining without removing the cron entry. Set `FMP_BACKGROUND_REFRESH_ENABLED=false` to pause all FMP background refresh behavior; the wrapper exits successfully and logs `processed=0 succeeded=0 failed=0 skipped=1`.

The worker processes at most `DATA_ENRICHMENT_QUEUE_BATCH_SIZE` jobs per run and stops between jobs once `DATA_ENRICHMENT_QUEUE_MAX_SECONDS` is reached. FMP provider calls still pass through the central provider budget guard, so budget exhaustion is logged as `provider_budget_exceeded` in provider usage telemetry.

The wrapper uses an atomic `/tmp/data_enrichment_queue.lock` directory so overlapping runs on the same cron Machine skip instead of running concurrently. Keep exactly one Fly `cron` Machine running to avoid cross-Machine overlap.

## Manual QA

1. Trigger a ticker page with missing or stale cache.
2. Confirm a `data_enrichment_jobs` row is created with `status='queued'`.
3. Wait for the 5-minute cron run, or run manually:

```powershell
flyctl ssh console -a congress-tracker-api --command "cd /app && sh /app/scripts/run_enrichment_queue.sh"
```

4. Confirm the row moves from `queued` to `done` or `failed`.
5. Check provider usage logs for bounded FMP calls and any `provider_budget_exceeded` throttles.
6. Reload the page and confirm it shows refreshed cached data.
