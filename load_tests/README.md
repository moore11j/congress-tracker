# Walnut Capacity Load Tests

Reusable k6 harness for Walnut Market Terminal capacity planning.

These scripts are intentionally conservative. They are for measuring capacity, not proving capacity by assumption. Do not run broad production load tests without an explicit watched window and rollback/stop plan.

## Safety Rules

- Default target is local/staging, not production.
- Production targets are blocked unless `ALLOW_PRODUCTION_LOAD_TEST=true`.
- Do not run these tests during institutional ingestion, backfills, or incident recovery.
- Do not enable, increase, or restart cron jobs for a load test.
- Do not hardcode real user credentials.
- Do not print secrets.
- Stop immediately on fresh `500`, `503`, `OperationalError`, `db_pool_timeout`, or `heavy_route_saturated`.

Production-like hosts blocked by default:

- `app.walnutmarkets.com`
- `walnutmarkets.com`
- `congress-tracker-api.fly.dev`

## Files

- `k6/walnut_capacity_smoke.js`: tiny smoke profile plus bot/prefetch guard scenario.
- `k6/walnut_capacity_stages.js`: staged small/medium/large/target profiles.
- `env.example`: safe environment variable template.

## Install k6

Windows:

```powershell
winget install k6.k6
```

macOS:

```bash
brew install k6
```

Linux:

```bash
sudo gpg -k || true
curl -s https://dl.k6.io/key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/k6-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt update
sudo apt install k6
```

## Run Tiny Smoke Locally Or Staging

PowerShell:

```powershell
$env:BASE_URL="http://localhost:3000"
$env:API_BASE_URL="http://localhost:3000"
k6 run load_tests/k6/walnut_capacity_smoke.js
```

Staging example:

```powershell
$env:BASE_URL="https://staging.example.com"
$env:API_BASE_URL="https://staging-api.example.com"
k6 run load_tests/k6/walnut_capacity_smoke.js
```

The smoke profile defaults to:

- `SMOKE_VUS=3`
- `SMOKE_DURATION=90s`
- Bot/prefetch guard: `1` VU for `30s`

## Production Smoke Only With Explicit Approval

Do not run this casually. Use only during a watched window.

```powershell
$env:BASE_URL="https://app.walnutmarkets.com"
$env:API_BASE_URL="https://congress-tracker-api.fly.dev"
$env:ALLOW_PRODUCTION_LOAD_TEST="true"
$env:SMOKE_VUS="3"
$env:SMOKE_DURATION="60s"
k6 run load_tests/k6/walnut_capacity_smoke.js
```

Production smoke should stay in the `1-5` VU range unless a separate test plan is approved.

## Staged Profiles

Run staged profiles against staging first.

```powershell
$env:BASE_URL="https://staging.example.com"
$env:API_BASE_URL="https://staging-api.example.com"
$env:TEST_PROFILE="small"
k6 run load_tests/k6/walnut_capacity_stages.js
```

Profiles:

- `small`: ramps to 25 VUs. Staging first.
- `medium`: ramps to 100 VUs. Staging only until small is clean.
- `large`: ramps to 250 VUs. Staging only unless separately approved.
- `target`: ramps to 1,000 VUs. Config exists for planning; do not run without a dedicated environment and approval.

## Traffic Mix

The smoke/stage scenario uses this weighted mix:

- 40% feed/events
- 25% ticker pages and ticker APIs
- 10% signals
- 10% screener/watchlists/monitoring
- 5% institution pages
- 5% auth/account/basic session routes
- 5% insider/member pages

Core routes include:

- `/api/events?limit=25&enrich_prices=0`
- `/api/tickers/AAPL/context-bundle`
- `/api/tickers/NVDA/context-bundle`
- `/api/tickers/{symbol}/signals-summary`
- `/api/tickers/{symbol}/government-contracts`
- `/api/market/quotes?symbols=NVDA,AAPL,LMT,PLTR`
- `/feed`
- `/ticker/{symbol}`
- `/signals`
- `/screener`
- `/institution/0001067983`

Secondary routes are intentionally light:

- `/member/NANCY_PELOSI`
- `/insider/tim-cook-0001214156?lookback=1095`
- `/watchlists`
- `/monitoring`
- `/login`
- `/pricing`
- `/api/plan-config`

The harness does not hammer insider/member secondary analytics directly.

## Authenticated Scenario

Authenticated runs are optional and should use staging-safe sessions only.

Supported env vars:

- `AUTH_TOKEN`
- `SESSION_COOKIE`

If no safe token/session is available, skip authenticated load tests. Do not put credentials in scripts, docs, Git, or terminal transcripts.

## Metrics And Thresholds

k6 reports:

- `http_req_duration` p50/p95/p99
- `http_req_failed`
- requests per second
- status-code counters via `walnut_status_codes`
- per-route timings via `walnut_route_duration`
- `five_xx_rate`

Request tags:

- `route_family`
- `route_priority`
- `user_state`
- `endpoint_name`

Smoke thresholds:

- core route error rate: less than 1%
- core route p95: less than 1000 ms
- no meaningful 5xx rate
- overall p95: less than 1500 ms

## Bot / Prefetch Guard Scenario

The smoke script includes a tiny bot/prefetch guard scenario:

- bot UA request to `/ticker/AAPL`
- prefetch-style request to `/api/tickers/AAPL/context-bundle`
- prefetch-style request to `/api/tickers/NVDA/government-contracts`

Expected:

- lightweight success or `204` where guarded
- no `5xx`
- no heavy lower-page fanout in logs

## Observability Checklist

Before test:

- Confirm app health is green.
- Confirm institutional ingestion/backfills are not running.
- Confirm heavy cron jobs are not being changed for the test.
- Record current Fly release/image.
- Record DB connection baseline if available.

During test, watch Fly logs for:

- `db_pool_timeout`
- `db_pool_checkout_slow`
- `heavy_route_saturated`
- `OperationalError`
- `500`
- `503`
- request attribution by `route_family`
- context-bundle cache hit/stale/miss/build logs
- quote cache hit rate
- FMP calls/minute if available

Also capture:

- app CPU/memory
- DB CPU/connections
- request rate by route family
- p95/p99 by route family
- feed first-page behavior
- ticker context bundle timings

After test:

- Compare k6 p95/p99 with app logs.
- Check whether DB pool slow-checkout rose.
- Check whether provider calls stayed within budget.
- Check whether errors are concentrated in one route family.
- Confirm no background job was accidentally started.

## Stop Conditions

Stop the test immediately if any of these occur:

- core-route 5xx responses appear
- `heavy_route_saturated` appears on core routes
- `db_pool_timeout` appears
- `OperationalError` appears
- app health checks fail
- DB connection usage approaches the configured ceiling
- feed/ticker pages become visibly incomplete
- provider errors or latency spike enough to affect user routes

## Recommended First Real Test Plan

1. Staging `small` profile: 25 VUs.
2. Staging `medium` profile: 100 VUs.
3. Watched production smoke only: 1-5 VUs for 60-90 seconds.
4. Revisit route timings and logs before any larger production test.

Likely first bottlenecks to watch:

- DB pool checkout under bursty ticker context-bundle traffic.
- `/api/events` latency if feed traffic spikes.
- quote cache misses if many unique ticker symbols are requested.
- app-host SSR pressure from public ticker pages.
- insider/member secondary analytics if they are accidentally loaded eagerly.
