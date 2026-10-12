# Entitlement default query cost

October12,2026. Local repair on codex/bulk-entitlement-defaults from production403b0f4a; release pending.

## Problem and change

The ordinary19:03-19:05 receipt records watchlist-price job7.5982CPU/15.62wall seconds, monitoring refresh6.1555/12.61 and intraday sweep5.3019/19.82. These individual samples motivate profiling; they do not attribute all job cost to configuration or prove delivery. API two-hour mean0.202397 remains above0.125allowance at19:00.

Access checks invoke seed_feature_gates and seed_plan_limits, which queried each default primary key individually even when every row existed. The reproduced complete-default case makes215SELECTs. The repair bulk-reads existing feature keys and tier/feature pairs in two queries, then retains the same missing-row insertion and conditional commit. It neither caches presence across calls nor modifies existing administrator values. Deleted defaults remain eligible for reseeding in the same session. No entitlement limits, feature gates, plan prices, schedules, event identities or provider selection change.

## Validation

Python3.14.2: new regression first fails with213 excess reads; after repair,224 checks pass across default-query, email-digest, price-alert/reference and account/admin tests. Three failures are independently reproduced by restoring only the two original seed functions from origin/main in a fresh test process: market-pressure analytics persistence expectation, admin export price expectation, and existing same-session entitlement-cache invalidation after an admin gate change. No full-suite pass is claimed. The working files are not reverted for that baseline verification.

Actual isolated PostgreSQL16.15: six synthetic accounts across Free/Premium/Pro, an admin override and legacy configuration rows. Four alternating baseline/candidate runs preserve identical full entitlement and settings projections. SELECTs fall1339 to61 (includes two final projection reads). Baseline CPU1.21875/1.203125seconds, candidate0.125/0.0625; wall1.37980/1.26093 versus0.11509/0.06304. This demonstrates the exercised path, not an overall production CPU reduction. No providers or customer emails; owned local schema removed and PostgreSQL stopped/verified. Receipt entitlement-defaults-postgres-oct12.json, entitlement projectionSHA868a9e882df40b93ca81cf62180014869e087cf93e4f75c08b9d66ca5139a8a6 and settingsSHA5380fb713eefce12ae2f10df5e5dae3d8c6cff7efba8a6cb3bc765ff76b1987c.

Remote profiling attempts did not produce usable timing: first failed because an explicit new engine selected missing psycopg2 before connection; later read-only attempts reached observation deadlines. A process inventory verified each matching remote probe was absent before another action. These are not claimed completed performance measurements or a blocker to isolated verification. No private candidate payload was saved.

## Release acceptance

Review exact-source CI; deploy the unchanged behavior repair; verify runtime hashes and the default-query/override fixture with isolated SQLite on productionPython3.12; then observe ordinary alert jobs and public latency. Keep previous403b0f4a release for rollback. Sustained CPU credit/capacity and every provider/consumer gate remain independently required.

Yahoo preparation remains in separate draftPR619 and is not included. Core fundamentals and transcripts remainFMP, institutional publicationfalse. No new collection schedule, source activation, capacity/storage subscription, customer email, video or landing change, or FMP cancellation.
