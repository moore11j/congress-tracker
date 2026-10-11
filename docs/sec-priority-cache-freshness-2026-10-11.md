# SEC priority cache freshness, October 11, 2026

The financial audit found that SEC preparation tracked the time of a cache hit as the latest warming attempt, and used that attempt to decide the next priority refresh. A response fetched 18 hours ago could be reused now, then wait another 20 hours before priority refresh, despite the cache expiring after 24 hours. The preparation service also accepted 24-hour caches when the warmer intended a 20-hour refresh. This affects freshness of the free replacement, which remains unselected for public core fundamentals.

## Repair

The existing attempt timestamps continue to order the broad rotation. A separate priority deadline follows the actual source observation plus the existing 20-hour refresh interval. Priority preparation accepts caches younger than 20 hours; other callers retain the 24-hour default. Failed attempts retain the existing 20-hour priority backoff, preserve old evidence and allow other symbols to proceed. Known unavailable source results are still cached with their real observation time. Existing batch bounds, collector serialization, leases, cron timing, public providers and delivery behavior are unchanged.

On initial rollout, a single bounded metadata query initializes missing priority deadlines from cache timestamps. It reads only cache keys and timestamps, not financial payloads. This prevents old attempt state from postponing repair until another complete rotation. Stored deadlines cover only current priority symbols. Cache reuse requires source, symbol, version and observation timestamp to match the cache record; malformed or mismatched observations trigger fresh preparation instead of postponing a deadline.

## Evidence

Two actual service/job regression cases failed on the original implementation: a cached response was still reused after the intended refresh point, and a due refresh never reached the simulated source refusal. An additional migration case failed before metadata initialization because a recent attempt hid a 21-hour-old cache. Tests use isolated SQLite, actual preparation/projector behavior and captured test SEC structures; source transport is replaced and no email or public cache is written.

Read-only production PostgreSQL probe at 22:43 UTC applies the candidate cache function to five actual records on Python 3.12. AAPL, NVDA and ACN are accepted under the old 24-hour age but correctly rejected under the priority 20-hour age; AZO remains valid under both, and old-version/expired ETR is rejected under both. Each query runs in an explicitly read-only transaction with a three-second statement timeout. This is a cache-read proof, not an ordinary scheduled refresh. Receipt: `priority-cache-readonly-1791758613.json`.

The final expanded local suite passes 125 checks on Python 3.14.2. It covers preparation, identity, combination, provider baselines, schedules, timestamp corruption, cache-age boundaries, failure backoff and broad rotation. Earlier 124 checks passed before the rollout-state regression was added. The first test-file creation used the wrong working-directory prefix; a later suite command referenced a nonexistent test filename. Both setup errors were corrected and neither is counted as a test pass.

## Release and remaining gates

Prepared, not yet deployed. Verify the exact release and an ordinary warming run after deployment. Core field coverage, ETR source failure, ACN/AZO reporting-period differences, institutional active capacity, Senate access, transcripts, all retained consumers and final Massive remain separate gates. This repair does not authorize an early core switch or establish complete financial coverage. No new subscription, hardware change, customer email or FMP cancellation.
