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

Deployed and first ordinary warming run verified; details below. Core field coverage, ETR source failure, ACN/AZO reporting-period differences, institutional active capacity, Senate access, transcripts, all retained consumers and final Massive remain separate gates. This repair does not authorize an early core switch or establish complete financial coverage. No new subscription, hardware change, customer email or FMP cancellation.


## 2026-10-11, 22:52 UTC: Release and scheduled verification

PR #587 merged as `9723cff1e5fe3a627dc07f7a1be86fa26484da51`; backend workflow38181813937 succeeds. At22:49UTC all30 checked runtime file hashes match, both approved hosts retain their sizes, and the queue is empty. Eight sampled public requests return200: feeds0.388/0.434s, fresh AAPL2.423/NVDA1.028s, and50 news items. Core remains FMP, institutional publication false. Receipt `two-service-verify-1791758986.json`. These are sampled responses, not sustained capacity proof.

The first ordinary22:51 scheduled warmer processes10/10 symbols and records10 fresh source observations, versus zero new observations in the preceding four sampled batches. Fifty persisted priority deadlines all equal source observation plus20hours. Two unsupported financial-coverage results remain explicit; no public canonical writes or emails. AAPL, NVDA and ACN still await later bounded priority batches in this receipt, so their full refresh is not yet claimed. Receipt `priority-warming-runtime-1791759098.json`.

Automated review reports exhausted quota; the final application diff was directly reviewed. Secret scan and Vercel checks pass. Both dependency audits fail in unchanged dependencies; no full-suite or all-CI-green claim. PR attachment failed because this chat has100 attachments; the ordinary PR link remains usable. The local test setup errors recorded above are not passes. No purchase, capacity change, source activation, customer email or cancellation.

## Remaining source diagnostics

Fresh SEC submission indexes at22:47UTC confirm the latest periodic reports are ACN10-Q forMay31,2026 and AZO10-Q forMay9,2026. Their current prepared periods match those filings. FMP's later August periods are not evidence of a missed later periodic SEC filing; source availability differs, and earlier verified earnings-release inputs remain a coverage task. Receipt `sec-reporting-periods-1791758821.json` records both current index hashes and accessions.

A guarded, collector-serialized ETR preparation at22:50 reproduces `InlineFactsError: unsupported_report_date`. Its exact public company submissions, companyfacts and June30 filing are saved under `etr-inline-oct11` with verified hashes. The6,438,291-byte original filing hash is `8cba3a1c3a58ebd2ecd722493e51e08d840d9c936cb31aa5fbc9c00f3a20a3e9`. The date is explicitly `6/30/2026`, transformation `ixt:date-month-day-year`, namespace `http://www.xbrl.org/inlineXBRL/transformation/2020-02-12`, context `c-1`. This is the next bounded parser repair, not yet implemented. Failed preparation preserves the old cache. No source or fiscal date is guessed.
