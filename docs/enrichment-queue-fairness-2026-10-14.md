# Bounded enrichment queue fairness

October 14, 2026 UTC. Continuation of the FMP replacement and reliability work.

## Evidence and change

The ordinary 06:47 enrichment run completed 120 jobs, but 2,952 jobs remained queued at 06:48, including 26 press requests. Strict priority selection can continually defer financial, news and press work behind replenished quotes. The separate process scheduler queue being empty does not establish data freshness.

The repair retains at least four of every five slots for the normal priority order. Remaining slots draw from jobs overdue by at least 30 minutes, alternating across job types before taking additional jobs of a type. These slots are interleaved so a deadline does not always discard them. Batches smaller than five retain priority selection, and a batch that fits all eligible jobs needs no second query. Within a priority, waiting time uses the current enqueue/retry timestamp rather than the original creation date of a repeatedly reused deduplication row.

Batch size, time limit, pressure guards, retry delays, claim locks, event identities, provider selections, alert schedules and hosting resources are unchanged. This improves selection fairness; it does not establish that the backlog is drained or that every provider succeeds. Different job types have different processing costs, so unchanged job counts are not a claim of unchanged CPU usage.

## Validation

Read-only PostgreSQL comparison at 06:54 selected 120 quotes with the old code. The proposed selector chose 97 quotes and 23 jobs across 12 other types, with 120 unique IDs. ONDS was not yet among those selected. Its scheduled SEC preparation remains an independent refresh path. Selection took 0.0206 seconds after the old query's 0.1786 seconds; cache/order effects mean these samples are not a controlled speedup benchmark. No production writes or provider calls occurred in the comparison.

Local Python 3.14.2 tests cover a representative quote/metadata flood, overdue progress across types, interleaving before pressure and deadline stops, current enqueue age, filters, future retry eligibility, terminal jobs and preserved retry backoff. The first run passed 84 checks and failed one unrelated legacy assertion expecting direct workflow execution. Both that test and `.github/workflows/daily_ingest.yml` have exactly the same Git blob hashes as current main. No workflow files changed. Final focused result and release receipt are recorded below.

Evidence: primary `artifacts/enrichment-selection-1791960857.json`, `artifacts/press-queue-1791960539.json`, and [prior SEC press report](sec-press-size-continuation-2026-10-14.md).

## Release state

Implementation, deployed runtime and the first ordinary worker run are verified below; sustained recovery and broader source coverage remain open. Core financials, institutional publication, transcripts, all consumers, sustained capacity, final Massive and global FMP/billing gates remain open. No source activation, new subscription or cancellation.

## Verified release and ordinary operation

[PR629](https://github.com/moore11j/congress-tracker/pull/629), source `8d73fb49`, merged as `dd89afda`. Backend deployment `38525391671` succeeded, including readiness and Premium privacy checks. Vercel preview and secret scan pass. Both dependency audits fail in unchanged packages; no full CI pass claimed. App attachment attempted but rejected by the existing 100-attachment limit.

Final local result: **85 passed, one deselected** on Python 3.14.2. The excluded workflow text assertion was independently shown to fail in unchanged test/workflow blobs. Production Python 3.12 runs the new selector successfully against the actual queue. Four hashes verify the changed module, retained SEC press repair, unchanged worker script and unchanged crontab. Queue module SHA256: `21cc8436569163acf36374c3a45a6390009076df37afe3460cfd6456b8c7d5b4`.

The first ordinary post-release enrichment run finishes at 07:03:18 UTC with exit zero, 54.84 wall seconds and 13.3913 CPU seconds. Read-only row receipts confirm 97 quote jobs plus 23 across 12 other types completed: two each for metadata, CIK metadata, press, news, filings, technicals, historical financials, context, fundamentals, price series and profiles, plus one end-of-day price. The previous quote-only run used 44.84 wall/10.987 CPU seconds; different work makes this an observed cost comparison, not a controlled efficiency claim. Batch count and the 180-second deadline are unchanged. Many queued jobs remain.

During that run, Insights returns 200 in 0.652 seconds, Feed 0.439, news 0.250, research 0.492, AAPL context 2.053 and NVDA context 0.679. Five-minute CPU throttling is zero and credit balance is about 8,304; the preceding two hours included exhausted credits and throttling. This is current recovery, not sustained capacity proof. No hosting resource change.

Public content checks return three CLOV and four NVDA SEC releases with unique identities and identical repeats; NVDA and TSLA Finnhub news each return 12 unique items. ALAB historical financials contain four annual and 12 quarterly periods with explicit incomplete coverage. BULL remains unavailable with no statements: successful queue processing does not prove source coverage. ONDS subsequently recovers through its ordinary source rotation, as recorded below.

Receipts in primary artifacts: `enrichment-runtime-1791961209.json`, `enrichment-runtime-1791961435.json`, `fair-content-1791961482.json`; under `direct-feeds/free-replacements-2026-10-09`: `scheduled-job-cpu-1791961432.json`, `insights-incident-1791961368.json`, `cpu-quota-1791961401.json`.

Next: backlog and sustained load, unresolved financial coverage/definitions and institutional/transcript/consumer gates before final Massive validation and global FMP/billing shutdown. Neither the active goal nor the migration is complete. Previous and current goal turns both made concrete progress. No local processes remain running. No source selection, subscription, email or cancellation change.

## Related SEC press recovery

The ordinary SEC earnings run at 07:07:32 UTC completes all five planned scopes. ONDS now prepares three distinct releases instead of one, attempts all five selected filings, checks four, and retains one oversized original plus one existing-material reconciliation hold. The fourth isolated replay release, accession `0001193125-26-347973`, is held in production as `ambiguous_existing_material` with three existing candidates. That explains the isolated/live difference; no guard was relaxed. The previously visible release keeps its canonical key and source hash; two additional releases have distinct keys and hashes. Public response is partial/HTTP 200 in 0.743 seconds with an identical repeated item array. The scheduled receipt records zero canonical writes, emails and model calls. Demand queue job22918 remains pending, but the independent ordinary preparation has restored the public cache. Receipt: primary `artifacts/press-size-live-1791961754.json`. This verifies bounded recovery, not complete coverage of all eight discovered filings.
