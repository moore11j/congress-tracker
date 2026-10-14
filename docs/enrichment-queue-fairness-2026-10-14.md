# Bounded enrichment queue fairness

October 14, 2026 UTC. Continuation of the FMP replacement and reliability work.

## Evidence and change

The ordinary 06:47 enrichment run completed 120 jobs, but 2,952 jobs remained queued at 06:48, including 26 press requests. Strict priority selection can continually defer financial, news and press work behind replenished quotes. The separate process scheduler queue being empty does not establish data freshness.

The repair retains at least four of every five slots for the normal priority order. Remaining slots draw from jobs overdue by at least 30 minutes, alternating across job types before taking additional jobs of a type. These slots are interleaved so a deadline does not always discard them. Batches smaller than five retain priority selection, and a batch that fits all eligible jobs needs no second query. Within a priority, waiting time uses the current enqueue/retry timestamp rather than the original creation date of a repeatedly reused deduplication row.

Batch size, time limit, pressure guards, retry delays, claim locks, event identities, provider selections, alert schedules and hosting resources are unchanged. This improves selection fairness; it does not establish that the backlog is drained or that every provider succeeds. Different job types have different processing costs, so unchanged job counts are not a claim of unchanged CPU usage.

## Validation

Read-only PostgreSQL comparison at 06:54 selected 120 quotes with the old code. The proposed selector chose 97 quotes and 23 jobs across 12 other types, with 120 unique IDs. ONDS was not yet among those selected. Its scheduled SEC preparation remains an independent refresh path. Selection took 0.0206 seconds after the old query's 0.1786 seconds; cache/order effects mean these samples are not a controlled speedup benchmark. No production writes or provider calls occurred in the comparison.

Local Python 3.14.2 tests cover a representative quote/metadata flood, overdue progress across types, interleaving before pressure and deadline stops, current enqueue age, filters, future retry eligibility, terminal jobs and preserved retry backoff. The first run passed 84 checks and failed one unrelated legacy assertion expecting direct workflow execution. Both that test and `.github/workflows/daily_ingest.yml` have exactly the same Git blob hashes as current main. No workflow files changed. Final focused result and release receipt will be appended below.

Evidence: primary `artifacts/enrichment-selection-1791960857.json`, `artifacts/press-queue-1791960539.json`, and [prior SEC press report](sec-press-size-continuation-2026-10-14.md).

## Release state

Implementation and read-only production selection verification complete; release and ordinary worker validation pending. Core financials, institutional publication, transcripts, all consumers, sustained capacity, final Massive and global FMP/billing gates remain open. No source activation, new subscription or cancellation.
