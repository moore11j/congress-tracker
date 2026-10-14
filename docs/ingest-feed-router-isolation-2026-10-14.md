# Ingestion feed-router isolation

October 14, 2026. Candidate based on production/main 646ad8dc. Not deployed yet.

Scheduled ingestion imports the canonical Congress backfill writer through ingest_congress_recent. That writer eagerly imported the public events router solely for verify_event_filters. Move that import into the explicit verification function. The event writer, filter requests, exceptions, job choices, source gates, schedules and host sizes are unchanged. This also removes the unused router from other processes importing that writer. It does not remove the separate application import used by ticker-context enrichment.

## Evidence

The existing 600-second observer completed, including all six preloaded job types and ordinary delivery. Excluding initial buffered rows, the fully enclosed 20:56 through 21:05 UTC interval has 28 completed jobs and 28.3206 CPU seconds. One enrichment batch uses 8.7521 CPU seconds/34.23 wall seconds and exits successfully. This excludes API/supervisor/enqueue CPU and unfinished jobs; it is not whole-host attribution or proof that enrichment caused the earlier throttle peak. Partial preparation exits remain explicit and are not automatically classified as crashes.

Three alternating local Linux Python 3.12.3 cold imports average 2.598549 seconds before and 2.181394 after (16.05% lower). The third local candidate sample is slower than its paired baseline; local measurements vary. Four alternating isolated production Python 3.12.15 probes average 1.377726 seconds before and 1.166145 after (15.36% lower). Individual production samples are before 1.578444/1.177008 and after 1.218062/1.114228. These are import-only measurements, with sockets blocked, SQLite in memory and no actual jobs/provider calls/production writes. They do not establish full-job savings or sustained capacity. All samples retain the same 135-table schema signature and every CLI job choice; loaded modules drop from 793 to 758.

The initial production comparison stopped before imports on a diagnostic quoting error, then on a source-match guard. Investigation found Windows default text decoding misread a UTF-8 punctuation character; explicit UTF-8 resolves it and the deployed source exactly matches the baseline. No production source drift was found.

Artifacts in the primary workspace: remaining-workload-1792011942.json, router-workload-summary-oct14.json, router-startup-benchmark-1792011810.json and router-startup-production-1792012033.json. The benchmark scripts and baseline are retained alongside these receipts.

## Validation and release gate

Cold subprocess regression prohibits app.main and app.routers.events during ingestion startup and disabled profile dispatch, with network blocked. Explicit filter verification still calls the same reader for symbol, amount and both date windows, raises on inconsistent results and propagates reader exceptions. Ingestion, Congress event identities, duplicate prevention, source ownership, enrichment and monitoring checks complete with 146 passes on Linux Python 3.12.3. The first broad run has 107 passes, one existing workflow assertion failure and 39 setup errors from missing HTTPX; after adding HTTPX 0.28.1, all 39 previously blocked source publication checks pass. The 107 already-passing checks were not needlessly repeated. DuckDB 1.5.6 is the repository-pinned dependency, added only to the isolated test environment. The unchanged workflow assertion expects direct SSH ingestion despite the deployed serialized dispatch workflow; both files match the baseline and are outside this change.

Before rollout: finish affected checks, review the exact source diff and CI, then verify the deployed source and cold-import fixture. Observe an ordinary affected ingestion job and public consumers. Rollback is the previous backend release. A fast postrestart sample is not sustained capacity proof. Core fundamentals and transcripts remain FMP; institutions remain unpublished. Financial coverage/definitions, all score/ranking/monitoring/watchlist/daily/weekly consumers, institutional coverage, transcripts, final Massive activation, global zero-FMP traffic and billing cancellation remain gates.
