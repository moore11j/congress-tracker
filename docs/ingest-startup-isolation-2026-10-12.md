# Ingestion startup isolation

October 12, 2026. [PR #618](https://github.com/moore11j/congress-tracker/pull/618) is merged as `403b0f4abdb04fa3d13d35234c1c6c0e7a0ad745`; deployment 38291455996 succeeded; runtime verification completed at 18:33 UTC.

Every ingestion command imported app.main to obtain the profile cache warmer, initializing unrelated web routes before monitoring, enrichment and Congress work. The repair imports that same warmer only when the guarded profile job invokes it. Arguments, result and exception behavior remain unchanged. No schedule, source selection, resource, event identity or customer-email change.

## Evidence and checks

Four alternating isolated production Python 3.12 probes, with sockets blocked and no job execution, measured profiled import CPU of 3.551294/3.656052 seconds before and 2.141198/2.197766 seconds after, approximately 39.8% lower. Loaded module count fell from 964 to 858. Artifact: `ingest-startup-1791829451.json` in primary artifacts/direct-feeds/free-replacements-2026-10-09. These measurements cover startup only; full-job throughput, alert delivery and sustained capacity are not established by this result.

The cold subprocess regression prohibits importing app.main during ordinary startup and disabled profile dispatch, then proves the wrapper forwards default/explicit force_refresh arguments, returns the same result and propagates failure. Network connections are prohibited, with an in-memory SQLite setting. Ingestion/monitoring/enrichment checks on Python 3.14.2: 76 passed, one unchanged legacy workflow assertion failed. Its test and workflow blobs are identical to production c4a90c9f; the assertion expects direct dispatch predating the serialized queue. Additional startup/watchlist-price/reference/scheduler checks: 20 passed and five Linux-only skips on Windows. The startup test overlaps the two groups; these are not 96 unique passing checks or a full-suite pass.

Exact source head `212671f9ccb3ac5aa08e70b9fcc890284de5a9e3`, CI 38291296976: secret scan passes. Initial scan 38290908196 misidentified the literal prose phrase market/trading/news in two historical documentation lines as a credential. Only those two commit/file/rule/line fingerprints were allowlisted after inspection. Other secret scanning remains enabled. Backend and frontend audits retain unchanged Pillow 10.4.0 and nanoid/Next/PostCSS/Sharp/source-map-js findings; current logs were inspected through the required CI tool. Dependency manifests and lockfiles are unchanged. No all-green CI claim.

## Release acceptance

Verify deployed hashes and the isolated cold-import fixture on production Python, then observe an ordinary affected scheduled job and public feed/ticker latency. Retain the previous deployment as rollback. Prepared helper `verify_ingest_startup_oct12.py` uses the 74-source hash set, blocks sockets, sets SQLite in memory and invokes no actual job or production database. Public core fundamentals remains FMP; institutions publication remains disabled. ACN ordinary propagation and all broader financial coverage, institutions, transcript, ranking/alert, final Massive and global FMP/billing gates remain open.

## Deployed receipt, October 12 at 18:33 UTC

Workflow 38291455996 completed successfully, including readiness and Premium privacy checks. Receipt ingest-startup-release-1791830003.json verifies all 74 runtime source hashes with no mismatch, Python 3.12.15, cold import without app.main, lazy default/explicit delegation and propagated failure. The isolated fixture used no production database, network requests or emails. Its 1.37534 CPU seconds is unprofiled fixture cost and must not be compared directly with the earlier cProfile totals.

Public receipt issuer-staging-public-1791829993.json returns HTTP 200: readiness 0.424s, two feeds 0.388/0.442s, AAPL context 1.331s, NVDA 1.435s and AZO press 0.247s with four release identities retained. These immediate postrestart samples do not establish sustained capacity or complete coverage. Public core remains FMP; institutional publication remains false. Ordinary affected-job and sustained-load observation remain required.

Ordinary observation receipt ordinary-idle-cpu-1791830034.json covers 18:32-18:33 UTC: SEC earnings exits 0 (1.3954 CPU seconds); reference and prior cooldown jobs exit 0 (0.2355/0.2428 CPU seconds). An earlier reference run exits 1 and its reason is not established by this receipt. None of these commands invokes app.ingest_run, so this observation does not verify the affected ordinary-job saving. All new local command handles are terminal; no local PostgreSQL was started.
