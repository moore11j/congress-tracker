# Cold ticker build diagnosis, October 10

After PR539, public AAPL context generation took6.35s while unchanged-generation cached reads took0.357/0.275s. NVDA new generation took2.03s. This is an unresolved cold-path sample, not proof of database contention or a transcript regression.

The existing main.py builder already measures profile, quote, signals, source context, confirmation and build-slot wait, but emits the entire breakdown at INFO. The combined server starts uvicorn with access logging and no app logger/root INFO configuration. Reading the most recent100 Fly entries yields no retained timing rows. Initial log reader attempts assumed line-delimited/one-object JSON; corrected decoding handles concatenated pretty-printed objects without dumping unrelated logs.

Emit that existing breakdown at WARNING only when total build time is at least2000ms; fast builds remain INFO. No global logging-level change, user IDs, source text, private thesis content or credentials. Response payloads, cache freshness, score/price semantics and provider selections are unchanged. Twelve existing canonical entitlement-projection/public-cache checks pass on Python3.14.2; no full-suite claim. This release also carries completed transcript deployment receipts into tracked reports.

Deploy and capture actual slow-stage evidence before selecting a repair. Do not infer a database resize or mask missing/stale data. FMP remains enabled; Massive last.

## Deployment and bounded validation, 21:31 UTC

PR540 merged as ed68996caf4c9437af68246b3718ac004a976de8; workflow38087787347 completed successfully. The production image and five exact file hashes, including app/main.py, match. Exactly one shared2 CPU/2GB API/jobs host and one performance1 CPU/2GB database remain. Available API memory1397.8MB; no pending/running scheduled jobs at the sample.

Three ordinary public requests generate AAPL/NVDA/IBM contexts in2.110/1.348/1.643s, HTTP200, each quote marked not stale. No slow-stage warning appears in captured log buffers. End-to-end latency includes network time, so2.110s does not prove the internal build exceeded2000ms. This does not identify or resolve the earlier6.35s outlier.

Subsequent public verification returns all six HTTP200 responses: feed0.366/0.413s, NVDA profile0.217s, cached AAPL/NVDA contexts0.391/0.390s and readiness0.361s. These are bounded samples, not sustained weekday/weekly consumer or complete-data validation. No additional machine sizing or source cutover.

Artifacts: ticker-stage-probe-1791667892.json and two-service-verify-1791667912.json. No running local process handles remain. Continue actual slow-stage diagnosis before choosing a repair, and preserve source reconciliation and consumer gates before Massive last and FMP shutdown.

## Slow stage captured,21:43-21:45UTC

After PR541, new AAPL context takes7.839s end to end. Captured warning reports internal7.224s: profile394ms, quote196ms, signals5536ms, source context252ms, confirmation811ms, build wait0ms. This locates the slow stage without establishing its root cause. EXPO/APH/ACN build warnings2.27/2.29/2.70s were captured alongside it. Artifact ticker-slow-stage-1791668638.json.

A bounded read-only follow-up of the signal path (3s statement cap) completes AAPL0.945s/NVDA0.466s, so the full slow interval is not reproduced. Each makes19 SQL queries. The initial unified-signal query takes22/20ms; one global institutional_symbol_summary count takes193/311ms and its nonexecuting plan scans an estimated128060 entries. Other queries include a global government-contract count, per-symbol confirmation and underlying source metrics. No claim that this count explains all5.54s. Next inspect/remove unnecessary global scans with result parity and capture shared-process contention if the outlier persists. No index or query mutation performed. Artifact signal-query-profile-1791668703.json; all local sessions terminal.
