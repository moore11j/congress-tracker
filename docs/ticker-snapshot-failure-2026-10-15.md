# Ticker optional snapshot failure, October15,2026

Status: locally implemented and verified on main bbbc72c3; not merged/deployed yet. This is a reliability repair supporting the FMP migration, not a provider activation.

## Reproduced issue and change

During dependency validation, an unavailable backend exposed an uncaught `loadPublicTickerSnapshot` failure. Both metadata and the public fallback body awaited this optional snapshot. HTTP200 could still contain a failed React Server Component stream, so checking only status and literal application-error text was insufficient. Source intentionally rethrew non404 errors to avoid changing indexability, but current metadata already preserves index/follow independently of snapshot completeness.

The optional read now uses an AbortController with the existing2500ms SSR budget, returns unavailable on failure and clears its timer on every exit. It continues reading persisted snapshots without a rebuild, keeps the same success payload, and does not change canonical URLs or indexing policy. Failures log only symbol/status/error type. No invented facts, ranking changes, provider switches, paywall changes or landing redesign.

## Validation

- Nine focused metadata tests pass:503/500/network failure, abort/timeout, timer cleanup,404, successful original snapshot, indexing/canonical preservation and no profile rebuild on a cold public cache.
- The related five-file selection has31 passes and one unchanged macro positioning copy assertion failure. That exact failure was already present in the previous clean main comparison. Do not describe this as a full-suite pass.
- Node24.12/Next15.5.28 production build compiles, type-checks and completes successfully. No production API data used.
- Built app with backend fetch unavailable: plain reader, Chrome-like browser and Googlebot each return200 with zero decoded RSC error frames, no Suspense error markers, truthful unavailable content, canonical AAPL URL and index/follow. Ticker responses remain private/no-store. Timings0.701/0.220/0.081s are local fault-injection samples, not a production performance claim.
- Server logs retain expected source-unavailable diagnostics but no uncaught render digest. The stricter verifier parses embedded `self.__next_f` payloads; the earlier literal-only smoke check is not reused as full-page proof.

Evidence: ticker-snapshot-tests-oct15.log, ticker-snapshot-build-oct15.log, ticker-snapshot-server-oct15.log and ticker-snapshot-render-oct15.json in local artifacts. Production rollout and public verification remain pending. Sustained capacity/data completeness, core financials, institutional publication, transcripts/all consumers and final Massive/zero-FMP/billing remain separate gates.
