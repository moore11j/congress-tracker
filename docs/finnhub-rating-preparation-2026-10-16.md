# Finnhub individual rating-change preparation, October 16, 2026

## Remaining feature and implementation

Individual analyst upgrades/downgrades remain a distinct FMP dependency. The selected free monthly recommendation counts and prepared EPS/revenue estimates and aggregate price targets do not replace these events. This change adds an explicit one-symbol, dated-window Finnhub collector and internal evidence reader. It does not publish canonical analyst events or activate a public replacement.

Source contract: the official [pinned Finnhub OpenAPI specification](https://github.com/Finnhub-Stock-API/finnhub-go/blob/8071bcd46ca5101adcef7a844c96b81a3eceb76e/api/openapi.yaml), retained as artifacts/finnhub-official-openapi-oct15.yaml, defines stock/upgrade-downgrade with symbol/from/to parameters and symbol, gradeTime, company, fromGrade, toGrade and action fields. The five documented actions map to Upgrade, Downgrade, Maintained, Initiated and Reiterated. No provider event ID, coverage guarantee, verified publication latency or individual price-target history is established by that schema. Account entitlement and live response behavior remain unverified; the change makes no claim that a particular plan includes it.

Files:

- backend/app/services/finnhub_rating_preparation.py: normalization, duplicate/conflict checks, raw-response persistence and inspection.
- backend/app/jobs/prepare_finnhub_ratings.py: explicit bounded CLI.
- backend/app/services/finnhub_research.py: adds the endpoint to the existing gated transport paths.
- backend/tests/test_finnhub_rating_preparation.py: isolated source, transport, cache and consumer checks.

The command is `python -m app.jobs.prepare_finnhub_ratings --symbol AAPL --from 2026-10-01 --to 2026-10-16`. Both FINNHUB_PAID_PREPARATION_ENABLED and FINNHUB_SHARED_LIMITER_ENABLED must be enabled before database access/provider work. This package enables neither, adds no recurring schedule and makes no paid request. The transport retains header authentication, no redirects, response/time limits, shared request accounting and 429 cooldown without automatic retries.

## Evidence and compatibility rules

Each request must identify one security and a window of at most 90 inclusive calendar days, with no future dates. Share-class punctuation maps to the provider without dropping the class. Responses at the 1,000-row bound are held, not reported complete. Invalid identities, timestamps, actors, grades or actions reject the whole scope. Empty responses remain explicit and never prove market coverage.

Provider gradeTime is retained separately from actual observation time. The company/analyst field remains sourceActor because the schema does not establish a firm-versus-person split; analystName stays absent. Grade labels are preserved, without inventing score weights or rating equivalence. Deterministic source identities use provider, security, actor, source timestamp and action. Exact economic duplicates collapse, while conflicting grades under that identity hold the response. Identity remains stable across overlapping windows and observation times, but historical FMP reconciliation remains required before any publication.

The staging cache retains the complete raw response, its canonical JSON hash, normalized projection, source version and actual observation time. Repeats within 24 hours validate the stored projection against the raw response, issue no new request and preserve the row clock. Expired failed refreshes preserve prior evidence. PostgreSQL advisory locking serializes the explicit scope. This is a latest staged snapshot, not immutable append-only history. A future scheduled collector needs a freshness policy suitable for individual events and demonstrated connection/load behavior; the 24-hour reuse is not a production alert-latency promise.

The internal reader revalidates evidence and rejects stale, future, corrupt or conflicting cache data. publicationEligible remains false. Cross-provider reconciliation, full returned-window coverage, source publication latency and individual price-target events remain explicit gaps. No public selector, canonical event, alert, score, monthly recommendation panel or FMP fallback changes.

## Verification

190 focused tests pass on isolated Linux Python 3.12.3 against in-memory SQLite: rating preparation, existing paid preparation/readers and Finnhub research. The first 188 passed before adding two complete transport/consumer checks; the final 190 all pass. Test handle11574 exited0. The earlier historical test handle87332 was absent; it was not treated as a successful result. An initial sandboxed WSL invocation was denied before execution, then the reviewed isolated invocation succeeded.

Checks cover the five actions; exact source timestamps and actor labels; malformed/wrong-share identities; future/out-of-window timestamps; duplicates/conflicts; raw and projection tampering; empty and bounded responses; source-clock/cache repeat; expired failed refresh; complete chunked HTTP response persistence including unknown provider fields; header authentication; BRK-B/BRK.B cache reuse without merging BRK.A; disabled gates; and unchanged real monthly recommendation consumer output. Only the staging snapshot table is created in preparation fixtures. No production record, paid response, publication or delivery proof is claimed from synthetic fixtures.

Manual review confirms the new path has no canonical writes, cron registration or public consumer selection. The unrelated frontend/tsconfig.tsbuildinfo retains SHA256 A185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA and is excluded.

## Release and outstanding acceptance

Local implementation is ready for reviewed PR/CI; no deployment yet. Latest bounded production guard receipt artifacts/release-blocker-1792113782.json shows one continuing protected portfolio cycle,561/1135 scopes and zero failed scopes. Actual process receipt artifacts/active-work-1792113892.json confirms an institutional reference worker live, with12 pending jobs. PR694-699 remain undeployed; no guard bypass, cycle reset or rollout retry.

Next requirements: review/merge this package, guarded deployment and installed verification; live account entitlement and real-response review at the final subscription step; complete window acquisition; canonical FMP-event reconciliation and consumer projection; then scheduled/delivery acceptance before activation. Individual price-target history still needs a supported source or an explicitly approved product coverage decision. Financial/institutional/transcript coverage, all monitoring/watchlist/daily/weekly/ranking consumers, sustained capacity, final Massive validation, zero FMP traffic and billing retirement remain open.

The previous status-only turn was no progress. This continuation completes executable missing-feature preparation and190 checks, with fresh production process/guard evidence. Goal remains active and FMP enabled. Pending private-data export approvals were untouched. No subscription, production selection, new schedule, hardware, customer email, video or landing-page change.
