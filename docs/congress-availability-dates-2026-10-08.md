# Congress filing dates and observed availability

October 8, 2026. Continued execution of the owner's instruction to replace and turn off FMP. This checkpoint prepares the House date corrections without applying them to public records.

## Source-bound correction

Four complete public filings have uniquely matched stock economics but later legacy report dates:

| Official document | Member | Trades | Official filing day | Preserved legacy availability day |
|---|---|---:|---|---|
| 20035580 | Lloyd Doggett | 4 | October 5 | October 6 |
| 20035558 | Michael Rulli | 3 | October 2 | October 5 |
| 20035499 | Pete Sessions | 1 | September 22 | September 23 |
| 9116361 | Tony Wied | 4 | October 2 | October 7 |

The isolated correction changes filing/report fields and the official event date, while preserving canonical IDs, `Event.ts`, ingestion time, transaction dates, economics, saved outcomes, portfolio positions, alert/read state and queued work. Archived original rows, exact source hashes, row bindings and a receipt permit an exact repeat with zero changes. The earlier filing date does not move simulated entry dates backwards. Day precision matches the existing daily execution models; the retained legacy report day is explicitly labelled and is not a claim of exchange-level availability timing.

New direct Congress events now record their actual Walnut publication time and an explicit availability day separately from the official filing day. Backtests, Signal Mixer and replicated portfolios use that availability for disclosure-based entry dates. Watchlist activity windows use the preserved arrival index for explicitly annotated records. Legacy records retain their current date behavior. Invalid explicit availability is rejected by the execution-date helpers.

The operator-only PostgreSQL correction requires a paused source at the reviewed generation, unchanged staged bytes/metadata, a complete uniquely matched population and the reviewed before-state hash. It obtains bounded, non-waiting writer locks and archives originals atomically. It does not select or resume a provider, publish new events or send email. No automatic job calls this correction.

## Verification

Four real PDF fixtures and their captured public canonical records drive the tests. All 12 events retain the same backtest entry dates and saved portfolio context after correction; repeated correction changes nothing. Actual watchlist activity, monitoring and daily digest builders retain their item counts. Re-running alert creation produces no additional alerts, and email delivery stays at zero. Calendar acquisition is excluded from this date-only check; this is not a fresh replacement-driven Top Stocks or price parity claim.

Thirty-six date-correction tests pass locally on Python 3.14.2. An earlier alert fixture mistakenly added its pre-existing NVDA watchlist item twice; the fixture now respects the unique key. A broader run passed 235 tests and hit two unrelated default-database path errors; those two pass with the isolated in-memory test database. The exact isolated release checkout passes all 378 selected Congress, date, backtest, portfolio, Signal Mixer, monitoring and digest checks with `DATABASE_URL=sqlite:///:memory:`; only existing FastAPI deprecation warnings remain.

Six PostgreSQL scenarios pass on the deployed Python 3.12.15 runtime: successful apply/repeat, stale population, unpaused source, wrong generation, changed staged source and injected archive failure. Every table is session-private `pg_temp`, with temporary-table identity checked before use and removal verified afterward. No public data is read or written. The reviewed date-correction module hash is `54b763af4be45cbf1aed59b78564e191a5c3c3f58488d55889cb96a8a68303fe`.

Local evidence lives in `artifacts/direct-feeds/congress-date-correction-2026-10-08/`; durable public fixtures and the PostgreSQL test script are included in the release.

## Real Senate schedule observation

The October 8 23:27 UTC official Senate publisher started automatically and completed its processing at 23:29:03. It inserted zero events and sent zero emails. The previously adopted Whitehouse filing was not republished; the known Fetterman bond filing was held for its legacy date/lot conflict. The command intentionally returned exit status 1 for its partial result at 23:29:14. This verifies scheduled execution and conservative repeat handling, not complete Senate coverage or an all-success run. The preceding 23:17 collector completed same-day October 1–8 discovery.

## Remaining cutover work

Public House dates are unchanged. A date repair cannot be followed by blindly resuming FMP, whose old report dates could recreate the same transactions. Complete the House ownership transition or install a verified per-document legacy-write exclusion before applying these repairs, then refresh affected current evidence/ranking caches and verify no-send previews. Source-date correction may legitimately change evidence freshness; unchanged saved portfolio results are not proof of unchanged current rankings.

Cleo Fields 20035464 has five official rows versus four stored rows, including two separate MSFT lots and official GOOG versus stored GOOGL. David Taylor 20035549 has five official rows versus three stored rows because repeated HD/MPC lots are missing. These require source-row reconciliation, not date-only correction or arbitrary lot merging. Three current-window House scans remain held; the older Sessions correction is outside the October 1 cutover window.

Senate remains the only selected official Congress feed. House, Form 4 and 13F still use FMP for canonical ingestion. Massive Starter activation, fresh retained-product prices/scores/content and complete monitoring/daily/weekly checks, runtime FMP shutdown and billing cancellation remain open. No subscription changes or emails were made in this checkpoint.
