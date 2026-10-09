# Form 4 cutover validation

October 8, 2026 Pacific. Continued execution of the owner's request to replace and turn off FMP. The SEC writers remain unselected while the fresh production baseline is reconciled. House and Senate already use official sources.

## Publication safeguards

New Form 4 events now retain the official filing day in `event_date` and payload evidence, while `ts` and explicit `source_availability` record actual publication. Late collection cannot backdate the event's availability in monitoring, disclosure-based backtests or recorded portfolio decisions. Existing events and saved history are not rewritten by this change. Daily availability does not establish an intraday tradable execution price.

Publication fingerprints now include both event timestamps. Repeated publication holds a changed arrival or filing date without overwriting the successful receipt. Adopted legacy filings now bind their complete normalized transaction population; changed economics, missing rows or added lots are held on repeat. This legacy-population fingerprint does not claim to verify every legacy public event association. Source and event reconciliation remain separate cutover requirements.

New 13F-derived public events use the same actual-publication availability, including a current filing released only after a missing prior quarter arrives. Historical prior-quarter imports still produce no alerts. Holder-event receipts bind arrival time as well as official filing day; repeats preserve both dates. Existing institutional events, saved portfolio history and materiality thresholds are unchanged.

The bounded exporter runs in a repeatable-read, database-enforced read-only transaction. It selects the October 7 SEC cohort, verifies staged source checksums and captures complete relevant symbol populations, including older filings/events that could otherwise be mistaken for new trades. No customer records, credentials or email are exported. The replay reparses source bytes, applies only the existing source-bound correction planner in SQLite memory, runs the actual guarded publisher and checks monitoring deduplication and full-state repeat equality. These local corrections are not production repairs.

## Checks

The exact isolated release passes **106 checks** across publication, source parsing, canonical rehearsal, schedules, availability and no-send monitoring/watchlist/daily/weekly prepared-input parity on Python **3.14.2**. Two existing FastAPI deprecation warnings remain. An older assertion that new events use the historic filing day was updated to require actual publication time while preserving the official filing day. No full-suite result or fresh whole-product parity is claimed.

The institutional follow-up passes **39 checks** in both primary and isolated release checkouts, covering the 13F publisher, batch and projection, including late prior-quarter arrival and timestamp-drift rejection. Together these scoped release runs cover 145 checks.

Deployment, fresh cohort results and subsequent scheduled House receipt will be appended after verification. Global FMP usage and billing remain active. Massive Starter and FMP account login remain pending; the existing questions are not repeated.
