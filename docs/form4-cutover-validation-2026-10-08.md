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

## Fresh October 7 cohort

The production snapshot captured at **October 9 00:24:22 UTC** contains all **325** October 7 Form 4s and complete relevant-symbol populations: **19,806 normalized transactions, 20,054 raw records, 20,054 public events and 306 canonical Form 4 filings**. All 325 original-source hashes verify. The 84,095,685-byte public export has SHA-256 `f0fcaefdddab9f2388222bae7371487fc5636a6cf89aafc835949f8b2b26ee26`. No production mutation or private customer export occurred. Export/transfer was slow; it was allowed to complete without restarting it or the scheduled publishers.

The offline replay plans **334 corrections** across 106 accessions: 167 normalized transactions and 167 existing events. It changes 154 prices/values and 81 derivative classifications, preserves IDs/dates/raw provider evidence, and changes nothing on repeated apply. These are still local corrections, not production repairs.

After the local correction, 251 filings parse/reconcile for publication and 74 remain quarantined. The actual worker adopts 233 existing filings, projects 98 transactions from ten new filings, and holds eight additional filings for unmapped legacy events/raw rows. All successful repeats, 98 synthetic alerts and the complete local database fingerprint are unchanged on repeat; zero email deliveries. The 74 source holds include 69 canonical conflicts, five amendments, nine joint-owner cases and one holding-only submission, with overlapping reasons. None are counted as completed coverage.

Ninety of the newly represented rows are **late-disclosed IRIX purchases**, not recent executions. Three filings by the same reporting person cover March 25–June 5, 2025; June 6–December 1, 2025; and December 3, 2025–March 30, 2026. All were filed October 7, 2026. The 90 economic identities are distinct. Their old transaction dates and October filing dates must remain visible separately from Walnut's new observation time. These three sources account for most of the new-row count.

Local evidence is under `artifacts/direct-feeds/form4-cutover-2026-10-08/`, including the baseline, initial `fresh-rehearsal.json` and source-bound `.plan.json`. This is a bounded current-cohort rehearsal, not comprehensive historical integrity or fresh FMP-free score/price coverage.

## Verified safeguard deployment and Congress observation

Form 4 release `0c0deadc` and the institutional follow-up `a0bb6c214946f1c8d96027c4adcfaeb77ad9fc28` deployed through [workflow 37866087227](https://github.com/moore11j/congress-tracker/actions/runs/37866087227). All four workers are started on the exact image. Workflow readiness/database and Premium-access checks pass. At **00:44:48 UTC**, runtime Python **3.12.15**, all four changed service-file hashes, unchanged official Congress ownership, zero SEC publication receipts and default FMP SEC ownership verify read-only. No schema migration or canonical source change accompanies these safeguards.

First scheduled House publication started **00:29 UTC**, returned at **00:32:43**, and exited at **00:33:35**. It retried the known Taylor conflict, inserted zero events and sent zero emails. Senate's 00:27 run likewise retained its known bond exception. Both correctly report partial/exit 1 for holds. At **00:41:32**, both official generation-2 controls, all seven successful House fingerprints, the 13 corrected official dates with original arrival times, and zero recorded Congress legacy-ingestion requests since switching still verify. Scheduled execution is established; held coverage is not resolved.

## Alert-date and publication-window follow-up

The expanded real-data replay exposed that monitoring alert creation still used an effective filing date even when watchlist selection used explicit arrival time. The local follow-up preserves the annotated arrival in new monitoring alerts, preventing late collected disclosures from disappearing from the current digest. Existing alert rows/read state are not rewritten. Text and HTML monitoring plus watchlist/daily activity items distinguish transaction and filing dates. A focused current-window fixture reaches all three builders once with no duplicate alert or delivery.

Form 4 batch selection also excludes filings before the immutable publication boundary and incomplete New York filing days, preventing older backlog from consuming the current publication limit. The individual publisher retains its independent source/date checks.

The final exact-release set passes **181 tests** on Python 3.14.2, with the same two deprecation warnings. The expanded real-data replay now passes actual watchlist activity (98 items), monitoring (12) and daily (8) builders under their existing limits. The 98 synthetic alert identities, rendered output and full database state remain identical on repeat; all alert bodies retain transaction and filing dates, with no network/email delivery. Calendar is explicitly outside this replay; prepared Top Stocks/price checks remain separate from fresh replacement-driven rankings. Receipt: `fresh-alert-rehearsal.json`, full-state SHA `18f02f554720d60f49e87b5e05756b83ffcaf5d349cc6df82a1a02c822801c70`.

Actual scheduled SEC collection run 16 finished at **00:49:14 UTC**, partial. Current staging has 3,074 pending Form 4s, 480 parsed and 645 quarantined; 13Fs have 198 parsed, 28 quarantined and 11 failed. These span the wider staging inventory, not just the 325-filing rehearsal cohort. They do not establish canonical publication readiness. The follow-up deployment receipt remains pending at this checkpoint.

Global FMP usage and billing remain active. Next work: guarded production correction/coverage, SEC ownership and actual publication observation, fresh replacement-driven rankings/prices/content and all alert cadences, then global runtime shutdown and verified billing cancellation. Massive Starter and FMP account login remain pending; the existing questions are not repeated.
