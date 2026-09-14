# Missing public Outcome horizon measurements

## Audit

Using the latest completed market session, September 11, the continuous public ledger had 3,075 verified events. There were 218 overdue 7D measurements and 1,122 overdue 30D measurements. Another 13 events were legitimately awaiting 7D. No 90D, 180D, or 365D measurements were due.

The 7D backlog spanned every score band: 80+ (18), 75–79 (29), 70–74 (35), 65–69 (47), 60–64 (34), and 40–59 (55). DECK, DT, WELL, ONC, MAA, RF, TSN, and DMLP had target-session cache rows without canonical price-basis metadata. LZ, WOLF, and UTI lacked the target-session row. The detail price chart can render interim prices without a stored, verified horizon observation; those points do not establish a completed measurement.

The former provider job selected at most 100 scoring snapshots, including internal scoring updates and missing-entry reconstruction. Its ordering repeated unresolved work. Consequently, repairing cached prices alone did not complete the public event backlog.

## Change

The scheduled price job now first repairs the original anchors of continuous public events. It batches all due targets for each ticker, hydrates the shared benchmark first, and creates immutable horizon observations immediately after ticker hydration. Existing entry prices and completed observations are preserved. The provider phase is bounded by the existing job time budget, rather than the 100-snapshot reconstruction limit.

Attempt timestamps persist independently of public response caches. Unattempted tickers precede retries, so unavailable data cannot permanently block later symbols. Production allows up to 15 minutes for this background job to cover daily maturities. Internal snapshot reconstruction remains available after the public phase if time remains. Exact market-session and verified split-adjusted price requirements are unchanged.

## Validation

70 Outcomes tests passed, with the two previously documented baseline tests deselected. All 38 scheduler tests passed; one needed an explicit local SQLite test database instead of the production Linux default path. New regression cases cover more than 100 public anchors in one symbol batch, exclusion of internal scoring-version entries, idempotency, and advancing past a failing provider symbol across bounded runs.

The user explicitly approved a production backfill for the 218 overdue 7D and 1,122 overdue 30D measurements, followed by refreshing the public caches. Raw audit evidence is retained locally under `frontend/test-results/outcomes-7d-repair/` (ignored by Git).
