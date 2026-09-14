# Missing public Outcome horizon measurements

## Audit

Using the latest completed market session, September 11, the continuous public ledger had 3,075 verified events. There were 218 overdue 7D measurements and 1,122 overdue 30D measurements. Another 13 events were legitimately awaiting 7D. No 90D, 180D, or 365D measurements were due.

The 7D backlog spanned every score band: 80+ (18), 75–79 (29), 70–74 (35), 65–69 (47), 60–64 (34), and 40–59 (55). DECK, DT, WELL, ONC, MAA, RF, TSN, and DMLP had target-session cache rows without canonical price-basis metadata. LZ, WOLF, and UTI lacked the target-session row. The detail price chart can render interim prices without a stored, verified horizon observation; those points do not establish a completed measurement.

The former provider job selected at most 100 scoring snapshots, including internal scoring updates and missing-entry reconstruction. Its ordering repeated unresolved work. Consequently, repairing cached prices alone did not complete the public event backlog.

## Change

The scheduled price job now first repairs the original anchors of continuous public events. It batches all due targets for each ticker, hydrates the shared benchmark first, and creates immutable horizon observations immediately after ticker hydration. Existing entry prices and completed observations are preserved. The provider phase is bounded by the existing job time budget, rather than the 100-snapshot reconstruction limit. Immutable ORM objects remain loaded across per-ticker commits to avoid repeated full-ledger expiration; mutable prices are explicitly refreshed in a single query before grading. The caller's session expiration policy is restored afterward.

Attempt timestamps persist independently of public response caches. Unattempted tickers precede retries, so unavailable data cannot permanently block later symbols. Production allows up to 15 minutes for this background job to cover daily maturities. Internal snapshot reconstruction remains available after the public phase if time remains. Exact market-session and verified split-adjusted price requirements are unchanged.

## Validation

110 focused tests passed (72 Outcomes tests and 38 scheduler tests), with the two previously documented baseline tests deselected. Local tests use an explicit SQLite test database instead of the production Linux default path. New regression cases cover more than 100 public anchors in one symbol batch, exclusion of internal scoring-version entries, idempotency, advancing past a failing provider symbol across bounded runs, refreshing stale price objects while keeping immutable snapshots loaded, and waiting for the actual target trading session's close before classifying missing prices.

The user explicitly approved a production backfill for the 218 overdue 7D and 1,122 overdue 30D measurements, followed by refreshing the public caches. Raw audit evidence is retained locally under `frontend/test-results/outcomes-7d-repair/` (ignored by Git).

## Production backfill results

The public event count remains 3,075. Verified 7D measurements increased from 2,844 to 3,061 (+217); verified 30D measurements increased from 58 to 1,176 (+1,118). Thirteen recent events are not yet due for 7D. Stored observations have zero nonfinite returns and zero ticker/benchmark session mismatches.

The refreshed public overview and 500-row 7D snapshot cache expose these results. DECK now has a 7D return of -5.7185%, DT -5.9297%, ONC -1.4081%, and WELL +0.3511%. All six score bands have been covered; the sole remaining overdue 7D is LEG in the 65–69 band.

Browser verification also exposed a separate stale-table cause: `fetch(..., {cache: "force-cache"})` could keep using an expired browser HTTP response after the server refreshed. Outcome browser requests now use the default HTTP cache policy; Next server requests retain `force-cache` and their five-minute revalidation. Two execution tests cover both environments and the retry path, in addition to the 16 existing frontend Outcome tests.

Five missing measurements require corporate-action handling rather than an old-symbol close that does not exist:

- LEG, 7D target September 1: [Somnigroup completed the acquisition August 26](https://www.sec.gov/Archives/edgar/data/1206264/000120626426000121/sgi-20260826.htm); each LEG share became 0.1455 SGI shares.
- LPSN, 30D target September 8: [SoundHound completed the acquisition September 4 and announced that LPSN would cease Nasdaq trading](https://investors.soundhound.com/news-releases/news-release-details/soundhound-ai-completes-acquisition-liveperson-creating-world).
- LBRDA, 30D target September 9: [Charter completed its acquisition August 20](https://corporate.charter.com/newsroom/charter-and-cox-communications-complete-transaction).
- WBS, 30D target September 8: [Santander completed its acquisition August 20](https://www.websterbank.com/santander-bank/).
- GGRP, 30D target September 9: [the issuer changed its common-stock ticker to BTLN](https://ir.brightlineinteractive.com/brightline-interactive-inc-nasdaqbtln-formerly-the-glimpse-group-inc-nasdaqggrp-begins-trading-under-new-ticker-btln/).

These five observations remain absent. No stale last price, zero return, or unverified successor-security return was substituted. The new pending-status fix also avoids calling weekend targets missing before the next trading session closes; under the prior code 301 still-pending 30D events were prematurely counted as missing prices at the UTC date rollover.

The approved backfill and public cache refresh are complete. The permanent scheduler, runtime-budget, performance, and pending-status fixes are committed; their final Fly rollout awaits the separately requested explicit registry/deployment approval.
