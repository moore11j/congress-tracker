# Scheduled digest source integrity, October 15, 2026

## Verified defects and repair

On main b1195899, the scheduled unified monitoring digest built Congress, insider, institutional and government-contract sections before applying subscription preferences. Already persisted alerts therefore appeared even when their subscription was inactive, removed, daily-disabled or set to Off/intraday-only. Separately, the matrix trigger reader fell back to retained legacy chips when no matrix category allowed daily delivery. Explicit Off settings could consequently be overridden in content and other monitoring items.

The same daily qualifier treated ticker/category as the identity of news and press items. Five distinct articles about one symbol were reduced to one instead of the existing four-item category limit. Overlapping watchlists made this look like duplicate prevention while dropping distinct evidence.

The repair applies the current subscription filter before activity-section selection, limits and deduplication. Matrix decisions no longer fall through to legacy chips. News and press identity uses category/ticker plus canonical event IDs and normalized source URLs; URL path case and meaningful query values are preserved. Legacy rows without either identity use normalized title/date. Non-content ranking identity and the existing four-per-content-category display caps remain unchanged. Event/alert/source records, provenance, timestamps, entitlements and delivery schedules are not rewritten.

The older watchlist-activity builder is not the scheduled CLI route. Its possible fallback/content overlap is separate from the confirmed scheduled-path defects repaired here; no completed all-path claim is made.

## Validation

- Windows Python3.14.2: initial 25 isolated new checks pass; expanded digest/matrix/direct-source/confirmation/financial-alert/Top Ideas/Top Stocks selection passes207 tests. Final33 new checks pass, including eight added edge cases. This is215 distinct passing backend tests across the runs, not a full-suite pass.
- Linux Python3.12.3: final33 new checks plus23 existing matrix checks pass,56 total.
- The actual Finnhub prepared snapshot publisher feeds canonical Events, both overlapping watchlists' MonitoringAlerts and the real daily digest. Three distinct headlines appear once each; repeated builds preserve all three event fingerprints and six alert identities, with zero EmailDelivery rows.
- Four activity categories are checked for Off/intraday/daily/both; inactive/deleted/daily-disabled and legacy preferences are verified. Alternate enabled triggers and overlap between disabled/enabled watchlists remain valid. Same-URL provider overlap, tracking parameters, case-sensitive paths, meaningful query differences, dismissed/future content and disabled/stale prepared caches are checked. FMP is disabled in the final isolated fixtures; network and mail attempts fail the tests.
- The broader suite includes actual Top Stocks cached-reader to daily/weekly Top Ideas projections around a SEC correction and unchanged price-alert inputs. It does not establish all production ranking coverage or scheduled email receipt.

Initial test scaffolding omitted the required security asset class, reused a unique watchlist name and used the wrong snapshot constructor field. These were corrected before final checks. WSL required elevated execution after sandbox access denial; it completed on the same confirmed test handle. No production records were exported and pending data-export approvals were not retried.

## Release and remaining work

Local branch codex/digest-source-integrity, based on b1195899. Exact release/review/runtime and ordinary schedule observation remain pending. No new customer email, source activation, subscription, hardware/schedule change or FMP cancellation. Full financial/institutional/transcript coverage, sustained capacity, all scheduled consumers, paid Finnhub validation, final Massive and zero-global-FMP/billing retirement remain open.


## Ordinary daily observation, October15 20:05-20:10UTC

After deployed PR682/425fdd1c, the scheduled monitoring job completed20:05:13 with exit0 and3.5022CPU seconds. Existing job aggregates show force=false/dry_run=false, four results: three sent, one skipped, zero failures;21 selected items from314 candidates,293 excluded. This is the job's send result, not inbox receipt or proof that every source/consumer is complete. Only numeric totals and scheduler metadata were retained; no customer identities or message contents were exported and no sends were manually triggered. Receipts artifacts/digest-schedule-observation-1792094726.json and digest-summary-observation-1792094881.json.

The ordinary Top Ideas job completed20:10:04 with exit0/1.9039CPU seconds, but returned zero results/deliveries. It establishes execution, not populated daily or weekly delivery. Current code skips weekly subscribers outside Friday; this Thursday observation alone does not establish why all results were empty. Receipts artifacts/digest-schedule-observation-1792095046.json and digest-summary-observation-1792095035.json.

Public response samples20:06:42 were all200/no server-render errors, Feed0.269s/Insights0.396s/AAPL0.304s/NVDA0.249s, artifacts/public-render-1792094802004.json. Sustained capacity and full source values remain unproved. Backend PR683 was deployed only after both observed jobs completed; no delivery schedule or preferences changed.
