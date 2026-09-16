# Frozen prospective bullish challenger

Freeze this protocol and source checksums before the first production read. No public score, outcome, entry, cache or methodology is written. Email is disabled pending the explicit destination/payload approval requested earlier.

## Capture and cohort

The first successful read is a baseline inventory only. Existing calls are not prospective observations. Subsequently enroll each previously unseen, still-open, directional public confirmation anchor once, only when both its creation and calculation timestamps are at or after the initial capture and at or before the current capture. Reuse the application's continuous-direction event projection so a scoring-version change does not itself manufacture a new public event. Preserve raw anchor and current snapshots, version/input/source hashes, all available contribution/freshness fields, and disclosure inputs. An already-closed or old backfilled anchor is recorded as excluded and is not enrolled later.

Capture through October 15, 2026. A real, timezone-aware prediction timestamp must follow the server capture by no more than one hour. Both alternatives enter at the next NYSE session strictly after that recorded timestamp's New York calendar date, even if the public event's own entry has already passed. These are separate research entries. Do not recover missed predictions retrospectively. No numeric score is reweighted: the single challenger either retains a bullish confirmation, abstains from it, or falls back to the original decision when evidence is unknown. Bearish decisions are retained unchanged.

The runner captures all source rows within a repeatable-read, read-only database transaction, with statement timeouts. Date-only insider filings must precede the capture's New York date; same-day and future-dated filings are removed before constructing buyer histories. Ingestion is proven only by actual capture, not by the filing date. Hashed actor identifiers and original transaction/disclosure provenance stay local. Captures and decisions are append-only; repeat runs do not change a completed day's predictions.

## One fixed challenger

Reuse the September 14 conviction normalization, deduplication, security-type exclusions, suspect monetary-value quarantine and prior-purchase calculations. Look at insider filings in the 90 days preceding capture. A known buyer must have at least three earlier positive, known-dollar purchase filing groups in that ticker in the previous 1,095 days. A qualifying unusual purchase is at least twice their previous median purchase size. Require positive, fully known net insider buying over that 90-day window.

For a bullish event with at least one known purchase comparison and fully known amounts for the window, retain it only when an unusual purchase is present and net buying is positive. Otherwise abstain. With insufficient buyer history, missing actor identity, unknown amounts, or no usable comparison, retain the original call and label it unknown-input fallback. The fixed 90-day lookback is inherited from the existing hypothesis; there is no threshold sweep, fitting on forward outcomes, or paid options input. Report the known-evidence subgroup separately so fallback calls cannot hide lack of coverage.

## Calendar and outcomes

Primary 30 and secondary seven **calendar days** after the common research entry, using the first scheduled NYSE session on or after the due date. The bounded calendar is September 15–November 17, 2026. NYSE's published schedule has no scheduled full-day holidays in this interval, so weekdays are sessions. The code refuses dates outside this interval. Source checked September 15: [NYSE holidays and trading hours](https://www.nyse.com/trade/hours-calendars). If an unscheduled closure occurs, record it and handle it as a documented calendar correction before grading affected entries; never silently substitute the next date with a cached price.

Only measure after the target session's 4 PM New York close and a capture containing attributable daily prices. Require stock and SPY entry/target rows from the same provider, preferring FMP then Massive. Reuse the prior research price convention: recover raw-basis entry open using cached open × raw close / adjusted close, and compare with raw target close. Preserve original price evidence. Missing target prices stay missing; do not shift the target date or guess prices. No future-return-magnitude exclusion. Measured research results are written once and never overwritten silently; source/corporate-action concerns must be reported with the original evidence retained.

Preserve the existing bullish correctness definition (positive raw return OR positive two-decimal SPY excess) and bearish signed equivalent. Also report raw directional success, average signed return and excess, retained coverage, and missing/immature counts. Compare the baseline and challenger over the identical eligible cohort, with all-events, bullish, bearish, known-bullish-evidence, and first-per-ticker views. Include descriptive Wilson intervals and date/ticker concentration; these are not independent binomial trials and do not establish 75% expected accuracy.

## Completion and operation

Produce the first final assessment on November 17, including negative or inconclusive findings. Withhold any production promotion. Report missing capture days and stale source data, and do not extend enrollment to obtain a favorable result. Historical coverage repairs are a separately labeled supplement and cannot enter this prospective cohort.

Run from the repository root:

```powershell
$env:PYTHONTZPATH="$PWD/backend/.venv/Lib/site-packages/tzdata/zoneinfo"
C:/Python314/python.exe backend/scripts/research/forward_bullish_study.py --capture --evaluate
```

This invokes the existing read-only Fly export transport; authentication/network permission may be required. All artifacts are under `frontend/test-results/confirmation-research/forward-bullish-2026-09-15/`. Check `captures/`, `baseline.json`, and `evaluations/` to verify actual collection. A frozen plan alone is not proof of collection. Never send email under this protocol until the separate recipient/content approval is explicitly obtained and the scheduled task is updated.

Setup amendment before any successful capture or prediction: the first export failed because `price_cache.date` is text and its upper-bound parameter was a date. The exporter now binds an ISO date string for that column. The failed initial `frozen-plan.json` and error log are retained. The operative source freeze is `frozen-plan-v2.json`; the hypothesis, cohort rules, thresholds, and grading did not change.
