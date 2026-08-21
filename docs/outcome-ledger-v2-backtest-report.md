# Outcome Ledger V2 Backtest Report

Generated from production after the raw-or-SPY public grading correction on August 21, 2026.

## Current Public Grading

- Bullish is correct when the ticker return is positive, or when it outperforms SPY over the horizon.
- Bearish is correct when the ticker return is negative, or when it underperforms SPY over the horizon.
- Raw up/down correctness and SPY-relative correctness are both preserved as diagnostics.
- Mixed and neutral are watch states. They do not open, close, or grade directional outcome events.
- Direct bullish-to-bearish or bearish-to-bullish flips close the old event.
- Missing entry or target prices are excluded from scored accuracy.

## Clean 30D Training Set

The new read-only report builder projects a clean 30D training set before measuring anything:

- Keeps only directional bullish/bearish events.
- Keeps the latest same-day directional event per ticker, methodology, and calculation type.
- Excludes rows closed before the 30D horizon.
- Excludes mixed/neutral rows from scoring.
- Excludes missing reference price and missing horizon price rows.
- Separates real source payloads from placeholder backfill payloads.

Production run:

- Clean matured 30D events: 36
- Data correction rows: 7
- Historical reconstruction rows: 29
- Real component/source payload rows: 7
- Placeholder backfill payload rows: 29

Excluded rows:

- Pending 30D horizon: 8,235
- Closed before 30D horizon: 636
- Missing horizon price: 13
- Missing reference price: 2

## Baseline 30D Results

- Accuracy: 72.2%
- Raw accuracy: 72.2%
- SPY-relative accuracy: 58.3%
- Average directional return: +7.59%
- Average excess vs SPY: +6.35%
- Bullish accuracy: 73.9% on 23 calls
- Bearish accuracy: 69.2% on 13 calls

Score bands:

| Band | Sample | Accuracy | Avg directional return | Avg excess vs SPY |
| --- | ---: | ---: | ---: | ---: |
| 0-39 | 14 | 71.4% | +12.75% | +11.68% |
| 40-59 | 8 | 50.0% | -6.41% | -7.47% |
| 60-64 | 3 | 100.0% | +11.67% | +9.64% |
| 65-69 | 4 | 75.0% | +7.53% | +4.51% |
| 70-74 | 6 | 100.0% | +14.10% | +13.08% |
| 75-79 | 1 | 0.0% | -3.50% | -0.65% |
| 80+ | 0 | n/a | n/a | n/a |

This is directionally encouraging, but it is not enough for a public accuracy claim. The sample is only 36, and most component details are not real point-in-time payloads.

## Component Measurement

Only 7 cleaned 30D events currently have real source payloads, so component-level weight changes are not statistically defensible.

Real-payload component samples:

| Component | Sample | Accuracy | Avg excess vs SPY | Action |
| --- | ---: | ---: | ---: | --- |
| Fundamentals | 7 | 42.9% | -6.74% | Observe more |
| Price / Volume | 5 | 40.0% | -10.84% | Observe more |
| Macro Positioning | 4 | 25.0% | -5.20% | Observe more |
| Congress | 1 | 0.0% | -1.12% | Observe more |
| Insiders | 1 | 0.0% | -21.12% | Observe more |
| Signals | 1 | 0.0% | -21.12% | Observe more |
| Analysts | 0 | n/a | n/a | Insufficient data |
| Government Contracts | 0 | n/a | n/a | Insufficient data |
| Institutional Activity | 0 | n/a | n/a | Insufficient data |
| Options Flow | 0 | n/a | n/a | Insufficient data |

Source combination examples from real payload rows are also too small to tune from. The largest real-payload source combination is `price_volume+fundamentals` with only 3 events.

New live Outcome snapshots now persist a `__v2_features` block with source agreement, source freshness, score-change, SPY regime, ticker-relative regime, and sector-proxy regime context. Older rows still show these buckets as unavailable, so v2 calibration should only trust them after enough newly captured 30D events mature.

## Candidate V2 Threshold Tests

These candidate rules use the 36 clean matured 30D events. None reaches the 100-event minimum.

| Rule | Kept | Rejected | Coverage | Accuracy | Avg excess vs SPY |
| --- | ---: | ---: | ---: | ---: | ---: |
| score >= 60 | 14 | 22 | 38.9% | 85.7% | +8.91% |
| score >= 65 | 11 | 25 | 30.6% | 81.8% | +8.72% |
| score >= 70 | 7 | 29 | 19.4% | 85.7% | +11.12% |
| score >= 60 and sources >= 3 | 8 | 28 | 22.2% | 87.5% | +8.95% |
| score >= 70 and sources >= 3 | 3 | 33 | 8.3% | 100.0% | +16.73% |
| bullish >= 70, bearish >= 80 | 3 | 33 | 8.3% | 100.0% | +6.55% |
| sources >= 2 | 23 | 13 | 63.9% | 73.9% | +3.37% |
| short-horizon source absent | 22 | 14 | 61.1% | 72.7% | +9.46% |

Top failure modes among higher-coverage candidates:

- `score >= 60`: bullish 65-69 and bearish 75-79 failures.
- `score >= 65`: bullish 65-69 and bearish 75-79 failures.
- `sources >= 2`: bullish 40-59, bullish 65-69, bearish 40-59, bearish 75-79 failures.

## Decision

Status: do not ship calibrated v2 weights yet.

Reason: no candidate rule reached 70%+ 30D accuracy at the required 100-event minimum. Some threshold rules are promising, but they keep only 3 to 14 calls. That is not enough coverage to trust, and it would be easy to fool ourselves.

## Next Required Work

- Continue live capture until there are at least 100 clean matured 30D events with real source payloads.
- Stop using placeholder backfill rows for component-weight tuning.
- Continue capturing point-in-time score deltas so score-change-over-time can be tested once enough new rows mature.
- Continue capturing SPY and sector-regime features at event open so regime-conditioned performance can be tested once enough new rows mature.
- Re-run this report after every daily cache/horizon update.
- Only promote v2 weights after they improve 30D accuracy and excess return at credible coverage.
