# Outcome Ledger V2 Backtest Report

Generated after deploying SPY-relative public grading on August 21, 2026.

## Current Grading

Public directional accuracy now uses directional excess return versus SPY:

- Bullish is correct when ticker return minus SPY return is positive.
- Bearish is correct when ticker return minus SPY return is negative.
- Raw up/down correctness remains available as `raw_directionally_correct`.
- Mixed and neutral are excluded from directional scoring.
- Missing ticker or SPY prices are excluded from scored accuracy.

## Production Baseline

### 7D

- Sample: 2,982 benchmarked directional events
- Accuracy versus SPY: 51.5%
- Average directional return: +0.46%
- Average directional excess vs SPY: +0.45%
- Bearish accuracy: 53.9%
- Bullish accuracy: 50.6%

7D is useful as an early diagnostic, but current v1-era rows do not show enough predictive separation for public trust claims.

### 30D

- Sample: 36 benchmarked directional events
- Accuracy versus SPY: 58.3%
- Average directional return: +7.59%
- Average directional excess vs SPY: +6.35%
- Bearish accuracy: 69.2% on 13 calls
- Bullish accuracy: 52.2% on 23 calls

This sample is too small for public claims or reliable model selection.

## 30D Low-Sample Signals

These are directional hints only, not production tuning proof:

- Congress present: 66.7% on 27 events.
- Insider activity present: 64.7% on 17 events.
- Signals present: 70.0% on 10 events.
- Price/volume present: 50.0% on 12 events.
- Fundamentals present: 25.0% on 8 events.
- `congress+insiders+price_volume+signals`: 83.3% on 6 events.
- `congress`: 70.0% on 10 events.

No 30D component cohort currently clears the minimum 100-event standard.

## 7D Component Diagnostics

The larger 7D sample shows broad v1 score bands and most component-presence rules clustering near coin-flip accuracy:

- Government contracts present: 58.0% on 69 events.
- Fundamentals present: 53.1% on 1,289 events.
- Signals present: 52.4% on 955 events.
- Insiders present: 52.1% on 1,661 events.
- Congress present: 51.7% on 1,230 events.
- Price/volume present: 51.4% on 2,399 events.
- Analysts present: 51.5% on 1,099 events.
- Institutional activity present: 50.6% on 2,036 events.
- Macro positioning present: 49.9% on 1,383 events.

High score alone does not currently create a high-confidence public call at 7D.

## Current Conclusion

We have not yet proven a confirmation-v2 model capable of 70-75% 30D SPY-relative accuracy at credible sample size. The right move is:

- Keep v2 more conservative than v1.
- Keep ticker pages simple with the existing 30D Confirmation Score surface.
- Continue capturing live v2 events.
- Treat 7D as a fast diagnostic, not the optimization target.
- Re-run component analysis once there are at least 100 matured 30D v2 events.

Do not market a 75% public accuracy claim from the current dataset.
