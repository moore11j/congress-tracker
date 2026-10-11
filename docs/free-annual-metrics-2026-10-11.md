# Free annual EBITDA growth and beta preparation, October 11, 2026

## Scope and current state

The free financial adapter now implements annual EBITDA growth from the two latest consecutive observations in Finnhub's annual EBITDA series, plus beta for the exact requested security. Public fundamentals still select FMP. This is preparation for compatibility review, not a public cutover or a claim of numerical equivalence.

Annual growth preserves both original inputs, dates, formula, provider observation and source payload hash. It uses (current - prior) / abs(prior) * 100; absent or zero prior values do not become zero growth. Consecutive fiscal periods allow 350-380 days, including 53-week years. Future, conflicting, malformed or stale data is held. Optional invalid annual data does not discard otherwise valid ratios. A newer complete SEC fiscal year prevents using an older annual growth observation; ordinary SEC quarterly periods do not silently relabel annual growth as TTM.

Beta is a current provider observation. Its benchmark, lookback and return frequency are not supplied by this endpoint, so those properties are not inferred. Reviewed alternate issuer securities can share consolidated annual growth with independently verified SEC CIK, but cannot share beta, prices or valuation/per-share fields. Existing SEC values remain authoritative where present. Version v2 prevents old preparation caches being counted as new coverage; existing collection budgets and schedules remain unchanged.

## Actual-source compatibility evidence

Saved-source replay `annual-metrics-source-replay-oct11.json` completed successfully across ten requested symbols using three original Finnhub response files. All existing normalized values and field evidence remain unchanged. Actual saved SEC source hashes were checked; composite cache and screener values agree and repeated cache writes retain their timestamp. Seven compatible SEC composite cases were exercised; SGRP/SPY lack saved SEC composites and TSM's saved projector reports unsupported current revenue. No provider requests, production writes or emails occurred.

| Symbol | Finnhub annual growth % | Cached FMP growth % |
|---|---:|---:|
| AAPL | 7.4907 | 7.0385 |
| MSFT | 19.6954 | 29.5658 |
| NVDA | 59.9073 | 67.8164 |
| SGRP | -777.7186 | -7.1013 |
| BRK-B | -15.1609 | 0.0899 |
| CX | -39.0137 | -8.4342 |
| TSM | 46.4399 | 38.6746 |

These differences are material. The FMP comparison is a read-only provider-filtered production cache observation at 23:07 UTC, not a contemporaneous raw FMP financial-statement reconciliation. It must not be called a parity pass. `annual-ebitda-sec-comparison-oct11.json` verifies both annual Finnhub inputs for AAPL and NVDA equal filed operating income plus depreciation/depletion/amortization, after scaling the observed series values by one million. That is evidence for those two issuers only, not a general currency/unit rule or proof of the FMP definition. FMP's current adapter also uses magnitude-based percent normalization; SGRP's apparent scaling difference needs raw-source investigation before public activation. Do not change recorded public metrics based solely on this diagnostic.

## Remaining release and cutover gates

Focused and expanded tests, review, deployment and live v2 preparation are tracked below. Core selection stays FMP; no source switch is included. Annual definition and raw-FMP outlier reconciliation, net debt/EBITDA, earlier earnings releases, full coverage, score/ranking rebaseline, monitoring/watchlist/daily/weekly consumers, sustained capacity and final Massive validation remain open. No new paid subscription is established by the adapter's former field omissions.

## Pre-release validation

111 focused/expanded checks pass on Python 3.14.2 in 197.93 seconds: metrics, issuer identity, invalid/overflow annual data, v1 cache rejection, SEC warming, provider baselines, snapshots and confirmation consistency. Initial 61 narrower checks also passed. Actual source replay handle94279 is terminal/pass. PR589 is under automatic review; no deployment yet. Existing dependency audit failures are not changed by this package.
