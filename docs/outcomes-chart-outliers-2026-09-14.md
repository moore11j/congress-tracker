# Outcome chart outliers

The scatter previously scaled both sides of its Y axis to the largest absolute return in the selected events. GOSS at -8,051.16% flattened the 7D view; TJGC at -196.06% dominated 30D.

The scatter now omits returns outside [-100%, +100%] before calculating its axes and rendering points. The rule is symmetric, applies to measured and provisional returns at every horizon, and does not depend on ticker. Values are never clamped to the boundary. The chart reports how many points are omitted, and an all-omitted selection directs users to the table. Ledger rows, detail views, CSV exports, and summary metrics are unchanged. Axis ticks have more space beside the vertical title.

## Read-only price audit

Public snapshot 22574, GOSS, bearish, opened August 28 at $0.172. Its stored September 4 close is $14.02, producing the erroneous -8,051.1628% directional return. On September 14, the provider's same-date history reports the August 28 open as $13.76, exactly $0.172 multiplied by 80. The issuer [announced its 1-for-80 reverse split effective for September 11 trading](https://ir.gossamerbio.com/news-releases/news-release-details/gossamer-bio-announces-effectiveness-1-80-reverse-stock-split/). Comparing the historical open and close on the same current basis gives `(14.02 / 13.76 - 1) * -100 = -1.8895%`.

This confirms an immutable entry/current provider price-basis mismatch, not a real 8,051% move. Both horizon endpoints predate the split, but the provider has retrospectively rebased its historical OHLC. Its `historical-price-eod/full` values arrive through our code marked `raw` despite that rebasing. A future split correction must preserve the actual opening quote and audit history, reconcile both endpoints to one explicit basis, and avoid applying the split a second time to already rebased provider bars. Simply fetching split actions only between entry and target misses actions after the target that rebase history. This presentation change does not repair that stored observation or its contribution to summary averages.

Public snapshot 2649, TJGC, bearish, opened August 7 at $3.55. The provider independently confirms that open and the September 8 close of $10.51, with no split in that interval. The stock rose 196.0563%; the bearish directional return is therefore -196.0563%. This is a valid directional result, rather than a stock falling below zero.

No production price, entry, observation, or summary was mutated during this investigation.

## Validation

21 focused frontend tests pass, including executed chart selection and server-rendered scatter tests covering extreme values on both sides, unchanged input objects, measured and provisional dots, axis rescaling, and all-omitted selections. TypeScript verification (`tsc --noEmit`) also passes.
