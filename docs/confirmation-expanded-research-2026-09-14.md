# Expanded confirmation research: prices, market conditions, and positioning

Research date: September 14, 2026. No production weights, classifications, outcome records, entry prices, cache behavior, or grading rules were changed. All alternative predictions are local research calculations against immutable labels.

## Broader historical evidence

The read-only database audit found 1.7 million cached price rows, with broad coverage beginning in 2023 and a smaller universe extending to 2013. Historical fundamentals were unsuitable for a strict point-in-time study: 1,811 older snapshots explicitly use statement-period dates plus 45 days as a proxy, without filing acceptance timestamps. Another 885 snapshots begin August 2026. Those proxies were not included as historical facts known at the time.

The price experiment sampled 256 symbols by a fixed hash from 931 symbols with at least 180 attributed OHLC observations in 2023, and added SPY/QQQ. The export contains 253,677 rows. Selection used coverage, not outcome performance, but current-cache availability and survival remain selection biases. This is a separate historical opportunity panel, not reconstructed public confirmation events.

Features include trailing momentum, acceleration, realized volatility, drawdown, moving-average distance, RSI, volume ratios, relative strength, beta, residual momentum, SPY/QQQ conditions, and interactions: 61 features in total. Every feature ends before the hypothetical next-session entry. Cached opens are on the adjusted-close basis; the research converts them by `raw_close / adjusted_close` before comparing to raw closes. It does not apply split factors a second time.

Training has 14,905 opportunities ending November 2024; validation has 5,069 from January–May 2025. June validation entries are purged so all training/selection outcomes mature before the next period. The subsequent test has 12,677 opportunities over 56 weekly dates, July 2025–July 2026. July 2026 is severely undercovered (only eight observations) because this first panel required FMP-attributed prices throughout; most test evidence is July 2025–June 2026. The actual-ledger transfer separately combines attributed FMP history and Massive split-adjusted rows to fill provider-transition gaps.

Three logistic and three constrained gradient-boosting configurations were trained separately for bullish and bearish correctness. Validation chose one bullish gate, one full-coverage directional policy, and one selective directional policy. Selection files were saved before test predictions were evaluated. The existing absolute-return OR SPY-excess-return definition was preserved; 7D was evaluated separately.

| Fixed policy | Historical test 30D | Coverage | 7D |
|---|---:|---:|---:|
| Always bullish | 51.27% | 100% | 52.06% |
| Always bearish | 58.08% | 100% | 57.84% |
| Stock above/below 200-session mean | 56.72% | 100% | 56.24% |
| Market above/below 200-session mean | 51.56% | 100% | 51.42% |
| Learned bullish qualification | 59.32% | 29.11% | 60.46% |
| Learned direction | 58.29% | 100% | 59.10% |
| Learned selective direction | 58.87% | 77.87% | 58.77% |

The bullish gate improved precision in this proxy panel, but coverage fell from 61% in validation to 29% in test, failing the prespecified 50% minimum. The directional policy barely exceeded always bearish and called 11,634 of 12,677 opportunities bearish. These are not sufficient improvements to promote.

Repeated opportunities for the same security and overlapping 30D windows are correlated. The output includes date-cluster bootstrap intervals, which account for shared entry-date exposure but do not fully account for serial overlap; event-level Wilson intervals are too optimistic as a measure of independent evidence. There are 207 research-panel 30D returns above 100% in absolute magnitude. These were retained, not removed because they hurt results; some may require independent corporate-action verification. Return averages are not portfolio returns and do not include execution costs.

## Transfer to the actual immutable ledger

After filling provider gaps, complete 253-session trailing features were available for 748 of 3,075 actual events, including 385 completed 30D outcomes. Missing features retain the original classification in whole-ledger simulations. Models and thresholds were frozen using the earlier historical validation period; they were not refit to the actual ledger outcomes.

| Policy on the whole ledger, original fallback where needed | 30D accuracy | Retained events | Bullish 30D | 7D accuracy |
|---|---:|---:|---:|---:|
| Published original calls | 47.02% | 1,176 | 39.98% | 51.09% |
| Historical bullish gate; preserve original bearish calls | 47.19% | 1,102 | 39.51% | 50.70% |
| Historical directional model | 45.58% | 1,176 | 39.93% | 51.85% |
| Historical selective model | 45.78% | 1,077 | 39.49% | 50.96% |

On the 385 covered 30D events, the original accuracy is 45.19%. The bullish gate yields 45.34% while retaining 311; bullish accuracy falls from 41.08% to 40%. The learned directional model calls all 385 bullish and reaches 40.78%. That reversal from mostly bearish predictions in the historical test demonstrates a strong regime-dependent behavior, not a robust forward improvement. The historical price-only candidates are rejected.

## Raw COT follow-up

The next experiment uses official annual CFTC financial-futures archives for 2013–2026, downloaded only to local research files. Inputs distinguish dealers, asset managers, and leveraged money for S&P, Nasdaq, Russell, dollar, and Treasury futures. Features use net positioning divided by open interest, four- and thirteen-week changes, trailing crowding percentiles, and relative-strength interactions.

Availability uses a conservative seven-calendar-day lag and withholds known extended reporting backlogs until after catch-up. This matters: some 2025 reports were released weeks after their report dates. The archives can also contain later corrections, so this remains reconstructed research, not a fully vintage-audited dataset. [CFTC historical announcements and release delays](https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalSpecialAnnouncements/index.htm).

The historical test period has already been inspected during the price experiment. This follow-up is explicitly exploratory; it cannot turn the same period into a new untouched final test. Candidates are still selected on the earlier validation period and then applied without ledger-specific threshold tuning.

Twelve additional configurations were evaluated: the same six model settings using COT alone and price plus COT. Their selected results were:

| Policy | Historical 30D / coverage | Actual-ledger 30D / coverage | Actual-ledger 7D |
|---|---:|---:|---:|
| Price+COT bullish gate; preserve original bears | 61.63% / 20.04% | 47.99% / 84.69% | 50.63% |
| COT directional model | 58.30% / 100% | 64.46% / 100% | 58.25% |

The COT directional model chose bearish for every actual ledger event. Its 758/1,176 correct 30D forecasts exactly equal the always-bearish counterfactual baseline. On the longer historical test it was only 0.22 percentage points better than always bearish (58.30% versus 58.08%), and 7D was worse (57.39% versus 57.84%). Its apparent +17.44-point improvement over the ledger's original 47.02% is therefore not demonstrated stock-selection skill. Calling this a solved confirmation model would be misleading.

The price+COT gate again failed to improve actual bullish accuracy: 39.49% versus the original 39.98%. It also discarded 180 of 1,176 completed 30D calls. A final fixed-model diagnostic ranked bullish probabilities within each entry-date batch and retained the top half, avoiding threshold-driven coverage collapse. On the historical proxy it retained 6,356 opportunities (50.14%) and scored 56.84% at 30D, compared with the always-bullish 51.27%; 7D was 55.76%. But transferred to the actual ledger with original bearish calls and missing-feature fallbacks preserved, it reached only 47.94% overall, 39.72% bullish, and 50.63% at 7D. That also fails the requested bullish improvement. The batch-ranking diagnostic would require a contemporaneously defined opportunity universe before it could be used prospectively.

## Stock-specific disclosure extension

A further read-only export covered 215,019 Congress/insider records. Strict purchase/sale and date checks retained 99,337 rows and excluded 115,682 non-market or invalid-date records. These were collapsed to 33,922 distinct symbol/source/side/filing-date combinations, so duplicated records and multiple transactions on one filing day cannot inflate the count. This measures distinct disclosure days, not unique buyers or institutions.

Features use filing dates rather than transaction timestamps. Without an exact publication time, same-day disclosures are withheld until the following day. Awards, exercises, gifts, and other non-market classifications do not count as purchases. Inputs add buying/selling disclosure-day counts over 30/90/365 days, their age, and buy-minus-sell differences. The six earlier model settings were compared with these features added to prices and COT; selection again used only the historical validation period.

| Disclosure-augmented policy | Historical 30D / coverage | Actual-ledger 30D / coverage | Actual bullish 30D | Actual 7D |
|---|---:|---:|---:|---:|
| Bullish qualification gate | 59.01% / 25.08% | 47.69% / 88.27% | 39.55% | 50.86% |
| Full-coverage direction | 57.80% / 100% | 47.96% / 100% | 39.10% | 51.29% |
| Retain top half of bullish candidates | 57.02% / 50.14% | 47.84% / 86.73% | 39.57% | 50.49% |

The directional version produces a small +0.94-point 30D and +0.20-point 7D whole-ledger change, but bullish precision worsens and the longer historical result is below the always-bearish benchmark. There is no basis to treat those small changes as a robust improvement. The stock-specific extension is also not recommended for deployment.

## Finding and decision

This extension tested 24 model configurations across price, raw-COT, and dated-disclosure inputs beyond the earlier frozen-score study, multiple decision policies, and a coverage-stable ranking diagnostic. It found no model that credibly supports >75% 30D accuracy or a robust improvement to the actual bullish confirmation cohort. The best apparent total-ledger result is 64.46% from an all-bearish policy; its longer-period benchmark comparison does not justify promotion. Historical bullish ranking has a modest signal in the separate proxy universe, but it does not transfer successfully to the present ledger.

Keep the current scoring unchanged. The remaining evidence gap is richer contemporaneous stock-specific information—filing content and participant-level changes, genuine point-in-time fundamentals, and source revisions—plus additional independent 30D confirmation cohorts. Basic disclosure-day counts did not fill that gap. The current completed 30D ledger still spans only six opening sessions, so repeated experiments on it cannot create independent validation. This research does not establish that 75% is impossible; it establishes that these tested candidates do not substantiate it. No improvement is claimed for 90D, 180D, or 365D.

## Reproducibility and controls

The protocol is in `docs/confirmation-expanded-research-protocol-2026-09-14.md`. All scripts are under `backend/scripts/research/`; ignored local datasets/results are in `frontend/test-results/confirmation-research/`. The frozen ledger SHA-256 remains `de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b`. Read-only database transactions were enforced during exports; no provider hydration, application writes, or model registration was used. Nineteen research invariant tests cover no intraday future closes, immutable inputs, preserved grading, price-basis conversion, temporal purging, COT release delays, outcome-independent ranking, and disclosure availability/type filtering.

Chronological validation is necessary because ordinary random splits can leak dependence between time-correlated observations. [Scikit-learn time-series validation guidance](https://scikit-learn.org/dev/modules/generated/sklearn.model_selection.TimeSeriesSplit.html).
