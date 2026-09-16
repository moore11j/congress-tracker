# Confirmation research: Congress dollars and insider conviction

## Finding

The strongest narrow lead is an insider buying more than twice their own prior typical amount. Under the existing ledger grading, **6 of 8 matching bullish events were correct at 30D (75%)**. That is only 0.99% of the 808 completed bullish events. Its event-level 95% Wilson interval is **40.93–92.85%**, and the entries span just three sessions. The older historical check reaches **60.98% on 41 weekly opportunities across seven tickers**, not 75%. This is a research lead, not evidence of a dependable 75% model.

Broad dollar weighting, buyer counts, executive purchases, and ownership-increase features did not fix the actual bullish cohort. No live score, public event, original return, entry price, grading rule, or cache was changed.

## What was tested

This study adds information that the earlier disclosure-frequency experiments did not capture:

- Congress buy and sell dollar ranges separately, conservative net buying, independent participants, filing delays, and prior member performance.
- Insider disclosed transaction dollars, independent buyers, executive participation, purchases relative to that same buyer's prior buying, approximate ownership increases, new positions, and available plan flags.
- Amounts use log scales without the production source summary's $1 million saturation. Congress's conservative net lower bound is purchase lower bounds minus sale upper bounds; bounds are not represented as exact transaction dollars.
- Unusually large purchases require at least three earlier purchase filings for that actor and ticker within three years, and compare against their median dollar amount. The current filing and future filings cannot enter that median. This is a size-surprise proxy, not the academic definition of an opportunistic insider.
- Member performance uses only disclosure-following 30D outcomes matured before the decision date, with at least five observations and shrinkage toward neutral. It never substitutes today's member leaderboard for historical information. Price coverage allowed 2,859 such past purchase measurements; this is incomplete member history.

All date-only disclosures become usable the following day. Awards, exercises, and other non-market transactions do not count as purchases. Known options, bonds, crypto, and other unsupported Congress security types were excluded; stock/REIT/ETF purchases were retained. Known insider preferred shares, warrants, options, and convertible notes were excluded. Beneficial ownership and transaction types require care because Forms 3/4/5 cover more than straightforward common-stock market buying. [SEC investor guidance](https://www.investor.gov/introduction-investing/general-resources/news-alerts/alerts-bulletins/investor-bulletins-69).

Eighteen learned configurations compared conviction features alone, price/COT plus disclosure counts as a control, and price/COT plus conviction. Six model settings per family used regularized logistic regression or constrained gradient boosting. Within each family, historical validation selected a probability gate and a ranking gate. Both were required to retain at least half of eligible bullish opportunities during selection. Gates preserve every original bearish call; missing features or no observed trade activity retain original classifications. Ten fixed source rules were also reported, including unsuccessful ones.

## Data audit

The export contains 99,349 display-event records and 60,418 normalized market-trade records. Deduplication matched 59,169 events to their normalized equivalents and removed 13,382 duplicate display records. Security-type checks excluded 1,192 Congress records and 695 normalized insider records. The resulting 84,704 distinct trade signatures were aggregated into 43,874 actor/ticker/filing/side groups, including 9,664 Congress purchase groups and 4,073 insider purchase groups.

After the price audit, 3,950 insider purchase groups have known dollar amounts; 3,596 have a computable ownership-increase proxy and 900 have sufficient prior buying history for a size-surprise ratio. Ownership calculations are approximate: complex indirect accounts and filing footnotes can require further reconciliation. A false or absent plan flag is not proof that a transaction was discretionary.

The audit found a concrete bad-looking NMM input: 1,131 shares at **$748,119 per share**, producing **$846,122,589** of supposed purchase value. Two nearby transactions in the same filing have prices of $79.019 and $77.6356. The research did not guess a corrected decimal or edit the production record. A general same-filing sanity check marked five extreme transaction prices as unknown when they differed by more than 10× from a consistent peer-price cluster over at most seven days. Their transaction presence remains available, but unverified dollars cannot manufacture a large-buy signal.

Initial research artifacts are preserved with `.initial.json` suffixes. Before this check, the unusual-buy subgroup appeared to be 7/9 correct; after the suspect NMM value stopped qualifying it, the result is 6/8. The same model procedure was rerun after the input quarantine. Published NMM outcomes and all other ledger records remain unchanged.

## Fixed-rule results on the actual bullish ledger

All rows below use the immutable completed 30D labels and the existing absolute-return OR SPY-excess-return correctness definition. These are overlapping, exploratory subsets, not independent experiments or recommendations.

| Rule | Correct / matching events | 30D accuracy | Fraction of 808 bullish events |
|---|---:|---:|---:|
| Original bullish cohort | 323 / 808 | 39.98% | 100% |
| Congress conservative net buying, past 30 days | 31 / 75 | 41.33% | 9.28% |
| Congress net buying above $100k, past 30 days | 0 / 0 | Unmeasured | 0% |
| At least two Congress buyers and positive net buying, 30 days | 7 / 14 | 50.00% | 1.73% |
| Prior member accuracy at least 60%, with positive Congress net buying, 90 days | 11 / 18 | 61.11% | 2.23% |
| Insider purchase at least $100k and positive net buying, 30 days | 16 / 33 | 48.48% | 4.08% |
| Insider purchase at least $1m and positive net buying, 90 days | 10 / 23 | 43.48% | 2.85% |
| At least two insider buyers and positive net buying, 30 days | 6 / 14 | 42.86% | 1.73% |
| Insider purchase at least twice usual size and positive net buying, 90 days | **6 / 8** | **75.00%** | **0.99%** |
| Stock has an ownership increase at least 10% and a purchase at least $100k, positive net buying, 90 days | 12 / 36 | 33.33% | 4.46% |
| CEO/CFO purchases totaling at least $100k and positive net buying, 30 days | 4 / 9 | 44.44% | 1.11% |

The eight unusual-buy tickers are GOTU, STIM, PFBX, COE, MXF, GF, KRNY, and ACOG. Five of eight had positive raw stock returns (62.5%); the sixth correct call beat SPY despite a negative absolute return. The original grading was preserved rather than rewritten to improve results. These include 10% holders and funds, not exclusively company executives.

The initial ownership rule used stock-level maxima, which can refer to different buyers. A stricter case audit requiring the same buyer's purchase to meet both the 10% ownership-increase and $100k amount conditions yields 11/33 correct, also 33.33%. That refinement does not change the conclusion. The case-level audit is reproducible with `audit_conviction_cases.py`.

The lack of $100k conservative Congress net-buy matches does not mean large Congressional transactions never occur. In the specific archive window relevant to these opening dates, lower-bound purchases were modest and sale upper bounds can make the conservative net bound negative. Different range assumptions would be a separate sensitivity experiment; none were chosen after seeing which would win.

## Does the narrow lead repeat?

| Unusual insider purchase rule | Correctness | Observations | Distinct tickers |
|---|---:|---:|---:|
| Earlier validation period | 75.00% | 12 | 3 |
| Subsequent historical period | 60.98% | 41 | 7 |
| Actual 30D bullish ledger | 75.00% | 8 | 8 |
| Actual 7D bullish ledger | 58.33% | 12 | 12 |

Historical weekly opportunities can repeatedly reference the same filing and overlap in their return windows, so 41 observations do not represent 41 independent insider signals. The broader active historical cohort's always-bullish 30D accuracy is 54.87%. The rule is directionally interesting against that baseline but has insufficient independent evidence for promotion. It requires forward evaluation with a fixed definition; 75% should not be advertised as its expected accuracy.

The member-track-record subgroup did not repeat convincingly: its earlier validation accuracy was 46.43% on 84 opportunities, and later historical accuracy was 53.17% on 126. Its actual-ledger 61.11% on 18 events is too weak and unstable to justify promoting selected members' trades broadly. The available data do not establish that copy-Congress portfolio strategies lack value; their objectives, exposure, holding periods, and return concentration differ from this ledger's fixed-horizon hit rate.

## Learned weighting results

The active historical source cohort has 1,928 training opportunities, 888 validation opportunities, and 3,751 later test opportunities. Training ends in November 2024; validation labels mature before July 2025. The existing historical test period and ledger have already been inspected in earlier experiments. These are exploratory comparisons, not newly untouched validation sets. Archive coverage is uneven, especially for insider purchases before 2026, and only 900 purchase groups have sufficient recorded history to estimate unusual size.

| Whole-ledger policy | 30D accuracy | Retained 30D events | Bullish 30D | 7D accuracy |
|---|---:|---:|---:|---:|
| Original | 47.02% | 1,176 | 39.98% | 51.09% |
| Conviction-only probability gate | 47.53% | 1,054 | 39.50% | 50.64% |
| Conviction-only ranking gate | 49.09% | 880 | 39.45% | 51.18% |
| Price/COT + counts control, probability gate | 48.24% | 964 | 39.43% | 50.70% |
| Price/COT + counts control, ranking gate | 47.54% | 1,035 | 39.28% | 51.12% |
| Price/COT + conviction, probability gate | 48.00% | 977 | 39.24% | 50.56% |
| Price/COT + conviction, ranking gate | 47.34% | 1,035 | 38.98% | 50.80% |

The largest headline increase, 47.02% to 49.09%, comes from dropping bullish opportunities while retaining all bearish calls. Its retained bullish accuracy is slightly worse than originally. That is a change in directional mix, not demonstrated improvement in bullish selection.

In the older historical test, the conviction-only models reached roughly 54%, versus 54.87% always bullish. Adding conviction to price/COT with a 50% ranking quota yielded 56.87%, versus 56.34% for the count-based control. The probability gate retained only 24.53% in that later period, failing the coverage requirement. None supports a broad production weight increase.

## Decision and reproducibility

Keep live scoring unchanged. Continue treating **purchase size relative to the same buyer's history** as the most useful narrow hypothesis from this round. Generic million-dollar thresholds, larger ownership changes, and blanket Congress weighting are not supported by these results. Any prospective use needs verified input prices, a fixed definition, more independent purchase events, and monitoring of coverage as well as accuracy. No 90D/180D/365D improvement has been established.

The protocol is `docs/confirmation-conviction-protocol-2026-09-14.md`. Code is in `backend/scripts/research/`, principally `conviction_features.py`, `analyze_conviction_models.py`, and `test_conviction_research.py`. Ignored local files include `conviction-input.json`, `conviction-security-types.json`, `conviction-trades.json`, `conviction-results.json`, `conviction-predictions.json`, and `conviction-manifest.json`.

All 26 research tests passed. They cover deduplication, dollar-versus-count direction, disclosure availability, prior-only purchase baselines, matured-only member histories, security-type filtering, suspect-price handling, and the earlier grading/immutability checks. The frozen ledger SHA-256 remains `de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b`. Database exports used read-only transactions; no production write, model registration, deployment, or retrospective outcome recalculation occurred.
