# Unusual insider purchases: disclosure-triggered results

The broader check does **not validate the earlier 75% result**. The fixed unusual-purchase rule achieved **60.0% 30D accuracy (21/35)** and **64.1% 7D accuracy (25/39)** after limiting repeated ticker signals. The ordinary-purchase comparison achieved 50.6% and 53.8%, respectively. This is a possible narrow lead, with insufficient evidence to change live confirmation weights or promise a 75% hit rate.

These are separate research entries following disclosure, not replacements for public ledger events. The public ledger, scoring, entry prices, and completed outcomes remain unchanged. The last available price session is September 11, 2026.

## Fixed experiment

An unusual purchase must be at least twice the median dollar size of the same buyer's previous purchases in the same ticker, with at least three earlier known purchases within three years. The ticker must also have positive, fully known net insider buying over 90 days. We retained the earlier duplicate, security-type, and suspect-price safeguards, with no threshold sweep.

The research entry is the first SPY trading session after the filing date. Date-only disclosures are not used on the same day. Stock and benchmark prices must be from the same provider and use consistent entry/exit price bases. The existing bullish grading rule is preserved: positive stock return OR positive SPY-relative return. That means accuracy is not identical to the percentage of profitable trades.

The primary sample allows one qualifying signal per ticker every 90 calendar days. This limit is applied before looking at prices or results: a missing-price signal still consumes its interval. Ordinary purchases use the same requirements and timing, except that their purchase size is below twice usual. The groups can have different tickers and entry dates; their difference is not a causal estimate or an estimate of whole-ledger improvement.

## Results and sensitivity

| Sampling | Unusual 30D | Ordinary 30D | Unusual 7D | Ordinary 7D |
|---|---:|---:|---:|---:|
| **Primary: 90-day ticker interval** | **21/35 = 60.0%** | **41/81 = 50.6%** | **25/39 = 64.1%** | **49/91 = 53.8%** |
| Every distinct ticker/filing | 38/65 = 58.5% | 153/293 = 52.2% | 45/74 = 60.8% | 179/341 = 52.5% |
| First qualifying filing per ticker only | 16/28 = 57.1% | 31/60 = 51.7% | 20/32 = 62.5% | 40/68 = 58.8% |

The primary unusual 30D sample covers 28 tickers, 29 triggering buyers, and 35 measurements. Twenty of those 35 have positive stock returns; one additional result qualifies by outperforming SPY. Its descriptive Wilson 95% interval is **43.6–74.4%**. Resampling whole ticker groups gives approximately **41.9–75.5%**. These wide intervals do not adjust for the earlier hypothesis search, shared market exposure across different tickers, or incomplete data coverage.

The advantage is less convincing when only the first signal per ticker is counted: 57.1% versus 51.7% at 30D. A favorable small sample should not be promoted as an expected hit rate.

| Filing year, primary sample | Unusual 30D | Ordinary 30D |
|---|---:|---:|
| 2023 | 0/1 = 0.0% | 2/3 = 66.7% |
| 2024 | 5/8 = 62.5% | 7/15 = 46.7% |
| 2025 | 3/4 = 75.0% | 2/6 = 33.3% |
| 2026 | 13/22 = 59.1% | 30/57 = 52.6% |

Earlier years are too thin to establish stability across market conditions. There is no demonstrated 90D, 180D, or 365D improvement.

## Coverage and new evidence

Of 4,073 insider purchase filing groups, 3,173 lack sufficient known prior purchase history or a usable current amount. Before the net-buying condition, 147 unusually large purchase groups span 52 tickers. After net-buying requirements and collapsing same-day actors, there are 103 distinct unusual ticker/filing signals; the primary interval rule retains 54.

Of those 54 primary signals, **35 have measured 30D results, five are overdue but lack consistent prices, and 14 are not yet mature**. The five missing cases are NYC (June 29 filing), COE (June 30), LGHL (July 27), KRNY (July 28), and DLHC (July 31). All remain in the research cohort. Even if all five overdue cases eventually qualify as correct, the current due cohort would reach only 26/40 = 65.0%.

The original exports supported 29 primary unusual 30D results and 56 ordinary results. A read-only export of cached prices for all 116 symbols in both complete cohorts, including SPY, raised coverage to 35 and 81. It changed **zero** previously measured results and preserved exactly the same cohort membership. Initial and supplemented outputs are retained separately. No price-fetching service, cache refresh, or production write was invoked.

Previously unexamined securities—outside both the earlier ledger and historical-panel ticker sets—supply only six measured primary unusual 30D observations across five tickers: four correct, or 66.7%. Their 7D result is five of six. This is far too small to validate the rule, and it shares historical market periods with earlier research. It is not an untouched prospective test.

Other limitations remain: archive completeness varies by year; reported filing dates do not prove contemporaneous app ingestion; historical revisions and survivor coverage are not fully audited. The ordinary comparison has 22 additional due 30D cases with missing prices, which can affect its measured accuracy. Missing or invalid ticker identifiers are retained as unresolved coverage rather than assigned guessed prices.

## Decision and next evidence

Keep the live confirmation model unchanged. Relative purchase size remains a candidate for further fixed-rule research, but the current evidence does not justify a broad insider weight increase. The original six-of-eight ledger result did not generalize to disclosure-triggered entries across more securities and dates.

The 14 immature primary signals are frozen in the local `disclosure-trigger-pending.json` artifact. Their research 30D calendar targets run from September 20 to October 11, with measurements due on the next trading session when necessary. They include TSM; this creates no new public TSM event. This list is a research snapshot, not a scheduled monitor or a production shadow model. A subsequent evaluation should use the same rule and membership, disclose missing results, and avoid retuning after seeing outcomes.

## Reproducibility

Protocol: `docs/confirmation-disclosure-trigger-protocol-2026-09-14.md`. Offline analyzer: `backend/scripts/research/analyze_disclosure_triggers.py`. Read-only supplement exporter: `backend/scripts/research/export_disclosure_trigger_prices.py`. Tests: `backend/scripts/research/test_disclosure_trigger_research.py`.

Ignored local artifacts are under `frontend/test-results/confirmation-research/`: `disclosure-trigger-cohorts.json`, `disclosure-trigger-results.initial.json`, `disclosure-trigger-results.json`, `disclosure-trigger-prices.json`, `disclosure-trigger-supplement-audit.json`, and `disclosure-trigger-pending.json`. The results contain hashes of input data, frozen cohort membership, protocol, and analyzer code.

All 13 conviction and disclosure-trigger tests passed. Checks cover prior-only filings, deduplication, dollar weighting, suspect-price quarantine, sampling independent of outcome availability, next-session entry, missing versus immature results, consistent provider endpoints, and the unchanged SPY-relative grading definition. The frozen ledger SHA-256 remains `de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b`.
