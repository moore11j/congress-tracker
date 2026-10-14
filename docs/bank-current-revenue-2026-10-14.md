# Current SEC bank revenue, October 14, 2026

## Reproduced coverage defect

Fresh SEC companyfacts and submissions for FITB, RF and EWBC reproduce `No current complete trailing revenue period` in the existing core projector. The bank fallback stops whenever any ordinary revenue series exists. In these three cases that series is an obsolete contract-revenue component: FITB ends September 2024, RF December 2021, EWBC September 2020. It blocks current, aligned `InterestIncomeExpenseNet` plus `NoninterestIncome` facts.

The original June 2026 statements confirm the inputs. The selected basis is U.S. GAAP net interest income before credit-loss provisions plus non-interest income, preserving the existing bank-revenue label. Fifth Third's tax-equivalent revenue is a separate non-GAAP measure and is not substituted.

| Issuer | Current six-month net interest income | Current six-month non-interest income | Derived trailing revenue |
| --- | ---: | ---: | ---: |
| [Fifth Third](https://www.sec.gov/Archives/edgar/data/35527/000003552726000202/fitb-20260630.htm) | $4,149 million | $1,954 million | $10,744 million |
| [Regions](https://www.sec.gov/Archives/edgar/data/1281761/000128176126000055/rf-20260630.htm) | $2,525 million | $1,255 million | $7,617 million |
| [East West](https://www.sec.gov/Archives/edgar/data/1069157/000106915726000044/ewbc-20260630.htm) | $1,355.844 million | $209.048 million | $3,101.193 million |

Trailing figures use full fiscal year plus current YTD minus corresponding prior YTD. Each component pair has identical period, currency, accession and filed date. Source hashes and all nine captured files are retained in `artifacts/bank-coverage-oct14/manifest.json`. Web retrieval exceeded its document-size bound; bounded direct downloads retained the originals, each below 8 MB.

## Repair and limits

The bank fallback may replace an obsolete contract-component series only for the existing supported bank SICs, after more than 380 days without a new contract observation, with a complete trailing bank period no older than 180 days. Reported total-revenue concepts, conflicting observations, recent contract series, missing components, mismatched filings, unsupported currencies and nonbanks remain protected. The derived bank series wholly replaces the component series; it never splices the two denominators. Every derived input records the displaced concept and last period.

Only the three reproduced issuers receive immediate statement/ratio cache-version changes. Other eligible banks use ordinary expiry. Existing timeouts, schedules, source budgets, source selectors and hosting remain unchanged. The financial statement reader already displays the bank revenue basis. No industrial operating-income proxy or debt ratio is introduced.

## Validation

The new positive test fails on the baseline with the reproduced coverage error; 25 other selected cases initially pass. Expanded checks exposed a missing-tag defensive guard and an invalid two-year test fixture; both are corrected. The initial Linux invocation named a nonexistent statement test file and ran no tests; the corrected selection uses the actual cache/comparison tests. Final regression result is recorded with release evidence below.

Actual captured-source replay on Python 3.14.2 recovers six prepared metrics for FITB and three each for RF/EWBC. All three trailing revenue totals match the table, repeat identically, and retain period and component provenance. Twenty saved control issuers preserve their entire baseline result or explicit error exactly. Real isolated SQLite ratio and statement preparation writes six preparation records and repeats from cache without additional transport; zero canonical financial/event rows and no email. The harness initially used a nonexistent fetch entry point and was corrected to the actual prepared reader. Receipt: `artifacts/bank-revenue-replay-oct14.json`.

These fields do not establish complete financial scores, score/alert rebaseline, global financial coverage or FMP exit. Public core remains FMP. Current publication and scheduled validation remain pending until documented below.

## Other source and reliability evidence

The existing free Finnhub key successfully reads `stock/financials-reported` for NUE and NVDA, while ASML returns no data. Both U.S. samples have empty balance-sheet arrays and six-month YTD income/cash-flow envelopes; they do not resolve the missing debt/lease inputs. No endpoint is integrated or scheduled on that evidence. Receipt `artifacts/finnhub-reported-probe-oct14.json`, observed 18:02 UTC, contains three bounded responses and no credentials.

At 18:06 UTC, the same two production machines remain in place, API CPU credits are 8,538 with zero current throttling, and news has 42/42 fresh company caches. Source jobs complete their bounded scopes; institutional reference coverage remains explicitly partial. Repeated releases make these samples insufficient for sustained-capacity acceptance. Receipts `preload-receipts-1792001195.json` and `direct-feeds/free-replacements-2026-10-09/cpu-quota-1792001192.json`.

Final affected Linux Python 3.12.3 selection: 136 passed, including bank revenue, SEC fundamentals/preparation, financial cache/comparison/continuity and selected-provider financial alert consumers. This is not a full-suite result.
