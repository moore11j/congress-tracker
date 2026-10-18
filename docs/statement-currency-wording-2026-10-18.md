# Shared currency declaration repair, October 18, 2026

## Problem and behavior

The shared HTML statement reader recognized explicit "translations from" declarations but missed "conversion of Renminbi (RMB) into US$" in an actual SEC earnings release. The repair accepts both explicit directions, resolves parenthetical aliases to the same currency, rejects conflicting/negated/ambiguous directions and preserves the accounting-basis, period, source-identity and currency-column gates. It makes one declaration scan rather than scanning each full release for every currency pair. No ticker allowlist, inferred FX conversion, annualization, formula, provider or schedule change.

The complete JD source set yields22 native CNY observations from33 total observations. The11 USD convenience observations do not reach native records. Two reported quarterly revenue/cash-flow rows recover, together with the current ratio404602/354736. The actual originals support quarterly revenue356660/346401 million CNY and operating cash flow24409/37718 million CNY. TTM income, EPS, parent/common income, equity and valuation are still unqualified and stay missing. Existing statement footnote tables remain held for missing date headers. This is partial financial recovery, not complete JD coverage or readiness to switch all core fundamentals.

Seven complete original files are retained with compressed exact bytes, URLs, lengths and SHA256 in backend/tests/fixtures/sec_currency_wording/sources.json. One missing older public exhibit was fetched once,200, with a truthful Walnut user agent and strict size/time limits; no retries or bypass. Other sources were reused from earlier captures. Source artifacts are artifacts/currency-wording-oct18/.

## Verification

The final single-scan implementation passes all99 affected Linux parser/source/cache/watchlist cases in42.99seconds. The preliminary implementation also passed99cases; its slower201.98second result is not used as production-capacity evidence. Final full49-source offline comparison shows48 unchanged results and JD unavailable to partial. Both versions use identical retained source inputs; network is forbidden and there are no production writes. The retained cohort is not the full equity universe.

The installed-verifier local rehearsal passes16 source/fixture hashes, seven originals, exact source values, canonical panel/core preparation and cache repeats without refetching. It uses isolated SQLite and denies network; Windows Python3.14.2 CPU12.672s is local component evidence, not production capacity proof. The watchlist regression checks unavailable-to-available recovery without a false alert, later threshold crossing once, identical-repeat deduplication, and rollback without a new event/email. Exact release, installed verification and ordinary production acceptance remain pending.

## Discovery decision and remaining gates

Current sec_earnings_materials.discover_earnings_filings explicitly selects8-K/8-K-A with item2.02. Its original-byte staging cannot directly supply missing foreign6-K earnings statements. No redundant collector or unreviewed persistent cursor has been added. The broader discovery gap described in financial-discovery-followup-2026-10-18.md remains open. GOTU has not been admitted by assuming that an unqualified GAAP mention means US GAAP.

Core FMP selection, public SEC historical-statement selection, disabled institutional publication, two-host topology and final Massive sequencing remain unchanged. Financial coverage, institutional/transcript/paid analyst gaps, all monitoring/watchlist/daily/weekly/ranking consumers, sustained capacity, Massive activation, zero FMP egress and billing cancellation remain separate exit gates.
