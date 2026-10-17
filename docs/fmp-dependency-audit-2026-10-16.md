# Remaining FMP dependencies and FRED source integrity, October 16, 2026

## Scope and reproduced inventory

Audited the managed application at merged11ec7d824334751295580c7d5d903a0968dcebeb, with the subsequent local FRED repair described below. The primary checkout is divergent and is not the authoritative application. New source-only utility `backend/scripts/research/audit_fmp_dependencies.py` reads Python ASTs without importing the application, database access, environment credentials or network calls. Run it from the repository with `python backend/scripts/research/audit_fmp_dependencies.py --output /path/to/inventory.json`.

Result: **74 endpoint references,72 stable paths and2 legacy paths, across32 FMP-referencing modules**. None of the known catalog paths is missing; no unclassified literal request argument was found. This includes optional/unused helpers, registry defaults and literal references; it is not74 active calls or74 independently missing replacements. Computed endpoint expressions and database-configured overrides are not fully resolved. HTTP call sites can also serve non-FMP sources and are marked for review. Inventory does not establish zero traffic, source selection, data freshness or shutdown readiness.

Two independent runs produce byte-identical JSON, SHA256 feca89ff409be9cdea08c6324a3ede16d5988b1e270d5f039f8aadeab65b3c62. Receipts artifacts/fmp-dependencies-oct16.json and artifacts/fmp-dependencies-oct16-repeat.json retain every reference, function, line and normalized source hash. The older inventory omitted market-capitalization-batch; current client/leaderboard paths include it. The tracked table below makes the complete endpoint list durable.

## Acceptance map

These are code findings plus the latest recorded release receipts, not newly probed production settings. Each capability still needs its own applicable live coverage/consumer check.

| Feature family | Replacement position | Remaining acceptance |
|---|---|---|
| Congress | Official House and guarded Senate backup selected in recorded receipts | Held filing reconciliation, Senate freshness/rollover, ordinary consumers |
| Insiders | SEC Form4 selected; ordinary run125 verified188filings/365events | Remaining backlog/holds and complete monitoring/watchlist/digest acceptance |
| Institutions | SEC13F collection/replay implemented, publication still off | Current cohorts, full ownership/float denominators and performance/sector consumers, safe publication and schedules |
| Equity prices/quotes | Massive adapters/preparation; final subscription step | Actual entitlement, adjusted history/current quotes, all read paths and market-pressure consumers, stale/cache/source metadata |
| Corporate actions | Massive target | Split/dividend semantics, price adjustments and scheduled consumer verification; calendar publication is separate |
| Company/security reference | SEC metadata selected; some pricing/market-cap clients remain | Exact security coverage, market cap/share basis; market_pressure._fetch_required_sp500_profile still uses FMP market-capitalization |
| Index membership | Wikipedia is an existing optional alternative | Current membership/cache verification; FMP helpers remain optional code paths |
| Statements and core ratios | SEC historical reader selected; direct/free core preparation | Broad financial/period/unit coverage, score/alert rebaseline, screener/comparison/ranking verification before core selection |
| Earnings and forecasts | Direct/SEC actuals and gated Finnhub paid readers | Reviewed forecast basis, live entitlement/coverage and scheduled preparation |
| Valuation | ticker_valuation.get_ticker_valuation still uses FMP price-target-consensus and custom-discounted-cash-flow | Existing pending valuation-model decision; a paid forecast subscription alone does not replace the DCF service. No new model or silent removal approved here |
| Analysts | Selected monthly counts; EPS/revenue/targets readers built; PR700 individual-rating preparation merged | Individual grade canonical reconciliation, source/publication timing, live coverage; individual price-target events/history remain a separate gap |
| Calendar | Selected BLS dates and Finnhub earnings | free_calendar explicitly reports IPO/dividend/split coverage unavailable; BLS dates are not complete global economic actuals/consensus |
| News and press | Finnhub news and SEC press selected | Broad ticker/source coverage, thumbnail availability, freshness and content/watchlist consumers |
| SEC filing search | SEC metadata/filing readers selected | Broad identity, newly filed documents and rollover checks |
| Transcripts | Nine of ten configured issuer readers verified; selector still FMP | UL source hold, broad issuer/period coverage, publication and research consumers |
| Macro/Treasury | Normal builder_safe Insights path already uses FRED cache | Ordinary source refresh/reader reliability; legacy FMP macro code is not the normal selected path |
| Sector/market snapshots | ETF-proxy code exists with separate INSIGHTS_SECTOR_PROVIDER flag; normal Insights also uses cached ETF proxies | Explicit proxy semantics and fresh Massive price basis; no claim that all-company sector averages equal ETF returns. Legacy alternative mode retains FMP index/commodity paths |

Existing free_calendar lines149ff explicitly retain three missing event kinds. Existing ticker_valuation._fetch_dcf_input_rows requests twelve source groups before the custom DCF call; switching historical statements elsewhere does not migrate that consumer. Screener only permits FMP fallback when its selected fundamentals provider is FMP; leaderboard uses verified selected-market inputs in the replacement path. These distinctions must remain in the final cutover audit.

Final consumer acceptance must cover monitoring digests, watchlist alerts, daily/weekly delivery and Top Stocks; Congress/member and institutional profiles; ticker financials/news/transcripts/valuation; screener/comparison; market quotes/pressure; strategy/portfolio/outcome price consumers; and Insights. Source timestamps, event identities, duplicates, entitlements, opt-in delivery and rollback must be checked with the final selected sources. A static off-switch test or public200 cannot satisfy these gates. Prices remain last, Quiver excluded, FMP enabled.

## FRED defect found and repaired

Current parser reproduction before the change accepts a CSV headed DGS2 as requested DGS10, accepts Infinity, and silently chooses the first of conflicting same-date observations. These were synthetic reproductions of actual code paths, not evidence of production data corruption. FRED defines [DGS10](https://fred.stlouisfed.org/series/DGS10) and [DGS2](https://fred.stlouisfed.org/series/DGS2) as distinct maturity series; both are daily percentages. A numeric value alone is insufficient identity evidence.

Repair validates the exact requested series column and date column, rejects extra/missing fields, invalid dates and nonfinite values, and rejects conflicting duplicate dates. Identical duplicates collapse; legitimate zero/negative values and documented dot/empty missing observations remain supported. The entire response validates before the existing bounded history is retained or database writes begin. No schedule, series list, macro formula, source switch or customer feature changes.

Invalid refreshes preserve every prior observation and its observation/fetch/update clocks, while marking the refresh state error so the consumer explicitly shows stale data. Existing FRED snapshot source identities remain intact. This does not audit old production rows or claim earlier values were corrupt.

## Validation and deployment

90 distinct tests pass on Linux Python3.12.3:47 source/cache/macro tests plus43 Insights snapshot and sector-source tests. Two pre-existing FastAPI on_event deprecation warnings occur; no failing tests. A public-source probe fetched one bounded CSV for each of all11configured FRED series (2024-10-01 through2026-10-15), all HTTP200. Complete original hashes are in artifacts/fred-public-parity-oct16/receipt.json. Every valid observation matches the exact previous parser.

Full offline replay of those originals persists2647 observations and compares complete cache rows, six macro cards, five Treasury cards and diagnostics under old/new parsers. All outputs match; repeated refresh is skipped with no additional source requests. Wrong-series refresh fixture preserves the full old cache and returns stale10Y data. Receipt artifacts/fred-public-parity-oct16/consumer-receipt.json, Windows Python3.14.2. The first replay attempt lacked managed Git read access; reviewed rerun succeeded. No production database records or customer data were used. No claim about live deployed parser behavior yet.

Reviewed PR/CI/merge and guarded backend deployment remain next. The current protected portfolio cycle still defers PR694-700 in the latest read; no bypass or reset. The prior goal turn merged analyst preparation and was concrete progress. This turn produces a reproducible dependency inventory, identifies and repairs a free-source integrity defect, and verifies full public-source consumer parity. Goal remains active, with all remaining financial/institutional/transcript/analyst/valuation/consumer/capacity/finalMassive/zero-FMP/billing gates intact. Pending private-export approvals are untouched.

## Complete literal endpoint inventory

| Endpoint | Family | Representative code reference |
|---|---|---|
| `/api/v3/profile/{symbol}` | Legacy company and security reference | [backend/app/services/ticker_meta.py:47](../backend/app/services/ticker_meta.py#L47) `_fmp_profile` |
| `/api/v3/search` | Legacy company and security reference | [backend/app/services/ticker_meta.py:103](../backend/app/services/ticker_meta.py#L103) `_fmp_search` |
| `/stable/analyst-estimates` | Earnings and forecasts | [backend/app/services/ticker_financials.py:1131](../backend/app/services/ticker_financials.py#L1131) `_fetch_financial_sections` |
| `/stable/balance-sheet-statement` | Statements and calculated fundamentals | [backend/app/services/ticker_financials.py:1127](../backend/app/services/ticker_financials.py#L1127) `_fetch_financial_sections` |
| `/stable/balance-sheet-statement-growth` | Statements and calculated fundamentals | [backend/app/services/ticker_valuation.py:560](../backend/app/services/ticker_valuation.py#L560) `_fetch_dcf_input_rows` |
| `/stable/balance-sheet-statement-ttm` | Statements and calculated fundamentals | [backend/app/services/ticker_valuation.py:553](../backend/app/services/ticker_valuation.py#L553) `_fetch_dcf_input_rows` |
| `/stable/batch-commodity-quotes` | Prices and market snapshots | [backend/app/services/fmp_market_snapshot.py:780](../backend/app/services/fmp_market_snapshot.py#L780) `_request_quote_rows` |
| `/stable/batch-index-quotes` | Prices and market snapshots | [backend/app/services/fmp_market_snapshot.py:638](../backend/app/services/fmp_market_snapshot.py#L638) `_build_indexes` |
| `/stable/batch-quote` | Prices and market snapshots | [backend/app/services/fmp_market_snapshot.py:620](../backend/app/services/fmp_market_snapshot.py#L620) `_proxy_rows` |
| `/stable/cash-flow-statement` | Statements and calculated fundamentals | [backend/app/services/ticker_financials.py:1125](../backend/app/services/ticker_financials.py#L1125) `_fetch_financial_sections` |
| `/stable/cash-flow-statement-growth` | Statements and calculated fundamentals | [backend/app/services/ticker_valuation.py:559](../backend/app/services/ticker_valuation.py#L559) `_fetch_dcf_input_rows` |
| `/stable/cash-flow-statement-ttm` | Statements and calculated fundamentals | [backend/app/services/ticker_valuation.py:552](../backend/app/services/ticker_valuation.py#L552) `_fetch_dcf_input_rows` |
| `/stable/company-screener` | Company and security reference | [backend/app/clients/fmp.py:385](../backend/app/clients/fmp.py#L385) `fetch_company_screener` |
| `/stable/custom-discounted-cash-flow` | Valuation | [backend/app/services/ticker_valuation.py:562](../backend/app/services/ticker_valuation.py#L562) `_fetch_dcf_input_rows` |
| `/stable/dividends` | Corporate actions | [backend/app/services/event_calendar.py:40](../backend/app/services/event_calendar.py#L40) `module` |
| `/stable/dividends-calendar` | Calendars | [backend/app/services/event_calendar.py:31](../backend/app/services/event_calendar.py#L31) `module` |
| `/stable/earning-call-transcript` | Earnings transcripts | [backend/app/services/operational_intelligence.py:260](../backend/app/services/operational_intelligence.py#L260) `_ingest_latest_transcript` |
| `/stable/earning-call-transcript-dates` | Earnings transcripts | [backend/app/services/operational_intelligence.py:248](../backend/app/services/operational_intelligence.py#L248) `_ingest_latest_transcript` |
| `/stable/earnings` | Earnings and forecasts | [backend/app/main.py:9072](../backend/app/main.py#L9072) `_normalize_ticker_financials_payload` |
| `/stable/earnings-calendar` | Calendars | [backend/app/services/event_calendar.py:30](../backend/app/services/event_calendar.py#L30) `module` |
| `/stable/economic-calendar` | Calendars | [backend/app/services/event_calendar.py:29](../backend/app/services/event_calendar.py#L29) `module` |
| `/stable/economic-indicators` | Macro and sectors | [backend/app/services/fmp_market_snapshot.py:932](../backend/app/services/fmp_market_snapshot.py#L932) `_request_indicator_series` |
| `/stable/grades` | Analysts | [backend/app/clients/fmp.py:429](../backend/app/clients/fmp.py#L429) `fetch_grade_events` |
| `/stable/grades-consensus` | Analysts | [backend/app/clients/fmp.py:418](../backend/app/clients/fmp.py#L418) `fetch_grades_summary` |
| `/stable/grades-historical` | Analysts | [backend/app/clients/fmp.py:440](../backend/app/clients/fmp.py#L440) `fetch_historical_grades` |
| `/stable/historical-chart/1min` | Prices and market snapshots | [backend/app/main.py:10041](../backend/app/main.py#L10041) `_ticker_price_volume_summary` |
| `/stable/historical-price-eod/full` | Prices and market snapshots | [backend/app/services/price_lookup.py:980](../backend/app/services/price_lookup.py#L980) `get_eod_close_with_meta` |
| `/stable/historical-price-eod/light` | Prices and market snapshots | [backend/app/main.py:6192](../backend/app/main.py#L6192) `_fetch_eod_light_closes_for_symbol` |
| `/stable/house-latest` | Congress disclosures | [backend/app/ingest_house.py:21](../backend/app/ingest_house.py#L21) `module` |
| `/stable/income-statement` | Statements and calculated fundamentals | [backend/app/services/fundamentals_cache.py:632](../backend/app/services/fundamentals_cache.py#L632) `fetch_fundamentals_for_symbol` |
| `/stable/income-statement-growth` | Statements and calculated fundamentals | [backend/app/services/fundamentals_cache.py:194](../backend/app/services/fundamentals_cache.py#L194) `fundamentals_source_diagnostics` |
| `/stable/income-statement-ttm` | Statements and calculated fundamentals | [backend/app/services/ticker_valuation.py:551](../backend/app/services/ticker_valuation.py#L551) `_fetch_dcf_input_rows` |
| `/stable/insider-trading/latest` | Insider trades | [backend/app/clients/fmp.py:114](../backend/app/clients/fmp.py#L114) `fetch_insider_trades` |
| `/stable/insider-trading/search` | Insider trades | [backend/app/clients/fmp.py:111](../backend/app/clients/fmp.py#L111) `fetch_insider_trades` |
| `/stable/institutional-holder/latest` | Institutional filings and analytics | [backend/app/clients/fmp.py:169](../backend/app/clients/fmp.py#L169) `fetch_institutional_buys` |
| `/stable/institutional-holdings/latest` | Institutional filings and analytics | [backend/app/clients/fmp.py:170](../backend/app/clients/fmp.py#L170) `fetch_institutional_buys` |
| `/stable/institutional-ownership/dates` | Institutional filings and analytics | [backend/app/clients/fmp.py:255](../backend/app/clients/fmp.py#L255) `fetch_institutional_filing_dates` |
| `/stable/institutional-ownership/extract` | Institutional filings and analytics | [backend/app/clients/fmp.py:242](../backend/app/clients/fmp.py#L242) `fetch_institutional_filing_extract` |
| `/stable/institutional-ownership/extract-analytics/holder` | Institutional filings and analytics | [backend/app/clients/fmp.py:303](../backend/app/clients/fmp.py#L303) `fetch_extract_analytics_by_holder` |
| `/stable/institutional-ownership/holder-industry-breakdown` | Institutional filings and analytics | [backend/app/clients/fmp.py:340](../backend/app/clients/fmp.py#L340) `fetch_holder_industry_breakdown` |
| `/stable/institutional-ownership/holder-performance-summary` | Institutional filings and analytics | [backend/app/clients/fmp.py:325](../backend/app/clients/fmp.py#L325) `fetch_holder_performance_summary` |
| `/stable/institutional-ownership/industry-summary` | Institutional filings and analytics | [backend/app/clients/fmp.py:354](../backend/app/clients/fmp.py#L354) `fetch_industry_summary` |
| `/stable/institutional-ownership/latest` | Institutional filings and analytics | [backend/app/clients/fmp.py:168](../backend/app/clients/fmp.py#L168) `fetch_institutional_buys` |
| `/stable/institutional-ownership/symbol-positions-summary` | Institutional filings and analytics | [backend/app/clients/fmp.py:271](../backend/app/clients/fmp.py#L271) `fetch_symbol_positions_summary` |
| `/stable/ipos-calendar` | Calendars | [backend/app/services/event_calendar.py:32](../backend/app/services/event_calendar.py#L32) `module` |
| `/stable/key-metrics-ttm` | Statements and calculated fundamentals | [backend/app/main.py:12155](../backend/app/main.py#L12155) `_cached_fmp_symbol_row` |
| `/stable/market-capitalization` | Company and security reference | [backend/app/clients/fmp.py:509](../backend/app/clients/fmp.py#L509) `fetch_batch_market_capitalization` |
| `/stable/market-capitalization-batch` | Company and security reference | [backend/app/clients/fmp.py:508](../backend/app/clients/fmp.py#L508) `fetch_batch_market_capitalization` |
| `/stable/nasdaq-constituent` | Index membership | [backend/app/clients/fmp.py:545](../backend/app/clients/fmp.py#L545) `fetch_index_constituents` |
| `/stable/news/crypto-latest` | News and press releases | [backend/app/services/fmp_news.py:181](../backend/app/services/fmp_news.py#L181) `module` |
| `/stable/news/forex-latest` | News and press releases | [backend/app/services/fmp_news.py:182](../backend/app/services/fmp_news.py#L182) `module` |
| `/stable/news/general-latest` | News and press releases | [backend/app/services/ai_marketing.py:234](../backend/app/services/ai_marketing.py#L234) `module` |
| `/stable/news/press-releases` | News and press releases | [backend/app/services/fmp_news.py:605](../backend/app/services/fmp_news.py#L605) `_request_ticker_press_rows` |
| `/stable/news/stock` | News and press releases | [backend/app/services/fmp_news.py:538](../backend/app/services/fmp_news.py#L538) `_request_ticker_news_rows` |
| `/stable/news/stock-latest` | News and press releases | [backend/app/services/fmp_news.py:177](../backend/app/services/fmp_news.py#L177) `module` |
| `/stable/price-target-consensus` | Analysts | [backend/app/clients/fmp.py:451](../backend/app/clients/fmp.py#L451) `fetch_price_target_consensus` |
| `/stable/price-target-news` | Analysts | [backend/app/clients/fmp.py:480](../backend/app/clients/fmp.py#L480) `fetch_price_target_news` |
| `/stable/price-target-summary` | Analysts | [backend/app/clients/fmp.py:462](../backend/app/clients/fmp.py#L462) `fetch_price_target_summary` |
| `/stable/price-target-summary-bulk` | Analysts | [backend/app/clients/fmp.py:498](../backend/app/clients/fmp.py#L498) `fetch_bulk_price_target_summary` |
| `/stable/profile` | Company and security reference | [backend/app/clients/fmp.py:515](../backend/app/clients/fmp.py#L515) `fetch_company_profile` |
| `/stable/ratios` | Statements and calculated fundamentals | [backend/app/services/fundamentals_cache.py:196](../backend/app/services/fundamentals_cache.py#L196) `fundamentals_source_diagnostics` |
| `/stable/ratios-ttm` | Statements and calculated fundamentals | [backend/app/main.py:12155](../backend/app/main.py#L12155) `_cached_fmp_symbol_row` |
| `/stable/search-cik` | Company and security reference | [backend/app/services/ticker_meta.py:195](../backend/app/services/ticker_meta.py#L195) `_fmp_search_cik` |
| `/stable/search-symbol` | Company and security reference | [backend/app/services/ticker_meta.py:147](../backend/app/services/ticker_meta.py#L147) `_fmp_stable_search_symbol` |
| `/stable/sec-filings-company-search/symbol` | SEC filing search | [backend/app/services/fmp_news.py:1470](../backend/app/services/fmp_news.py#L1470) `get_sec_filings` |
| `/stable/sec-filings-search/symbol` | SEC filing search | [backend/app/services/fmp_news.py:1469](../backend/app/services/fmp_news.py#L1469) `get_sec_filings` |
| `/stable/sector-performance-snapshot` | Macro and sectors | [backend/app/services/fmp_market_snapshot.py:1631](../backend/app/services/fmp_market_snapshot.py#L1631) `_build_sector_performance` |
| `/stable/senate-latest` | Congress disclosures | [backend/app/ingest_senate.py:22](../backend/app/ingest_senate.py#L22) `module` |
| `/stable/shares-float` | Institutional filings and analytics | [backend/app/clients/fmp.py:283](../backend/app/clients/fmp.py#L283) `fetch_shares_float` |
| `/stable/sp500-constituent` | Index membership | [backend/app/clients/fmp.py:544](../backend/app/clients/fmp.py#L544) `fetch_index_constituents` |
| `/stable/splits` | Corporate actions | [backend/app/services/event_calendar.py:41](../backend/app/services/event_calendar.py#L41) `module` |
| `/stable/splits-calendar` | Calendars | [backend/app/services/event_calendar.py:33](../backend/app/services/event_calendar.py#L33) `module` |
| `/stable/treasury-rates` | Macro and sectors | [backend/app/services/fmp_market_snapshot.py:1520](../backend/app/services/fmp_market_snapshot.py#L1520) `_build_treasury` |
| `/stable/upgrades-downgrades-consensus-bulk` | Analysts | [backend/app/clients/fmp.py:490](../backend/app/clients/fmp.py#L490) `fetch_bulk_upgrades_downgrades_consensus` |


## Reviewed merge and pending runtime, October16 01:38UTC

[PR701](https://github.com/moore11j/congress-tracker/pull/701), tested2d13ab4a4c28b42d1c560d31ea0b520cb45d906b, merged237d593b27f190b7a539c5b2b3c594d0a5291475 at01:38:39UTC. Exact-head backend/frontend dependency audits and secret scan succeed in workflow38836104721, and Vercel preview succeeds. Fetched merged application/test/auditor diff is empty. The chat reached its100-attachment limit; ordinary PR URL retained. Unrelated TypeScript build metadata still hashesA185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA.

Prepared installed verifier artifacts/verify-fred-runtime-oct16.py and passed local Python3.14.2 rehearsal: two source hashes, all11public originals/2647observations, exact full prior-consumer digest67071e99235a0b2c30f53ed59660bed97b8644bc4b434c979d1c3d63f48cad07 and rejection of wrong-series/nonfinite/conflicting responses. No production records, network or writes. Receipt artifacts/fred-runtime-1792114704.json. An initial local command exceeded Windows argument length; compressed in-process code transport repaired the harness without changing application logic. Old-parser replay helpers are pinned to11ec7d82 so future reruns cannot silently compare the repaired parser with itself.

Post-merge guard artifacts/release-blocker-1792114732.json still reports one continuing protectedportfolio cycle595/1135, zero failed scopes and12pending jobs. No portfolio process was active at that instant; its unfinished durable cycle continues to prohibit rollout. PR694-701 remain merged but undeployed. No guard bypass, cycle reset or deployment retry. Earlier local/unreviewed wording is superseded by this reviewed merge; installed runtime and ordinary FRED refresh remain unverified. Prior/current turns both concrete progress; goalactive/FMPenabled and all remaining source/consumer/capacity/final-provider/zero-FMP/billing gates remain. All local test/probe handles are terminal.


## October17 installed release

This package is deployed in f4f9ff0f through successful guarded workflow38986463595, and its isolated installed-runtime proof passes. See the [central release and ordinary-operation report](migration-release-verification-2026-10-17.md) for exact receipts, verifier corrections, live-source distinctions and remaining gates. Earlier deferred-release notes above are historical. No provider selection changed; FMP retirement remains incomplete.
