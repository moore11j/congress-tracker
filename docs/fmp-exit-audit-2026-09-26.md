**Walnut: FMP cancellation and free replacement audit — September 26, 2026**

**Decision:** Walnut can be rebuilt to operate without FMP. Cancelling access before that migration would stop or degrade many current features. I did not find a verified, commercially usable, completely free combination that preserves every feature, coverage level, and refresh behavior unchanged.

The best bootstrap approach is to replace public-record data with direct sources, keep Walnut's own calculations, and reduce the licensed market-data purchase to the smallest necessary scope. If the replacement-data budget must be exactly $0, expect to narrow the product: use embedded market displays, preserve public-record research, and suspend or reduce features that require backend market prices and proprietary analyst datasets.

**Scope and evidence**

This is a static audit of application code, imports, provider configuration defaults, Fly cron, GitHub scheduled ingestion, and current primary-source vendor documentation. It identifies **73 distinct FMP URL paths: 71 stable and 2 legacy**. This includes optional providers, fallback candidates, and two bulk helpers with no application callers; it does not mean all 73 receive production traffic. Parameters and repeated callers are not counted separately.

Repository revision inspected: `b1f7ddb7a6e7669330e8713990420fece94b7ab1`.

The [complete endpoint inventory](../artifacts/fmp-exit-audit-2026-09-26/endpoint-inventory.md) lists every path, feature family, execution status, and source location. The [JSON inventory](../artifacts/fmp-exit-audit-2026-09-26/endpoint-inventory.json) includes all 246 captured source references. Its [generator](../artifacts/fmp-exit-audit-2026-09-26/build_inventory.py) is reproducible and makes no provider calls.

I did not inspect the signed FMP enterprise agreement, invoices, production database overrides, live call counts, or each replacement account's entitlements. No subscriptions, production settings, or application code were changed. Proposed replacements have not undergone production parity testing. Runtime-configured endpoint URLs may extend or override the static inventory.

**What cancellation would affect**

“After migration” below means engineering and validation are required; the alternative does not automatically activate when the FMP key stops working.

| Feature | What happens when FMP access ends today | Best free replacement and remaining gap |
|---|---|---|
| Congress trade feed, member activity, alerts | The scheduled recent and core ingestion paths call FMP House/Senate feeds. New disclosures stop entering through those paths. | Direct House disclosures and Senate eFD. Existing normalization/staging code helps, but it is not a complete scheduled replacement. PDF/OCR, amendments, identity matching, discovery, and permitted use need resolution. |
| Insider feed and profiles | The core ingest calls FMP insider trading. Fresh events and dependent alerts stop. | SEC Form 4 XML, filing discovery, and amendments. Existing parser and promotion functions are a starting point; ongoing discovery and orchestration still need work. |
| Institutional holdings and ownership | Scheduled latest/extract ingestion and several ticker ownership enrichments lose updates. | SEC 13F filings and XML information tables; existing exact-period SEC recovery can be expanded. Build latest-filing discovery, amendment handling, security mapping, and ownership/performance calculations. |
| Quotes, charts, daily prices, corporate actions | Cache hits may temporarily display old data; misses and refreshes fail or show unavailable/warming states. | Free TradingView widgets can supply displays. Free Alpaca historical bars are technically useful but standard terms do not authorize redistribution. No verified unrestricted free backend replacement found. |
| P&L, performance, leaderboards, backtests, retirement simulations | Walnut owns the calculation logic, but new evaluations need fresh prices, splits, dividends, and events. Old stored results are not proof of continuing operation or retention rights. | Keep calculation engines and replace their inputs. A licensed backend price/history feed remains the major unresolved $0 requirement. |
| Technical indicators, market pressure, price alerts | Dependent on new price/volume observations; market pressure also calls market-cap data. | Compute locally from replacement OHLCV. Single-exchange volume is not equivalent to consolidated volume and requires recalibration. |
| Financial statements, growth, ratios | FMP financial caches stop updating and ticker financial panels lose uncached values. | SEC XBRL/company facts and local calculations for US SEC filers. Normalize tags, units, fiscal periods, restatements, TTM, and share counts. Exact FMP coverage and definitions will differ. |
| Screener and company metadata | FMP company-screener, metadata lookup, and enrichment paths lose inputs; existing cache is finite. | Build a local universe from SEC ticker/submission data and screen locally using SEC fundamentals and replacement prices. SIC is not identical to FMP sector/industry classifications; ETF/non-US coverage needs separate handling. |
| DCF/valuation | FMP statements, estimates, profile inputs, and provider baseline cease refreshing. | Keep a Walnut DCF using SEC actuals and explicit user/model assumptions. Analyst-based growth and the FMP baseline are not preserved unchanged. |
| Analyst ratings, target prices, consensus, forward estimates | Fresh consensus, revisions, targets, and forward EPS/revenue inputs are lost. | No verified comprehensive free commercial substitute found. Broker announcements can support selected sourced events, but cannot recreate a complete consensus dataset. |
| Earnings and corporate calendars | Economic, earnings, dividend, IPO, and split calendar calls fail to refresh. | BLS/BEA/Fed schedules for US macro; issuer IR announcements and SEC filings for corporate events. Future earnings coverage, IPO dates, forecast consensus, and surprise metrics would be narrower. |
| News, press releases, article-driven marketing | FMP ticker/general/category news and article-reactive discovery stop updating. | Issuer IR feeds, permitted publisher RSS, SEC announcements, GDELT discovery, or TradingView news widgets. Full-text reuse, ticker tagging, completeness, and AI ingestion rights must be handled separately. |
| SEC filings tab | Its FMP search calls stop returning fresh filings. | Direct SEC submissions/archives. This is one of the clearest free replacements. |
| Research Memory / operational evidence / briefs | Transcript discovery and text ingestion lose FMP. Briefs also consume other affected datasets. | Reuse existing direct SEC facts support, filings, issuer releases, presentations, and selectively available issuer transcripts. Full call/Q&A transcript coverage is not guaranteed; transcription introduces processing costs. |
| Macro, Treasury, sector/world/FX/crypto/commodity displays | FRED-backed data can continue, but a separate FMP macro-snapshot implementation still calls FMP and checks its key. Sector and market displays also depend on FMP prices. | Keep FRED/public-agency macro; replace FMP legacy snapshot paths. ETF proxies, official reference series, or free widgets can preserve reduced displays, with clearly different timing/coverage. |
| Index memberships | Optional FMP constituent refresh would fail if selected. | Current code already defaults to Wikipedia with validation and provenance. Preserve attribution and distinguish current membership from historical point-in-time membership. |
| Options calculator / options activity | The dedicated services use Massive and Alpaca rather than FMP. | FMP cancellation alone does not remove those provider integrations. Their own costs, entitlements, and any surrounding ticker-page FMP panels remain separate issues. |
| Accounts, subscriptions, saved watchlists, notes, application UI | No direct FMP dependency for the underlying account/product infrastructure. | Keep them; data-dependent alerts and views still require working inputs. |

**Where the dependencies actually live**

The following groups cover every path in the inventory. Paths are relative to `https://financialmodelingprep.com/stable/`, except the two explicitly listed legacy paths.

| Feature family | FMP endpoints |
|---|---|
| Congress | `house-latest`, `senate-latest` |
| Insiders | `insider-trading/latest`, `insider-trading/search` |
| Institutional | `institutional-ownership/latest`, `institutional-ownership/extract`, `institutional-ownership/dates`, `institutional-ownership/symbol-positions-summary`, `institutional-ownership/extract-analytics/holder`, `institutional-ownership/holder-performance-summary`, `institutional-ownership/holder-industry-breakdown`, `institutional-ownership/industry-summary`, `shares-float` |
| Legacy institutional candidates | `institutional-holder/latest`, `institutional-holdings/latest` |
| Prices / market snapshots | `historical-price-eod/light`, `historical-price-eod/full`, `historical-chart/1min`, `batch-quote`, `batch-index-quotes`, `batch-commodity-quotes` |
| Corporate actions | `dividends`, `splits` |
| Metadata / universe | `profile`, `search-symbol`, `search-cik`, `company-screener`, `market-capitalization` |
| Optional index constituents | `sp500-constituent`, `nasdaq-constituent` |
| Statements | `income-statement`, `balance-sheet-statement`, `cash-flow-statement`, `income-statement-ttm`, `balance-sheet-statement-ttm`, `cash-flow-statement-ttm` |
| Fundamentals | `income-statement-growth`, `balance-sheet-statement-growth`, `cash-flow-statement-growth`, `ratios`, `ratios-ttm`, `key-metrics-ttm` |
| Earnings / valuation | `earnings`, `analyst-estimates`, `custom-discounted-cash-flow` |
| Analyst data | `grades`, `grades-consensus`, `grades-historical`, `price-target-consensus`, `price-target-summary`, `price-target-news` |
| Unused bulk client helpers | `upgrades-downgrades-consensus-bulk`, `price-target-summary-bulk` |
| Calendar | `economic-calendar`, `earnings-calendar`, `dividends-calendar`, `ipos-calendar`, `splits-calendar` |
| News | `news/general-latest`, `news/stock-latest`, `news/stock`, `news/press-releases`, `news/crypto-latest`, `news/forex-latest` |
| Filing search, including fallback candidate | `sec-filings-search/symbol`, `sec-filings-company-search/symbol` |
| Transcripts | `earning-call-transcript-dates`, `earning-call-transcript` |
| Macro / sectors | `economic-indicators`, `treasury-rates`, `sector-performance-snapshot` |
| Legacy metadata | `/api/v3/profile/{symbol}`, `/api/v3/search` |

Key implementation findings:

- [Scheduled core ingestion](../backend/app/ingest_run.py) invokes FMP-backed House, Senate, and insider importers. [Recent Congress ingestion](../backend/app/ingest_congress_recent.py) also invokes those House/Senate importers. Both [GitHub schedules](../.github/workflows/daily_ingest.yml) and [Fly cron](../backend/crontab) matter in an exit plan.
- [Form 4](../backend/app/services/sec_form4.py) and [official Congress](../backend/app/services/official_congress.py) staging/promotion functions exist, but no application call sites for those staging/promotion functions were found. Provider registry labels saying “official” or “shadow” do not establish a working replacement feed. The separate [House annual-disclosure importer](../backend/app/ingest_house_annual_disclosures.py) is useful but does not replace the ongoing House/Senate transaction feeds.
- [SEC 13F support](../backend/app/clients/sec_edgar.py) and [exact-period recovery](../backend/app/ingest_institutional_activity.py) already exist. The scheduled institutional job still uses the FMP latest/extract pipeline. 13F positions are quarterly disclosures, not actual contemporaneous buys; rebuilding analytics must preserve that distinction.
- [Research briefs](../backend/app/services/research_briefs.py) already have SEC company ticker/facts fetches. Extend those ideas into a shared fundamental-data pipeline rather than treating this as an entirely new integration.
- [Price lookup](../backend/app/services/price_lookup.py) has a Massive fallback for some historical retrievals. It does not replace every quote/history/corporate-action route and is not evidence that Walnut has a free commercial data license.
- [Index memberships](../backend/app/services/index_memberships.py) default to Wikipedia. FMP constituent endpoints are optional, not a mandatory exit blocker.
- [Operational intelligence](../backend/app/services/operational_intelligence.py) calls FMP transcript endpoints when transcript analysis is enabled. [AI marketing](../backend/app/services/ai_marketing.py) currently fetches `news/general-latest`; old “FMP Articles” labels do not imply a separate articles endpoint is used.
- `walnut_cache` frequently means previously downloaded FMP data. Some quote fallbacks switch between two FMP endpoints. Neither is an independent replacement provider.
- Some reads explicitly filter `provider == "fmp"`, including screener/hydration paths. New adapters must update these assumptions. Simply inserting SEC rows with a different provider label can leave them invisible to existing readers.
- A key-disable test is insufficient by itself: some direct request functions bypass the shared FMP guard. A pre-cancellation exercise should deny FMP network access in an isolated environment and inspect missing/stale outputs.

**Free sources worth building around**

**SEC EDGAR — strongest foundation.** Use `data.sec.gov/submissions/CIK##########.json` for filings and reference metadata; `data.sec.gov/api/xbrl/companyfacts/CIK##########.json` for facts; filing archives for Form 4/13F XML and 8-K exhibits. Public APIs require no paid key and offer nightly bulk downloads. Respect the SEC's aggregate 10-requests/second limit across workers and use an identifiable user agent. Sources: [SEC API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces), [SEC access guidance](https://www.sec.gov/about/developer-resources).

SEC data can support annual/quarterly statements, locally derived ratios and growth, ownership feeds, and filing-based research. It does not supply a stock-price feed, analyst forecasts, or turnkey vendor-normalized tables. Company Facts excludes custom-taxonomy and segment-specific facts from its aggregated coverage; use underlying filings when needed. Fiscal/YTD normalization, amendments, units, and as-of availability are engineering work. Source: [SEC XBRL coverage](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).

**Official Congress disclosures — free access, substantial ingestion work.** Build discovery and parsing around the [House database](https://disclosures-clerk.house.gov/FinancialDisclosure/ViewSearch) and [Senate eFD](https://efdsearch.senate.gov/search/). The House notice restricts commercial use with an exception for news/communications dissemination. Confirm Walnut's intended use under the applicable disclosure rules; do not equate public access with unrestricted reuse. This is a specific source constraint, not a conclusion that Walnut is prohibited.

**FRED and government publishers — keep and extend.** Use government-origin macro/Treasury series, with BLS, BEA, Federal Reserve and Treasury schedules/data for a US calendar. Official releases supply actuals and dates, not the proprietary analyst consensus used to calculate consensus surprises. FRED hosts some third-party restricted series, so check each selected series. Sources: [FRED terms](https://fred.stlouisfed.org/docs/api/terms_of_use.html), [BLS release calendar](https://www.bls.gov/schedule/2026/), [BEA release schedule](https://www.bea.gov/news/schedule).

**TradingView free widgets — useful for the visible product.** Quotes, charts, market overview, news, heatmaps, and economic-calendar widgets include data and branding. They could replace selected FMP display panels. They are not a backend data API: Walnut cannot use them to calculate returns, populate its custom screener, trigger server alerts, or retain its own chart overlays unchanged. Available symbols and delay vary. Source: [TradingView widgets and FAQ](https://www.tradingview.com/widget/).

**Issuer IR + permitted feeds + GDELT — partial news/research replacement.** Discover official earnings releases, presentations, event dates and whatever transcripts issuers publish. GDELT can support article discovery and links, but does not grant publisher full-text/image rights or provide FMP-equivalent ticker tagging. Use documented feed permissions and independently sourced facts for briefs. Source: [GDELT DOC API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/).

**Locally computed analytics — no provider fee for the computation.** Ratios, growth, technical indicators, DCF, institutional deltas, and rankings can remain Walnut code. They still require appropriate inputs. Float is not simply shares outstanding; matching FMP float/ownership denominators needs careful definitions. Splits and dividend adjustments must preserve the current distinction between price return and total return. Do not substitute IEX-only volume for consolidated volume without changing market-pressure methodology and labels.

**Free API alternatives investigated**

| Candidate | Free offering / usefulness | Suitability for public Walnut |
|---|---|---|
| Alpaca Basic | 200 historical requests/minute; US stock history since 2016; real-time IEX and restrictions on the most recent 15 minutes of historical consolidated data. Good technical migration candidate. | Standard support policy prohibits redistributing API data. Requires a separate arrangement for Walnut; a free key or existing options integration does not settle rights. [Data plan](https://docs.alpaca.markets/us/docs/about-market-data-api), [redistribution policy](https://alpaca.markets/support/redistribute-alpaca-api). |
| Massive / Polygon Basic | $0, 5 calls/minute, two years of history, EOD data and corporate actions. Existing partial adapter helps. | Listed for individual use; short history also fails older backtests. [Pricing](https://massive.com/pricing). |
| Twelve Data Basic | 8 API credits/minute, 800/day; equities, FX, crypto, reference data and technical indicators. | Even Business Basic is internal non-display. External display appears in paid Venture; broader distribution has additional requirements. [Business pricing](https://twelvedata.com/pricing-business), [usage rules](https://support.twelvedata.com/en/articles/5332349-commercial-and-personal-usage). |
| Alpha Vantage | Standard free allocation of 25 calls/day; useful for tiny prototypes and selected data checks. | Far too small for Walnut's universe; real-time/delayed US market data is premium and production commercial rights need confirmation. [Official FAQ](https://www.alphavantage.co/support/). |
| Finnhub | Technically broad data catalog including quotes/news/fundamental and analyst-related products. | Published terms restrict listed plans to personal use unless otherwise agreed, including derived results. Not a verified free business solution. [Terms](https://finnhub.io/terms-of-service). |
| Yahoo / yfinance | Broad free-access prototype coverage. | Library authors identify the data API as personal-use and the project as research/educational. Open-source code is not a commercial data license. [Project documentation](https://github.com/ranaroussi/yfinance). |
| Tiingo Starter | Free personal/internal access; 500 symbols/month and 50 requests/hour listed. | No public redistribution on the free tier. Promising lower-cost paid display option below. [Pricing](https://www.tiingo.com/about/pricing), [licensing](https://www.tiingo.com/documentation/general). |
| Marketstack Free | 100 requests/month, one year of history, EOD and corporate-action data. | Explicitly non-commercial and too limited. Basic advertises commercial use from $9.99/month, but public redistribution rights are not established by that label. [Pricing](https://marketstack.com/pricing). |
| 3spread Community | 10,000 requests/day and SEC-related datasets; attractive technically. | Personal/non-commercial; neither Community nor Startup permits public product distribution. Business rights need separate terms. Do not assume normalized SEC data inherits the SEC's access terms. [Plans](https://www.3spread.com/plans), [terms](https://www.3spread.com/terms). |
| Stooq downloads | Potential EOD research source. | I did not establish a current commercial redistribution grant or service commitment; not counted as a verified production replacement. |

Free limits are not interchangeable. For illustration, 1,000 tickers at one request each already exceed Twelve Data's 800-credit daily free allocation, before statements, calendars, or quotes. Batching may reduce HTTP overhead without reducing per-symbol credits. Rate reduction and caching help consumption, but cannot resolve license restrictions or missing datasets.

**Cancellation and existing data**

FMP's published sections 6.2–6.3 end data/derived-information usage rights and require deletion of cached FMP data at agreement termination. Section 5 contains a qualified retention provision, so the signed enterprise terms and written clarification matter. Do not plan on keeping FMP history indefinitely or stockpiling it before cancelling. Source: [FMP terms](https://site.financialmodelingprep.com/terms-of-service).

That affects provenance review for prices, filings normalized by FMP, fundamentals, generated research, and derived performance caches. Independently reacquiring public filings and recomputing results is different from retaining FMP-sourced records. Determine the applicable agreement before any deletion; this audit performs none. Also distinguish stopping renewal from the date data-access/license rights actually end.

**Recommended bootstrap path**

1. Establish the access-end date and contract-specific retention rights. Seek a short migration bridge or a smaller FMP scope if needed; fewer calls alone may not reduce a fixed enterprise bill.
2. Replace SEC filings first, then fundamentals, Form 4, and 13F ingestion. Reuse existing code but add discovery, scheduling, normalization, amendments, and reconciliation against primary documents.
3. Complete direct Congress ingestion and validate coverage, parsing, identities, and applicable usage rules. Preserve raw-source provenance, filing dates, transaction dates, amendment links, and deterministic deduplication.
4. Resolve backend prices before promising continued performance/backtesting. If $0 is mandatory, use widgets for displays and explicitly narrow price-dependent features. If a small budget is possible, buy only the necessary redistributable prices/corporate actions.
5. Replace FMP news discovery and FMP macro snapshot calls. Retain the direct FRED lane. Rework briefs around SEC/issuer evidence; explicitly drop or label missing analyst/transcript coverage.
6. Update provider-specific queries and readers, then rebuild authorized independent histories and derived outputs. Reconcile split-heavy stocks, dividends, renamed/delisted securities, amended filings, financial restatements, and point-in-time dates.
7. Run an isolated FMP-blocked exercise through daily refreshes and scheduled alerts, using both populated and empty caches. Verify fresh results, not merely HTTP 200 responses. Use a complete historical quarter to validate 13F handling; compare representative upcoming and past corporate events.
8. End FMP access only after retained/replaced/retired features and data disposition are accounted for. No precise implementation timeline is justified until parser coverage and history requirements are measured; this is a multi-component migration, not a key swap.

**The most promising paid fallback if free cannot preserve the core:** Tiingo publicly lists an **EOD + IEX display-redistribution plan at $250/month for startups**, including both feeds. This is distinct from its $50 internal-commercial plan. Confirm startup eligibility, history/corporate-action coverage, public derived metrics, backtesting use, storage, and export rights in the agreement. It would address the major backend-price problem while public sources replace much of the rest, but does not restore proprietary analyst/transcript datasets or consolidated live volume. Source: [Tiingo EOD product pricing](https://www.tiingo.com/products/end-of-day-stock-price-data).

My recommendation is to target a much smaller licensed price-data bill plus direct public sources, and temporarily remove the hardest proprietary extras. A completely free version is feasible as a narrower public-record research product. Keeping every current feature live, fresh, and unchanged for $0 is not supported by this audit.
