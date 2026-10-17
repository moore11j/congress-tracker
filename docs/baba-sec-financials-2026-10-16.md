# Alibaba SEC financial preparation, October 16, 2026

## Verified replacement and limits

The exact BABA issuer adapter prepares seven ratios from native CNY/RMB US GAAP statements: gross, operating and parent net margins; revenue growth; operating margin change; current ratio; and parent ROE. Its source bundle has five complete originals: the June 2026 and June 2025 results and their explicitly linked 6-K covers, plus the fiscal March 2026 annual 20-F. It distinguishes the filing/publication dates from the financial quarter. The annual standard facts and exact interim financial tables are combined into aligned current/prior trailing periods, without widening generic foreign-issuer or currency policy. Core fundamentals remain FMP.

Source documents: [June 2026 results](https://www.sec.gov/Archives/edgar/data/1577552/000110465926099220/tm2623667d1_ex99-1.htm), [June 2025 results](https://www.sec.gov/Archives/edgar/data/1577552/000110465925085638/tm2524743d1_ex99-1.htm), [annual report](https://www.sec.gov/Archives/edgar/data/1577552/000119312526231755/baba-20260331.htm). Exact cover URLs, original hashes and complete deterministic gzip fixtures are tracked in sec_baba_sources.json and tests/fixtures/sec_baba. Five financial-source originals total774360compressed bytes. Twenty-four bounded covers and fourteen linked exhibits were captured for source review. Eight later filings concern share movements, votes, grants and share placements; their exact identities and linked hashes are recorded. No later quarterly financial statement was found in that reviewed set. New or amended filings require review before further preparation.

## Accounting and contract checks

Only the two RMB financial columns feed ratios. The adjacent US-dollar columns are convenience translations and are never treated as native USD earnings or cash. Income reconciles from revenue and expenses through operating income, pretax, tax/investee results, consolidated income, noncontrolling attribution, parent income and ordinary-shareholder earnings after mezzanine accretion. Gross profit derives from the reported revenue/cost pair with both inputs retained. ROE consistently uses parent income and parent shareholders equity; equity reconciles independently through its components and through assets minus liabilities, mezzanine equity and NCI. Published million-unit amounts use bounded rounding intervals.

Current TTM revenue is1044971million CNY, parent earnings73557million, and operating income30323million. The seven returned values match independent arithmetic and retain complete filing/unit/period provenance. There is no EPS, FX conversion, adjusted EBITDA or USD valuation inference. A USD market-cap test explicitly proves that native CNY income never produces P/E, earnings yield or price/sales through this adapter.

Cash coverage remains a real gap. Alibaba's issuer FCF definition excludes campus/land purchases, adjusts buyer-protection deposits and may include intangible purchases. The current capital-expenditure narrative does not establish PPE-only cash purchases across every trailing input. The adapter does not copy this non-GAAP FCF into Walnut's canonical CFO-minus-PPE fields, nor use depreciation combined with impairment as EBITDA. Cash ratios, leverage and complete historical statement panels remain unresolved; seven prepared ratios do not mean full financial replacement.

## Validation

276distinct Linux Python3.12.3 checks pass:275combined issuer/financial/preparation/cache/alert checks plus one additional native-CNY/USD-market-cap test. Initial findings were a nonbreaking-space exhibit label, test mutations that did not change the intended date node or exceeded no rounding bound, and a raw date ValueError requiring consistent coverage-error wrapping. These are repaired and the final checked scope has no failures. Complete originals are used, not invented financial fixtures.

The full five-original replay matches all18consumed annual facts to independent SEC companyfacts for exact accession/period/unit. Acquisition, projection, SQLite prepared cache and repeat are exact with network blocked. Preparation produces zero canonical fundamentals rows. Actual fundamentals summary and Confirmation input are exercised. Watchlist transition creates zero false alerts, a subsequent synthetic crossing triggers once, and repeated/rollback states create no duplicate or false alerts. Nineteen other issuer controls match pre-change revision9bf64c51 exactly; the older BABA control remains held under both implementations and is tested separately from the new current source bundle.

Twelve-hash installed-runtime verifier is prepared and passes local Python3.14.2 rehearsal with all five complete originals, isolated SQLite, currency/tamper guards and no network or production records. Its initial helper exception-name typo was corrected. Installed production and ordinary-source verification remain pending. Evidence: primary artifacts/baba-financials-oct16/covers.json, exhibits.json, annual-parsed.json and full-replay-linux.json; artifacts/verify-baba-runtime-oct16.py.

## Release and remaining gates

Local codex/baba-sec-financials; reviewed merge/deployment still pending. Final pre-review aggregate guard release-blocker-1792118352.json has a continuing protected portfolio cycle1010/1135, zero recorded scope failures, six pending jobs, no live portfolio PID at that observation. This is advancing checkpoint evidence, not completion or a currently verified process wait. PR694-704 backend releases remain deferred; no guard bypass/reset/retry. Public anonymous Feed/Insights/AAPL/NVDA return200/no server/RSCerror in0.626/1.749/0.867/1.020seconds (public-render-1792118257.json), which is a spot check only.

Prior NVO turn and current source/implementation/consumer turn are concrete migration progress. Goalactive/FMPenabled. Full financial coverage, institutional activation, transcripts, remaining analysts/valuation/calendar, all scheduled consumer checks, sustained performance, final Massive entitlement, zero-FMP traffic and billing closure remain. Pending private exports untouched. No source selector, schedule, hardware, subscription, customer email, video or landing-page change.


## October17 installed release

This package is deployed in f4f9ff0f through successful guarded workflow38986463595, and its isolated installed-runtime proof passes. See the [central release and ordinary-operation report](migration-release-verification-2026-10-17.md) for exact receipts, verifier corrections, live-source distinctions and remaining gates. Earlier deferred-release notes above are historical. No provider selection changed; FMP retirement remains incomplete.
