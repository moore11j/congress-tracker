# Free financial sources and issuer directory

Reviewed October 12, 2026 in response to the owner's Yahoo, Google and SEDAR question. This is a source assessment and proposed coverage extension, not a new activated feed.

## Verified sources

- [Yahoo Microsoft financials](https://ca.finance.yahoo.com/quote/MSFT/financials/) exposes income-statement data including EBITDA in indexed page content. [Yahoo statistics](https://finance.yahoo.com/quote/MSFT/key-statistics/?ltr=1) also exposes EBITDA and financial ratios. A successful unattended bulk adapter, consistent history and company coverage have not been established by this review.
- [Google Microsoft financials](https://www.google.com/finance/quote/MSFT:NASDAQ?hl=en) currently displays income statement, balance sheet and cash-flow tabs, with quarterly income-statement rows including interest and EBITDA. Displayed figures are not yet verified against Walnut's definitions or demonstrated as a dependable production interface.
- [Nasdaq's directory specification](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs) documents nasdaqlisted.txt and otherlisted.txt, including symbols, security names, exchange and security/test flags. This provides a basis for exchange-listed coverage, not a proof of every OTC instrument. It does not supply a complete verified issuer/IR website directory.
- [SEC mappings](https://www.sec.gov/about/webmaster-frequently-asked-questions) relate ticker, company identity and CIK; [EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) provide submission histories and standardized XBRL facts without authentication. The existing Walnut SEC pipeline already uses this approach. SEC cautions that ticker mapping scope/accuracy is not guaranteed. Preserve listing/share-class identity separately from issuer CIK.
- Fresh Microsoft submissions saved at 18:19 UTC contain ticker MSFT, exchange Nasdaq and SIC 7372, but both website and investorWebsite are empty. Source hash is recorded in [the debt report](sec-debt-aggregate-reconciliation-2026-10-12.md). Website discovery still requires verification; SIC is not automatically the same sector classification as FMP.
- [SEDAR+ public access](https://systems.securities-administrators.ca/onlinehelp/general-help/getting-started/general-public-access/) supports searching profiles and downloading public filings without an account. [Its filing inventory](https://systems.securities-administrators.ca/onlinehelp/filings/filing-inventory/) includes financial statements. It complements EDGAR for Canadian reporting issuers and cross-listed companies, not every U.S. equity. No free bulk API or complete transcript coverage was verified. CSA also documents a Data Distribution Service, but this review establishes neither its price nor access terms.

## Proposed integration

Join exchange symbols and SEC issuer IDs, then maintain verified official company, investor-relations, financial-results, release-feed and transcript URLs with observation time and provenance. Reuse known verified sources first. Validate redirects, issuer identity and any third-party IR hosting before collecting documents. Add Canadian issuer identities and SEDAR+ retrieval only after a bounded retrieval proof.

Prefer original statements and issuer releases for evidence. Evaluate Yahoo/Google as cross-checks or gap-fillers after automated-access, freshness, field, unit, period and coverage tests. Do not treat annual, quarter and trailing figures, reported EBITDA and adjusted EBITDA, or borrowings and lease-inclusive debt as interchangeable. Missing components remain explicitly missing. Transcripts are a separate availability problem.

Run bounded collection in the background and serve prepared cache entries. Reuse filing accessions and document hashes so the same release found on EDGAR, an issuer site and a secondary page does not create duplicate events or alerts. No new schedule, subscription, public source activation or database migration was introduced by this assessment.

## Next verification

Both [Nasdaq-listed](https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt) and [other-listed](https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt) text downloads were successfully opened in this review. Their headers and rows are machine-readable; the observed Nasdaq file includes ordinary shares, ADRs, ETFs, units and warrants. Counts cannot be treated as a count of operating companies without classification and deduplication.

Measure directory joins and verified website coverage against the current supported universe, starting with financial gaps and followed tickers. Prove representative automated retrieval and original-source reconciliation before expanding to a scheduled adapter. Preserve current financial, ranking, monitoring and alert contracts; Massive remains the final price subscription step.
