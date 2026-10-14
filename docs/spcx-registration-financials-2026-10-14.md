# SPCX registration financial history

October 14, 2026. Implemented and tested locally; not yet deployed. Public core fundamentals and prices remain FMP, institutional publication remains disabled.

## Problem and source basis

SPCX is the current SEC listing for Space Exploration Technologies Corp, CIK 0001181412. Its June 2026 companyfacts has interim observations but lacks the annual history required for trailing financials. This is not the former similarly named ETF.

The [June 12 prospectus](https://www.sec.gov/Archives/edgar/data/1181412/000162828026042639/spaceexplorationtechnologi.htm), accession 0001628280-26-042639, contains audited 2025/2024/2023 consolidated statements and March 2026/2025 interim statements. Original SHA256: 782a77c750517fde51c274def5753456d63e4aba186248e7341bc6970c3dce01. The [August 4 quarterly filing](https://www.sec.gov/Archives/edgar/data/1181412/000162828026052535/spcx-20260630.htm), accession 0001628280-26-052535, SHA256 b21adff93ed92113baf1c1d7a454aa9f1bf2448de5e2fe2db59c808711010280, supplies current June observations. Both originals describe common-control combinations involving xAI and X. Full compressed originals and captured companyfacts/submissions are test fixtures.

Eleven accounting checks reconcile prospectus March revenue, operating income, net income and common-shareholder income against June YTD minus second-quarter amounts for both years, plus December 2025 equity/current assets/current liabilities. Original annual operating and net-income totals reconcile independently. Every selected current-period input matches the complete quarterly original through the production inline parser.

## Implementation and definitions

`sec_registration_history.py` parses only the reviewed complete original. Original and normalized output hashes, exact issuer/listing, filing availability and source periods are enforced. Annual facts retain form 424B4, original table labels, USD million scaling, URL and hash. Other registration facts in companyfacts remain excluded. Current periodic facts and explicit conflicts are never overwritten; changed historical observations require a new accounting review. Newer/amended current filings are held until reviewed, and a stale June period cannot be reused.

The existing trailing-year calculator combines annual 2025 plus 2026 H1 minus 2025 H1. June trailing revenue is USD23.044b, operating income minus3.732b, net income minus8.218b, CFO9.900b and PPE cash purchases42.248b. Free cash flow is CFO minus those reported PPE purchases, minus32.348b, using Walnut's existing definition. It is not issuer-adjusted free cash flow; separate capitalized interest, rebates and finance-lease principal are not silently folded into it.

Six prepared metrics result: operating margin minus16.195105%, net margin minus35.662211%, current ratio5.115377, ROE minus12.410054%, free cash flow minus32.348b and FCF margin minus140.374935%. Common-shareholder earnings remain separate at minus8.889b, with no positive P/E manufactured. No EPS, ADR/share conversion, gross-profit proxy or canonical leverage is introduced.

June 2024 interim history is absent. Revenue growth, operating-margin change and FCF growth therefore remain unavailable. SEC alone supplies only one of the three scoring inputs, so this does not restore complete classification. Historical financial-statement endpoints are a separate reader and are not changed by this core preparation repair. Scope/version changes apply only to SPCX; no schedule, selector, host, subscription or public activation changes.

## Validation

210 selected tests pass on Linux Python3.12.3, including41 new registration tests, financial continuity, inline financials, core projection/preparation, common income and ENB regression checks. Guards cover wrong company/CIK/ticker, availability, future/amended/stale reports, missing/wrong-unit/changed/conflicting inputs, altered original/prepared evidence, unreviewed registration facts and preserved periodic conflicts.

Actual-source isolated SQLite replay passes on Python3.12.3 with networking prohibited. Exact cache persistence/repeat and zero canonical rows during preparation are verified. Real summary conversion exposes its one scoring input honestly. Real watchlist evaluation creates zero data-arrival, repeat or rollback alerts and one later synthetic threshold alert; no emails. Twenty complete dated source controls retain identical outputs against972fdb22. These are scoped replay checks, not proof of all alerts, rankings or production capacity. Receipt: artifacts/coverage-gaps-oct14/SPCX-full-replay.json. Two benign unavailable-observation logs correspond to the initial and rollback states.

## Release gates

Review/release, runtime hash/original verification and ordinary preparation remain pending. Wider financial coverage/definitions, all monitoring/watchlist/daily/weekly/ranking consumers, institutions/transcripts and sustained capacity remain open. Massive personal Stocks subscription/validation stays last. Global zero-FMP requests and cancellation/access-end confirmation remain required. No cancellation or email sent.
