# SPCX registration financial history

October 14, 2026. Deployed as42d1a44b through PR649; ordinary preparation verified. Public core fundamentals and prices remain FMP, institutional publication remains disabled.

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

Review/release, runtime hash/original verification and ordinary preparation are verified below. Wider financial coverage/definitions, all monitoring/watchlist/daily/weekly/ranking consumers, institutions/transcripts and sustained capacity remain open. Massive personal Stocks subscription/validation stays last. Global zero-FMP requests and cancellation/access-end confirmation remain required. No cancellation or email sent.


## Production release and ordinary operation

PR649 source3b9a37d14dce27e63dc3b5b37c2bde9a5063ecfe merged as42d1a44b26d0d1b12ae006b0728c2753b993ac9b. Backend workflow38650668361/job116148368293 completed successfully at22:54:59UTC, including readiness and Premium research privacy checks. Fifteen runtime hashes match. Production Python3.12.15 parses the complete saved prospectus and reproduces the exact accounting bridge, source-period points, six values and identical repeat with network/database access prohibited. Receipt artifacts/coverage-gaps-oct14/SPCX-runtime-1792018552.json.

The next normal22:56 fundamentals batch persisted the new version at22:56:00.710761UTC. A read-only check at22:56:33 proves exact six-value and current/prior/instant/accounting-proof equality to the replay. Receipt SPCX-ordinary-1792018593.json. No forced refresh or public source activation was used. The ordinary Enbridge verification remains separately established at22:46.

The exact release image and unchanged API shared2CPU/2GB and database performance1CPU/2GB are verified in artifacts/preload-receipts-1792018554.json. Public core/prices remainFMP; institutional publicationfalse. Normal22:55 news23/23 and metadata5/5 complete. Stored research16partial/4unavailable and institutional785scope truncated/partial receipts retain their limits; they are not evidence of complete retained products.

Postrelease public checks all return200: Feed0.402s, Insights1.485s with research present, news0.401s/12items, research0.472s/44items, AAPL2.742s andNVDA2.457s. Receipt artifacts/direct-feeds/free-replacements-2026-10-09/insights-incident-1792018546.json. The probe's obsolete hardcoded release comparison is ignored; exact image and hashes are independently verified. These are latency samples, not sustained capacity proof.

At22:56:51 CPU metrics show zero current and sampled two-hour throttling, but API credits decline from10,935 to8,777 across that window with intervening deployments. Sustained capacity remains open. Current balance8,590; receipt cpu-quota-1792018613.json. A separate one-shot local full-document acquisition/projection uses0.4378CPU seconds and85,692KiB process peak RSS on3.12.3, excluding cold imports/network; it is not production capacity evidence. Receipt SPCX-profile.json.

Read-only22:56:53 audit of1,752scopes still finds1,361 baseline versus1,070 replacement classifications:362 lost and71 gained. SPCX now has one verified scoring input but remains unavailable for classification. Its remaining growth/EV/debt fields and wider issuer/source gaps cannot be hidden by the six-field improvement. This audit is presence/consumer conversion, not complete numerical or alert parity. Receipt artifacts/financial-consumer-readiness-1792018616.json.

CI38650476457 secret scan passes. Frontend audit still reports five unchanged production dependency groups (four high/one critical), and backend Pillow10.4.0 lists33 advisory rows. Dependency manifests are unchanged; no all-green/full-suite claim. PR attachment hit the100-identity limit; existing attachments were preserved. One intermediate diagnostic result could not be stored by the tool wrapper; the same deployment was subsequently read to terminal success without redispatch. All local tool handles are terminal.

This closes the SPCX release/runtime/ordinary-preparation gates only. Remaining financial definitions/coverage and all-consumer verification, institutional/transcript publication, sustained capacity, finalMassive validation and global FMP/billing retirement remain required. No source selector, schedule, host size, subscription, customer email, video, landing-page or cancellation change.
