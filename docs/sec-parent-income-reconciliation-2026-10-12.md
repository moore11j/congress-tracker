# SEC parent-income reconciliation, October 12, 2026

## Finding and implementation

Mastercard's current companyfacts contains consolidated ProfitLoss and explicit income attributable to noncontrolling interests, while NetIncomeLoss has no current observation. Those concepts are not interchangeable. The new generic adapter subtracts explicitly reported NCI from consolidated profit only when the exact period, filing date, accession, currency and source CIK match. Positive, negative and explicitly zero NCI remain distinct from missing data. Reported parent income wins; an explicit conflict cannot be filled. Separately reported nonzero or conflicted redeemable NCI holds the candidate because overlap is ambiguous.

Every derived fact preserves both source observations and its formula. Original inline filing extraction accepts the same standard concepts. Canonical net margin/ROE and future Massive income valuations record the distinct calculation basis, so an existing alert rebaselines when its source calculation changes. Subsequent actual changes still trigger and repeats do not duplicate. Derived annual observations retain the EBITDA closing-filing guard. No public provider flag or preparation cadence changes; ordinary refresh is required to observe propagation.

## Fresh original-source evidence

Four identified, bounded companyfacts/submissions pairs were captured at 2026-10-12T17:16:26.022899+00:00. Original URLs and SHA256 hashes follow. Replays verify every hash and prohibit sockets; they do not use customer data or write production.

- MA-facts.json: `78082e9cbae91bfa8c1480d214bd20c8d35fcbbcc72d71eaf01739c48b1e9511`. https://data.sec.gov/api/xbrl/companyfacts/CIK0001141391.json
- MA-company.json: `d49ae45746c6ab84057a914366dea099f43d8b9945d0fdca04210975e164f5b0`. https://data.sec.gov/submissions/CIK0001141391.json
- AVGO-facts.json: `3bdb6644c12addf1e63c4dc861008d7804fbf3d3f5aa6eb9b97f72c0e89a7adf`. https://data.sec.gov/api/xbrl/companyfacts/CIK0001730168.json
- AVGO-company.json: `b3bb8094e6809c7b3c7d2d7f419363b213bfa5071b40adb2bc2534317637f061`. https://data.sec.gov/submissions/CIK0001730168.json
- GOOGL-facts.json: `a017f26ee88cb4a639d1c1ddb06d53b4afe22f0ac89cabec7cd67c88919fe1c5`. https://data.sec.gov/api/xbrl/companyfacts/CIK0001652044.json
- GOOGL-company.json: `9434407b197d6322de4bece8ad3b69459c8d6ddf3a68526b4b22b02e8175d33c`. https://data.sec.gov/submissions/CIK0001652044.json
- TSLA-facts.json: `d0c2b9288d027a6d735039a6fe874a348b65a7fdbc6c133130666c777c885204`. https://data.sec.gov/api/xbrl/companyfacts/CIK0001318605.json
- TSLA-company.json: `76ef728de78f5a7048154847052d1be4b76d32be9e790f57d8ce14a026be6c3a`. https://data.sec.gov/submissions/CIK0001318605.json

Mastercard's original 10-Q accession0001141391-26-000083 was also reviewed at https://www.sec.gov/Archives/edgar/data/1141391/000114139126000083/ma-20260630.htm. Original SHA25679bc20f3ec864e1d875d4cf33b4c6051dc3841d2ffd090c5b61e5550465df3ca,1900279bytes. The changes-in-equity table distinguishes parent and NCI allocations. The adapter uses explicit numeric SEC facts and never converts printed dashes or absent NCI into assumed zero.

For the current MA trailing period,16.257billion consolidated profit minus26million NCI yields16.231billion parent income. Projected net margin46.26457258501269%, ROE241.10219845513961%, prepared EBITDA22.090billion. These are source calculations, not an assertion of FMP definition equivalence or newly selected public ratios. TSLA reported parent3.804billion and GOOGL244.205billion remain unchanged. AVGO lacks current NCI and remains held. Source receipts parent-income-replay.json and gap-audit.json retain source facts, formulas and holds.

## Compatibility and validation

The separate20-source October9 fixture has manifest SHA256052495c6b28b8f7b3837cdbafbadbe5e67ca51ef78c2d1576124c9ab3487acf8. Against deployed6e7bcac2, every previously available canonical value remains equal and every full repeat is identical. AMT gains net margin31.08348946670932%/ROE91.51698620921628%; CAT gains net margin14.511100108391656%. These dated fixtures are not today's universe coverage. Four fresh pairs independently verify MA additions and unchanged other canonical values. Receipt parent-income-cohort.json records all20 cases.

Final Python3.14.2:189 selected tests pass, two existing FastAPI deprecation warnings. Coverage includes missing/zero/negative NCI, mismatched period/filed/accession/currency, conflicting parent and redeemable observations, reported-parent priority, actual original-inline extraction into the projector, valuation formula provenance, source immutability and repeated output. The actual SEC projector plus free combiner plus in-memory SQLite alert evaluator proves baseline suppression, later real ROE change, repeat deduplication and rollback baseline, without delivery. Initial test-only errors (empty combiner argument and inline fixture concept/provenance fields) were corrected; final selected checks pass. No full-suite claim.

The initial offline baseline read hit Git ownership protection before projection; rerun uses a command-scoped safe-directory argument, with no global Git change. Direct diff/whitespace review is required before release. Local source files are retained in ignored artifacts; these tracked hashes/results are the durable receipt.

## State and remaining gates

October12: Parent-income reconciliation is implemented locally, not deployed or selected. 189 focused SEC/projector/inline/custom-alert/monitoring tests pass on Python3.14.2. Four fresh SEC source pairs and the separate20-source October9 cohort replay preserve all existing values and identical repeats. MA gains parent income16.231billion, net margin46.26457%, ROE241.10220% and prepared EBITDA22.090billion; old cohort AMT/CAT gains are explicitly dated. Missing NCI is never zero; conflicting parent/redeemable allocation stays held. Actual projector/combiner/watchlist evaluator verifies source rebaseline, later real change and deduplication with no email. Production remainsPR613/6e7bcac2, coreFMP/institutionsfalse. Ordinary cache propagation, broad financial/debt coverage, institutions/transcripts/all alert and ranking consumers, finalMassive/globalFMP/billing remain gates. No source selection, host, recurring schedule, subscription, customer email, landing/video change or cancellation. All local handles terminal/no localPG; current source-backed repair makes progress, goalactive.


## Release verified

October12: PR614/8f1d8356 parent-income reconciliation deployed successfully (38284664682). 70 runtime hashes match; Python3.12.15 reproduces the full saved MA parent-income16.231billion and EBITDA22.090billion evidence/repeat. 189 local tests and four fresh plus20 dated source-pair replays pass; existing financial values remain unchanged. Actual no-send alert evaluation verifies formula rebaseline, later real change and deduplication. Feeds0.390/0.595s and AAPL1.311/NVDA1.252s all200 are samples, not sustained-capacity proof. Core remainsFMP. Ordinary cache propagation remains unobserved; broad financial/debt coverage, institutions/transcripts, all ranking/digest/watchlist consumers, finalMassive/globalFMP/billing remain gates. No source activation, recurring timing, host, subscription, customer email, landing/video change or cancellation. All local handles terminal/no localPG; goalactive and concrete release progress.

PR614 source head9088c5a7897accd05859403f679ff1ca71d43c93 merged as8f1d8356100099f577dd3d979a6f0849529247d2. Exact-head secret scan passes; reviewed CI logs retain unchanged Pillow10.4.0 and frontend nanoid/Next/PostCSS/Sharp/source-map-js audit failures. No full-suite, clean dependency audit or automated-review claim. PR attachment was attempted but the existing100-artifact limit prevented attachment; direct link: https://github.com/moore11j/congress-tracker/pull/614.

Runtime receipt parent-income-release-1791826395.json verifies70 hashes and the actual saved MA component evidence under Python3.12.15, including missing-NCI holds and identical repeats, with zero source requests or production writes. Public receipt issuer-staging-public-1791826389.json records all200 readiness/feed/ticker/press responses and four distinct AZO releases. A prior CI log request returned502, then the supported CI tool confirmed the successful deployment; no alternate CI-log channel was used.

Read-only preparation receipt leverage-preparations-1791826195.json still contains older AMZN/MA interpretations. That is not ordinary propagation proof; the existing20-hour observation refresh/24-hour cache remains unchanged, with no forced universe rebuild. Remaining financial gaps include missing interest, D&A, current NCI and debt-definition reconciliation. The source-derived values remain preparation evidence until the public cutover acceptance gates pass.

Checkpoint helper initially hit Windows default text decoding after writing primary AGENTS. The affected file's encoding was reversed, the helper corrected to explicit UTF-8 and rerun without duplicating the checkpoint. Existing primary context and unrelated code were preserved. Final documentation diff contains only the intended checkpoint/report changes.
