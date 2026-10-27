# Financial discovery starvation, October 27, 2026

## Confirmed findings

The owner resumed the full FMP migration. Production remains the previously verified `0f9be542` release; no application, production data, source selector, schedule or resource was changed during this investigation.

The bounded read-only public-security audit at 08:19 UTC (`artifacts/sec-coverage-shared-cohort-1793089164.json`) finds 202 cached unsupported scopes, including expired entries. Of these, 143 carry the partial-financial reporting-form hold. These are not the same denominator as the earlier fresh-only audit. Other reasons include 28 without a completed supported filing, 17 without independent metrics, six without current trailing revenue and several explicit review gates. A generic final reason does not establish that the company has no usable source.

The separate directory audit (`sec-directory-gaps-1793089370.json`) identifies 409 absent company symbols. Three freshly downloaded official SEC directories at 08:23 UTC resolve none to a company listing; 202 match the official fund directory. Company/exchange directories each contain 10,429 symbols; the fund directory has 28,616. Exact URLs, complete bytes and SHA256 values are retained in `artifacts/sec-directory-comparison-oct27/`. Fund identification is not permission to drop coverage, relabel instruments or change the acceptance denominator. Existing stored asset-class labels are not authoritative identity evidence.

## Shared discovery defect

`sec_document_sets.candidates` supplies qualified directory entries, but the standard, native-currency and HTML readers independently sort the same candidates and inspect the first three. An issuer with frequent event filings can permanently hide its financial statement behind that fixed window. Repeating the daily refresh does not advance the scan.

Complete-source receipt `artifacts/statement-followup-oct27/hidden-statement-replay.json` demonstrates this for NBIS:

- Fourteen qualifying 6-K directory candidates; the financial filing is seventh in the existing order.
- Ordinary discovery over the retained complete originals returns unavailable.
- The [explicitly linked financial statements](https://www.sec.gov/Archives/edgar/data/1513845/000110465926094844/nbis-20260812xex99d2.htm) in accession `0001104659-26-094844`, filed August 12, cover June 30, 2026. The companion [SEC cover](https://www.sec.gov/Archives/edgar/data/1513845/000110465926094844/nbis-20260812x6k.htm) identifies the exact statement exhibit.
- The unchanged parser verifies USD and US GAAP, extracting 18 observations. Explicit-source core and financial-panel replay repeats exactly, including current ratio `4.029839487029043`.
- Statement SHA256: `56202d2c492293511da556eb8c98afe80bc79312bccfb8c39eb47f3804a55d26`. Full repeated projection SHA256: `c916d88a79755c398c0220ed44b1bca372c9c2020b7cb7fc85e8b3dd825cb8e0`.

This proves a discovery failure and partial recoverability, not complete ratios, a restored financial classification, current directory completeness, live preparation or publication readiness. The diagnostic explicitly selected an already listed source; production has not been taught that accession or ticker. No issuer-specific adapter is proposed.

The public-source investigation fetched eight additional originals within an explicit eight-request budget, without authentication, redirects or retries after denial. Replays then forbade sockets. Original files and URL/hash receipts are in `artifacts/statement-followup-oct27/`. The source directory snapshot is from October 17; a fresh directory and ordinary acquisition remain required for release acceptance.

## Other holds retained

GLBE's sampled statement uses bare dollar symbols without an explicit currency declaration. CHKP's sample also lacks an explicit accounting jurisdiction; GOTU has currency evidence but not the required accounting declaration. Those conditions remain held. BZUN has a financial release outside the initial scan and multiple exhibit labels; its normalization has not been established. No ambiguous currency, accounting basis, parent income, adjusted EBITDA or stale annual figure was admitted to improve coverage counts.

## Next implementation and acceptance

Prepare shared, bounded discovery continuation across the existing three foreign-reader paths. It must preserve existing successful selection, exact source identity, daily/worker resource bounds, refusal of unqualified newer financial statements, and idempotence. Simply increasing the request cap, guessing financial filings from filenames, randomly rotating windows or pinning NBIS would not establish reliable discovery. Persisted progress must remain tied to the current issuer and filing inventory, survive new filings safely, and expose incomplete coverage honestly. Acquisition, prepared cache, public/core consumers and scheduled operation need separate proof.

Use the complete NBIS originals as the regression case, plus event-only covers, amendments, directory changes, competing same-date statements, access denials, interrupted/repeated scans and the retained full-source cohort. Measure added storage, database work and CPU before production activation. This investigation adds no schedule or implementation yet.

Institutional capture and OneDrive transfer remain unexecuted pending the previously requested explicit approval. Core FMP remains enabled. Broad financial and consumer acceptance, sustained capacity, final Massive subscription/validation, zero FMP traffic and billing closure remain open. The goal stays active; this turn produced a new source-backed diagnosis rather than a deployment.

## Local continuation foundation, October 27 at 09:00 UTC

The investigation is now followed by a local implementation on `codex/financial-discovery-continuation`, based on tested source `fc67678a`. This section supersedes the earlier statement that no implementation exists. No new release or production activation has occurred.

`sec_financial_discovery.py` reconciles issuer-bound filing receipts against the current directory and inspects at most three previously unobserved covers per call. New filings and amendments have independent identities; unrelated directory changes preserve progress. Strict bounds cover inventory, source bytes, links and checkpoint bytes. Complete SEC SGML document wrappers require matching form and filename before HTML inspection. Source denials and interrupted batches cannot skip the failed window. Discovery labels are hints, not financial observations or proof of completeness.

`sec_financial_discovery_store.py` persists compact receipts using the existing snapshot table with compare-and-swap protection and short query/lock limits. It closes the database connection before fetching sources, refuses API request contexts, and requires the default-off `SEC_FINANCIAL_DISCOVERY_ENABLED` flag. It has no scheduled caller. The common statement-link extraction was factored from the existing HTML reader without intentionally changing its source selection.

Verification:

- Eighteen complete public originals are retained with URL/hash receipts and deterministic compressed regression fixtures. The fresh NBIS directory has 15 covers; five bounded batches inspect each exactly once. A complete repeat makes zero source requests and preserves the 10,864-byte checkpoint. The linked statement still produces the previously verified 18 observations; missing ratios remain missing.
- Linux Python3.12.3: initial affected suites pass180checks; final discovery/storage suites pass35 after adding six identity/corruption cases. This is186 distinct passing checks across the two runs, not215 distinct tests or a single186-test run. Windows discovery also passes22checks. The initial Windows failure was test-runner environment size caused by a huge parametrized test ID; explicit short IDs repaired it.
- All49 retained source cases preserve exact full acquisition/projection/hold output and request sequences versus baseline. The comparison forbids provider network and production writes. Receipt: `artifacts/discovery-link-parity-oct27.json`.
- Local PostgreSQL16.15 verifies all five durable batches, unchanged repeat, competing insert/update writers and zero checked-out database connections during source reads. Receipt: `artifacts/discovery-postgres-oct27/result.json`; owned schema removed and owned server stopped. No production database access or provider requests.
- Local CPU/storage measurement is retained in `artifacts/discovery-prototype-measurement-oct27.json`. It is a local fixture measurement, not production capacity evidence. Zero in a short batch reflects timer resolution.
- Final source receipts use newline-normalized SHA256: scanner `4f82fd31d77e10f528bafce222246efe55556a7f74381c1dbcfbe76b0a9f2e5f`, store `82329d217b14b375e343a4554fd41a55cefafb99b75b384212f05c751cabcda0`, HTML reader `1d0e71694f117751c22487add38acefefe18d8413bbb0496a81c44a1ae494b21`. The49-case comparison records the raw checkout byte hash separately.

Next is actual integration with the existing bounded financial-preparation worker and the three reader paths. Collection must share existing request/time/load limits, not add unbounded work to page requests or silently double original-fetch budgets. Stored hints must be reconciled with current inventory and full source originals before financial qualification. Preserve successful existing sources, refuse ambiguous newer statements, test amendments and same-date competition, then verify prepared/public/core consumers and ordinary scheduled operation. Merely completing the cover inventory does not establish full statement qualification or coverage. No added schedule, resource, subscription, publication, customer message or FMP selector change is included.

All task-owned verification processes are terminal. The institutional capture remains unexecuted awaiting explicit approval; independent financial integration can proceed. The full migration remains active and incomplete.

## Integrated worker and consumer package, 2026-10-27 09:34 UTC

The continuation is now wired into the existing financial preparation worker and source readers, locally only. Both `SEC_FINANCIAL_DISCOVERY_ENABLED` and `SEC_FINANCIAL_DISCOVERY_READ_ENABLED` default off. No source, schedule, host, subscription or production data has changed. This section supersedes the earlier unimplemented worker/reader checkpoint.

The existing worker can use one ordinary slot for three cover observations OR one full filing qualification, with at most six original documents. Failed or previously discovered scopes qualify for continuation; existing successful unrelated preparations and reviewed issuer policies retain their paths. Pending work becomes eligible after 20 minutes through the existing fairness, batch, lease, CPU/load and source-lock controls. Access denials stop the batch. No extra cron, forced refresh or added host is introduced. Public request contexts cannot perform discovery or restore sources; an incomplete selected scan becomes cached warming rather than an uncaught exception, and its 20 minute expiry queues ordinary background work.

Each qualification re-fetches the complete cover, verifies its retained hash, and uses the existing standard-inline, native-currency or HTML reader according to the source's actual numeric markup. A plain HTML statement is no longer rejected merely because an older companyfacts report used inline XBRL. Qualification receipts contain metadata/hashes, never financial values or raw originals. Selection waits for the current candidate inventory, retains newer unqualified-report holds, respects domestic reporting boundaries and revalidates every selected original. Same-day competing HTML statements must pass the existing exact full-observation corroboration; values are never spliced across reports.

The complete current NBIS replay exposed a second shared parser defect: a printed `2025*` year failed date-anchor recognition. A single trailing footnote asterisk is now accepted while preserved in provenance. Unknown suffixes, multiple markers, extra years and estimated-year labels remain rejected. This restores the companion August 12 earnings release; its supported observations agree exactly with the complete statement. The prior alignment hold was a parser limitation, not evidence that the two reports disagree.

Thirty-one complete public SEC originals are now retained: the two metadata files, 15 covers and 14 exhibits. Thirteen additional explicitly linked exhibits were fetched once, totaling 767,091 bytes, with no redirects or denial retries. All later replays forbid provider transport. Full continuation takes five cover batches and 15 qualification steps; a complete repeat makes no source requests or checkpoint writes. The 25,093-byte checkpoint holds two qualified same-day reports, one older held report and 12 without supported statements. Final restoration reads four originals and preserves current ratio 4.029839487029043 for June 30. This is partial financial recovery, not complete ratios or a validated financial classification.

Verification completed so far:

- 247 affected Linux Python 3.12.3 checks pass; 47 final focused storage/reader/worker checks pass, plus two public-cache checks. This is 258 distinct passing tests across runs, not 296 distinct tests or a single 258-test execution.
- All 49 retained source cases preserve full acquisition, panel, core and request output against the deployed baseline in the five changed parser modules. The additional frozen replay including both top-level acquisition and panel entry points is still running at this checkpoint; do not substitute the earlier receipt for its terminal result.
- Local PostgreSQL 16.15 verifies all 20 progress steps, unchanged complete repeat, competing writes, no connection held during source reads, and the actual gated reader's four-original restoration and core/panel repeat. The verifier checks source stability across the run. The owned schema is removed and the owned local server stopped. Receipt: `artifacts/discovery-integration-postgres-oct27/result.json`.
- Negative tests retain accounting/currency holds, reject changed original bytes and directory identities, refuse conflicting same-day values, preserve transport denial, enforce caller bounds, and keep page requests away from collection. Existing warming pressure/lease/fairness behavior remains covered.

Release and actual scheduled operation remain pending. A default-off release must first pass exact-head review and installed source verification. Bounded collection activation and reader activation are separate gates; neither implies core FMP replacement. Broad financial definitions/coverage, institutional approval/publication, all alert/digest/ranking consumers, sustained capacity, Massive last and zero-FMP/billing closure remain open.
