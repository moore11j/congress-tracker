# Shared SEC financial document sets, October 17, 2026

## Problem and shared implementation

Some current foreign SEC financial statements are missing from companyfacts even though the complete tagged statement is available in the filing. Its cover and exhibit can share identity, contexts and units. Parsing the exhibit alone loses those resources; parsing only the cover finds no financial figures. This repair discovers and validates that reporting format without ticker eligibility lists.

The shared directory reader checks complete SEC submissions, exact CIK/ticker/accession/form/dates and same-filing HTML URLs. Only current completed 6-K/6-K/A candidates are considered, with at most three candidate covers per acquisition. Primary documents are parsed first; otherwise a uniquely labelled financial-statement link supplies the companion exhibit. Both originals are retained and separately hashed. Final parsing still validates every identity, namespace, context, unit, period and conflicting value. Cover accounting facts are held rather than attributed to the exhibit. Document-set limits remain2MB cover/20MB combined. Primary parser limits remain unchanged.

The production fallback admits only the existing USD US GAAP class with independently available historical balance/duration facts. Existing reviewed native-currency and annual-history readers keep precedence, as do completed domestic reports covering the same or newer period. Native IFRS/EUR parsing is demonstrated by ADSE/ALC originals with an explicit accounting basis; this is not automatic core admission or currency conversion.

Discovery priority uses lagged reporting dates only to order a bounded search. It does not prove a document contains statements or that every newer financial document has been examined. Evidence explicitly records the three-candidate bound and statement_complete:false. Source dates, partial-history labels and withheld unverified EPS remain visible. No global cache reset, schedule, provider selection or resource-size change.

## Complete affected-source assessment

All239 prior held public companyfacts/submission scopes were inventoried. The initial fallback class contains48 scopes. Complete candidate acquisition is terminal48/48 with116 new public SEC requests,20,524,140 retained bytes and no transport hold. Sources are in artifacts/document-set-cohort-oct17/state.json with original hashes; the earlier three explicit cover/exhibit pairs are retained separately. This capture was completed before the TikTok side request and was not restarted.

The primary-first draft regressed SLGL/TSEM because their cover-only parse lacks complete identity. The repaired preliminary fallback permits the final combined parser to validate identity; it does not ignore identity failures in the final statement. Full48 offline replay is repeated after the repair and domestic precedence guard, with zero network or production writes:

| Recovered source | Metrics | Annual rows | Quarterly rows |
|---|---:|---:|---:|
| AER | 3 | 6 | 9 |
| ASC | 4 | 6 | 12 |
| BOSC | 10 | 6 | 0 |
| NAMI | 5 | 2 | 0 |
| SLGL | 8 | 6 | 9 |
| TSEM | 8 | 6 | 9 |
| VLN | 9 | 6 | 12 |

Total47 supported fields across seven sources. Exact repeats pass; ASC uses the complete primary document and six use linked document sets. Replay-final.json takes7.266 local CPU seconds, not production throughput. Remaining41 scopes:32 unsupported reporting/coverage, two untagged/root cases, three cover-size bounds, one ambiguous multi-entity statement set and three internal table-of-contents link cases. B/WNW/TK/TNK/VTEX retained originals demonstrate untagged SGML/HTML; simply accepting their root or internal anchors would not create trustworthy tagged facts. WNW includes acquisition/pro-forma statements and RZLV includes multiple entities, so arbitrary link selection is incorrect.

Twenty-seven complete originals for all seven recovered sources are included in compressed fixtures, plus the initial ADSE/AER/ALC parser originals. The seven-source suite exercises acquisition, full metric projections, historical rows, cache reuse and each supported current-ratio/ROE/revenue-growth alert lifecycle. Recovery/repeat/rollback create no false alert; a synthetic real crossing creates one, repeat stays one, and no email/event is produced. Tests retain wrong identity/date/form/context/namespace/link/source-proof rejection and prior NVS/Shell behavior.

## Existing release acceptance and secondary-source evidence

PR711's six public historical responses now exactly match retained original projections in artifacts/tagged-financial-public-1792205300.json, all200 in0.325-0.440seconds. This closes those sampled current public values, not full-field or unprompted recurring-job attribution. The demand queue receipt at02:47 still had six queued rows despite populated caches; no exact scheduler attribution is claimed. PR710 aggregate receipt1792207014 verifies five of84 ordinary recovered scopes by03:16:54; broader live rotation remains pending.

The bounded Yahoo comparison completed eight requests across ADSE/AER/ALC/AMX with648 observations, no transport hold and minimum71.271second spacing. Current AER/ALC quarterly revenues match SEC originals exactly; ALC matches totalRevenue2.823B, not contractsRevenue2.782B. ADSE Yahoo income remains annual/trailingDecember2025 rather than the newer SEC June2026 half. AMX nativeMXN values have not been reconciled to independent originals. No Yahoo collector or canonical values were activated. Eight successful requests do not establish all-metric equivalence or service reliability.

## Validation and release status

Expanded Linux suite:358passing checks. A later domestic-precedence guard adds one case; the final affected rerun is pending at this checkpoint. The prepared37-file runtime verifier passes locally over all seven complete-source cache/repeat/history cases in9.891CPU seconds with transport forbidden, isolatedSQLite, zero production records/writes/emails. Full239-source no-supplement projection parity against deployedc45f105a is running separately; this cannot prove complete live acquisition.

Application changes are local on codex/sec-document-set-recovery. Review, exact release, installed verification and ordinary acquisition remain pending. Core financials stayFMP; institutional publication, broad source and financial coverage, transcripts/analysts/valuation, all scheduled consumers, sustained capacity, finalMassive, zeroFMPegress and billing closure remain gates. Previous side-request turn completed TikTok retirement but made no migration progress. This turn repairs shared acquisition and produces full affected-source/consumer evidence; goal remains active.

Final pre-release validation: 124 affected checks pass after the domestic-precedence guard, for359 distinct passing cases across the expanded/final runs. Complete239-source no-supplement projection comparison is unchanged239/239 against deployedc45f105a (projector-parity.json). Full48 final acquisition replay remains seven recoveries/47fields with identical repeat values. No whole-universe live-acquisition claim follows from these scoped checks.
