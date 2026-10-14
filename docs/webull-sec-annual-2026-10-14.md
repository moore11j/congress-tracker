# Webull annual financial coverage

October 14, 2026. Implemented and locally verified; release and ordinary production refresh pending.

The public BULL historical financial response was unavailable despite a completed enrichment job. Fresh SEC ticker/submissions identity confirms Webull Corp, CIK 0001866364, BULL ordinary shares and BULLW warrants. Webull files annual Form 20-F reports, which the default domestic statement reader excludes. The reviewed foreign annual reader previously covered only NVO, BABA and TSM.

The repair adds the exact BULL/CIK reporting profile: US GAAP in USD. It recovers four annual periods, 2022 through 2025, using the existing bounded reader, cache and scheduled preparation. BULLW does not inherit the profile. Old unavailable BULL caches fail the existing preparation-version gate and request normal refresh. No new schedule or provider selection.

## Source and semantics

[Webull 2025 annual report](https://www.sec.gov/Archives/edgar/data/1866364/000121390026041653/ea0283691-20f_webull.htm), accession 0001213900-26-041653, filed April 9, 2026, SHA256 `9b64a0c7b4ee3a72cd8fb4acd3bb1e756eb0e238999af201fa45671b07c66890`, 3,814,904 bytes. The reporting-currency disclosure specifies USD and the accounting policy specifies US GAAP. Captured [companyfacts](https://data.sec.gov/api/xbrl/companyfacts/CIK0001866364.json) SHA256 `d719daada14d23a0b63e8be4a4ea1b43ae96f3afd31265d915da937894bca56b`; submissions SHA256 `9e3984d332c730bc86274525f0f954e8de43d39c0cc7a1229868acd7c72f3b35`.

Latest annual revenue is $570,996,806 and net income attributable to the company is $24,770,704, before preferred-share adjustments. Operating cash flow is $566,424,456 and includes customer account movements. The $4,887,272 purchase line includes property, equipment **and intangible assets**. The arithmetic cash-flow difference is explicitly distinguished from issuer-adjusted cash generation. The existing frontend renders the reporting-basis notice. No inference from the unusually large cash-flow figure enters current ratios, scores or alerts.

Quarterly periods, EPS, missing margins, current TTM figures and estimates remain unavailable in this annual-only adapter. Form 6-K quarterly material exists but has not been validated for this consumer. Restatement, identity, unit and same-accession guards remain unchanged. FMP core financials and the other migration gates remain open.

## Checks

- Python 3.14.2: 108 affected tests pass, including actual extracted SEC rows, ordinary-share versus warrant identity, bad unit/form/conflict rejection, stale preparation invalidation, repeat without transport and zero canonical writes. Initial run had 99 passes and nine temporary-directory permission errors; rerun with an isolated writable directory passes all 108.
- Full hash-verified companyfacts/submissions replay prepares four years with two captured reads; repeat is identical with transport forbidden and no changes to canonical events/fundamentals. Revenue, parent income, operating cash and purchase amounts for 2023-2025 reconcile to the captured original statement tables. The 2022 figures remain companyfacts evidence from the prior annual report.
- No additional original request is needed when companyfacts already covers the latest annual period. Diff check passes. No full-suite or rendered-browser claim.

Local evidence: primary `artifacts/bull-financials-oct14/` originals and `replay.json`. The committed fixture is an extraction of unmodified selected standard-concept rows, not a claim to be the full original.

## Reliability observation

At 14:52 UTC the four PR629 runtime hashes still match. Older enrichment types have advanced, including ONDS press completion at 10:02. The current queue still contains 2,686 queued jobs, including 960 quotes, 884 metadata and 577 CIK metadata jobs. Two news jobs are terminal failed. Current row state is not a cumulative execution counter because jobs can be re-enqueued. This verifies continued progress, not complete freshness or sustained CPU capacity. The first SSH observation timed out; its remote process was confirmed absent before one successful retry. Receipt `artifacts/enrichment-runtime-1791989567.json`.

Next: exact release/runtime and ordinary public BULL refresh, then remaining financial and consumer coverage, institutional/transcript gates, sustained reliability, final Massive activation and verified FMP shutdown. No subscription, hosting change or cancellation.
