# Webull annual financial coverage

October 14, 2026. Deployed and ordinary production refresh verified at 15:59 UTC; historical preparation/hold notes below are dated evidence.

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


## Review state and renewed reliability gate

[PR630](https://github.com/moore11j/congress-tracker/pull/630), tested application head `57e83e3faf28df9965aea56fb24382b94fcef039`, is open and unmerged. Secret scan and Vercel preview pass. Required CI-tool inspection confirms unchanged Pillow and frontend dependency audit failures; no dependency files were modified. Attachment was attempted but the chat already has 100 attachments. No production release or public four-year recovery claim is made.

The 14:58 UTC check found exhausted API CPU credits and 18.8-19.5-second ticker reads. The financial release is held while recurring startup cost is addressed, as recorded in the [incident report](insights-outage-2026-10-13.md#october-14-1458-utc-throttling-recurs-after-the-overnight-recovery). FMP remains enabled. The implementation is preserved; current core, quarterly coverage and all remaining cutover gates remain open.


## October 14 refreshed release checks

Refreshed against deployed dffe004e on branch codex/webull-sec-annual. The application diff remains the reviewed reporting profile and explicit basis. Linux Python 3.12.3 passes 46 affected financial/foreign/cache/absence/comparison/warming checks; full hash-verified saved-source replay passes again, with four annual rows and identical transport-forbidden repeat. Earlier expanded 108 checks remain dated evidence. No new schedule or source activation.

At 15:50 UTC the existing release returns Feed 0.395s and Insights 0.467s with research; AAPL/NVDA 2.790/2.416s. News has 42/42 fresh company caches and 23/23 completed scopes. CPU credits recover from 3,489 at 15:40 to 4,392 at 15:50, with zero sampled throttling. This supports proceeding with the bounded existing-source coverage repair; sustained capacity and broader migration remain open. Partial financial/reference job exit codes match explicit coverage statuses, not proof of worker crashes. Production is still dffe004e before this repair's release.


## October 14 coverage and collector follow-up

Read-only cached audit at 15:56 UTC covers 1,751 symbols: 1,098 usable SEC preparations and 1,192 permitted SEC/Finnhub combinations. These are field-presence results, not numerical or methodology parity. Three SEC caches are expired and three missing; 379 symbols are absent from the SEC directory, 254 have no complete trailing-revenue period, and 14 companyfacts sources are absent. Finnhub missing/expired cases remain. Market-derived values are outside this audit; net-debt/EBITDA, cash-flow yield and earnings yield still have retained-field gaps. Core remains FMP. Receipt paired-free-coverage-1791993376.json.

Collector run476 at15:47 records only two discovery errors, both the October12 SEC daily index returning403 for Form4/13F. The official [2026 EDGAR calendar](https://www.sec.gov/submit-filings/filer-support-resources/edgar-calendar), checked October14, lists October12 as closed. Current discovery excludes weekends but requests every weekday. This supports a reviewed closure-calendar fix as the next investigation; it does not justify suppressing arbitrary403s or prove complete filings. The code continues through later dates, so do not claim this one response blocks all subsequent dates. No calendar change has been implemented in this release. Receipt collector-receipts-1791993410.json.


## Production release and ordinary refresh verified, 16:00 UTC

PR630 source d91328409e45ee301dee2e1cae9695e5e1020534 merged as 9d7280e1bdfdb2ffab90abc68eec6ff81d44bbfd. Backend workflow38593321440 succeeded. Eleven normalized runtime source hashes match; both existing machine IDs and sizes are unchanged. The exact-head secret scan and Vercel pass. Required CI-tool logs show unchanged Pillow33-advisory and five frontend dependency-group failures; manifests have no diff and no full CI pass is claimed.

The 15:55:41 public cache miss queued normal preparation. The existing15:59 research warmer completes20 scopes with one requested cache miss and zero canonical writes/emails; public BULL then returns four annual2022-2025 rows. Two200 reads take0.445/0.315s. Full payload repeats are identical, all annual values equal the saved-source replay, reporting-basis notes match, and quarterly rows remain empty. Repeated public payload SHA256286e436d70d171f295e0291ecead843c3fbbe1332ecc72e9a3bb36cfc6855bbf. This verifies the API and ordinary preparation, not a fresh rendered-browser test or broad coverage.

Post-release Feed0.465s, Insights0.427s with research, AAPL2.582s and NVDA2.286s. News42/42 fresh company caches,23/23 processed (22ok/oneempty). Sustained capacity remains open. Artifacts: webull-runtime-1791993328.json, webull-public-1791993567.json and1791993591.json, preload-receipts-1791993577.json, insights-incident-1791993594.json. Bounded observer and all command sessions are terminal; no local PostgreSQL server started.

The PR attachment attempt again hit the chat100-attachment limit. An automatic review initially rejected the PR-description update over unverified destination/sensitive-payload concerns; read-only repository/admin and exact-body inspection established the destination and absence of credentials/customer data/production metrics, and re-review approved the update. No bypass or further user approval was needed. Codex execution is currently working with ordinary usage allowed and28% weekly usage reported; no permanent fix for the historical429 is claimed. Existing FMP goal stays active. No provider switch, schedule/host/subscription change or cancellation.
