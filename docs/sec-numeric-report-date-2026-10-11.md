# SEC numeric report-date repair, October 11, 2026

ETR preparation failed on its complete June 30, 2026 SEC 10-Q because DocumentPeriodEndDate uses `ixt:date-month-day-year` with `6/30/2026`. The parser supported only the English month-name transform. This prevented the free prepared cache from replacing an expired version-3 result.

## Repair and evidence

Support the numeric month/day/year transform in the already accepted 2020 and 2022 registry namespaces. Match the specified digit lengths and nonnumeric separators, interpret short years in the 2000s, and validate the calendar date. The parsed date must still exactly match the submission report date. CIK, form, context, accounting basis, currency and original-byte provenance checks remain enforced.

Normative source: [XBRL Transformation Registry 4, date-month-day-year](https://www.xbrl.org/Specification/inlineXBRL-transformationRegistry/REC-2020-02-12/inlineXBRL-transformationRegistry-REC-2020-02-12.html), sections 4.53 and dateMonthDayYearType. No generic locale guessing or substitution from expected metadata.

Saved originals captured at 22:51:52 UTC: SEC submissions, companyfacts and the complete 6,438,291-byte filing. Filing SHA256: `8cba3a1c3a58ebd2ecd722493e51e08d840d9c936cb31aa5fbc9c00f3a20a3e9`. Original URL: https://www.sec.gov/Archives/edgar/data/65984/000006598426000283/etr-20260630.htm. All three hashes verified before replay.

Offline actual-source preparation passes using an isolated in-memory SQLite database, with requests/httpx network access forbidden. It prepares period 2026-06-30 and six fields: operating margin 22.897255772358676%, revenue growth 9.548117156171614%, operating margin expansion -4.184659683464975 points, current ratio 0.9117206238176622, free cash flow -2,972,801,000 USD and FCF margin -22.049777088013723%. These are source-derived calculations, not an FMP numerical-parity claim. Repeat result, stored payload and fetched timestamp are identical; only the first run reads the three originals. Zero provider requests, production writes or emails.

Regression reproduces unsupported_report_date before the repair. Afterward, 102 focused checks pass on local Python 3.14.2 across inline parsing, fundamentals, preparation and priority cache refresh. Checks include both registry namespaces, short-year semantics, leap days, invalid dates, untrusted transforms and report-date mismatch. Existing large-filing and issuer isolation checks pass.

Local replay receipt: `artifacts/direct-feeds/free-replacements-2026-10-09/etr-inline-oct11/preparation-replay.json`. Release and production verification are recorded below. Public core remains FMP; this change does not activate a provider, alter prices, schedules or resources, or close wider coverage/consumer gates.


## Deployed and live verified, October 11, 23:04 UTC

PR588 merged as1551526fdf0090563b04fafe806ef1ade569f3f9; backend workflow38182646952 succeeded, including readiness and Premium privacy probes. Automated review found stale implementation status in memory; corrected before merge. No parser findings. Existing unchanged dependency audits fail; secret scan and Vercel pass. PR attachment hit the100-item task limit; use https://github.com/moore11j/congress-tracker/pull/588. Initial merge command used an abbreviated expected-head SHA and was rejected without changing state; full-SHA retry succeeded.

All30 runtime hashes match and both approved hosts retain their sizes. Queue empty; eight HTTP200 checks. Feeds0.426/0.562seconds, fresh AAPL1.639/NVDA1.359seconds, news50items0.360seconds. This sampled post-deployment recovery is not sustained capacity proof. Runtime receipt two-service-verify-1791759783.json.

Guarded live ETR preparation at23:03:36 succeeds with six fields for2026-06-30, version4 and explicit partial coverage. All values exactly equal the saved-source replay. Companyfacts/submissions/filing hashes match the captured originals; inline evidence retains accession0000065984-26-000283, filed2026-07-31, CIK0000065984 and22 reported facts. A second preparation with source-client construction forbidden returns the same payload and stored observation timestamp. Receipt etr-preparation-1791759820.json. No public-core activation, canonical event write or email.

Ordinary22:56 SEC warming also refreshed AAPL and NVDA with deadlines exactly20hours after their source observations. ACN remained due at22:57 and must be rechecked later. All50 priority deadlines matched source observation plus20hours. Receipt priority-warming-runtime-1791759456.json. Later23:01 batch completed10scopes but per-symbol refresh was not inspected.

Next coverage work remains net debt/EBITDA, EBITDA growth, beta, earlier issuer earnings releases and explicit unsupported instruments; then source/publication/consumer gates and final Massive. Saved Finnhub identity probes contain beta and annual/quarterly EBITDA but refer to different securities for BRK-B/CX/TSM. Do not transfer beta across listings, treat five-year CAGR as annual growth, or assume quarterly amounts are TTM. These are candidates requiring source/formula verification, not completed replacements.
