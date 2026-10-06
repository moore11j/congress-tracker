# Institutional research evidence correction — October 5, 2026

## Scope and diagnosis

Owner approved fixing filing interpretation in the generator and reviewing related published briefs. The audit read all 35 published article payloads; 15 were institutional briefs. Three NVIDIA articles already corrected on September 27/October 5 are preserved. Twelve older institutional briefs across AAPL, APP, ASML, AVGO, MSFT, NBIS and NVDA receive explicitly dated corrections, retaining original publication dates, slugs and access.

The existing ingest chooses an amendment as the canonical filing without distinguishing its type. A NEW HOLDINGS supplement can therefore displace a complete report; missing rows become exits. The writer previously trusted cached summaries/change rows, mixed them with feed events, omitted underlying quarter-pair provenance, and its deterministic fallback said "Yes" to accumulation regardless of evidence.

SEC guidance distinguishes replacement amendments from additional holdings: [Form 13F FAQ](https://www.sec.gov/rules-regulations/staff-guidance/division-investment-management-frequently-asked-questions/frequently-asked-questions-about-form-13f). A reported market-value change is also not purchase spending or sale proceeds.

## Generator change

- Candidate managers/security identities come from Walnut's stored positions; these are discovery inputs, not verified claims.
- For institutional questions, a bounded read of primary SEC tables compares the same CIK/CUSIP and share units across adjacent quarters. Restatements replace a base report; NEW HOLDINGS supplements append to it. Unknown amendment types, absent base reports, options, principal-denominated securities, missing/invalid share counts and unusually large changes fail closed.
- Only matched positive holdings become named comparisons. Missing rows cannot establish new positions or exits. No aggregate market-wide net flows or largest-holder ranking is supplied. Related filers remain separate.
- The packet retains both source URLs, periods, counts, deltas and filing dates. Top additions mean the largest additions within this small verified sample, not the whole market. Six candidate managers maximum; further work stops after the bounded collection window.
- Unverified feed events and old cached ownership summaries no longer enter the writer's institutional evidence fields. Other research topics do not incur these SEC fetches.
- No verified pairs blocks an ownership generation before model spend. Old/unverified ownership drafts are a publication hard stop. Named buyers must have their exact share delta beside their name. The deterministic fallback cannot invent bullish accumulation.

This is an editorial safety boundary. It does **not** repair historical institutional events, score inputs or every public Ownership row. Full ingest reconciliation and a guarded historical repair remain a separate open data-integrity task; no historical events were rewritten by this release.

## Primary-source checks and corrections

Original Q1/Q2 2026 tables previously retained in the September 27 audit were recalculated by CUSIP, excluding options and requiring SH units. Amundi's August 26 supplement was reconciled. The new network-backed collector independently reproduced the AAPL comparison against live SEC responses before deployment.

| Amundi holding | March 31 shares | June 30 shares | Change |
| --- | ---: | ---: | ---: |
| AAPL | 73,082,616 | 70,758,208 | -2,324,408 |
| APP | 1,115,419 | 1,332,633 | +217,214 |
| MSFT | 41,675,076 | 46,694,778 | +5,019,702 |
| NBIS | 1,343,954 | 2,002,187 | +658,233 |

These are historical holdings comparisons, not present-day trades. Exact SEC table links and the other reviewed filers are retained in [the twelve correction patches](../backend/app/jobs/data/institutional_brief_corrections_20261005.json). Newest affected articles per stock contain the bounded comparison; older overlapping articles link to it rather than repeating unsupported counts. Existing reviewed NVIDIA comparisons remain untouched.

The maintenance job defaults to dry-run, checks each reviewed article hash, preserves identity/access/date, backs up original payloads on the backend volume and uses optimistic concurrency in one transaction. It sends no email and publishes no new article or social post.

## Verification and deployment

Local Python: 3.14.2; production pins 3.12. All 32 focused amendment, evidence, editorial, correction-preservation, availability and existing SEC-ingest checks passed before rollout; Python compilation and diff checks passed. Broader research tests expose seven pre-existing failures also reproduced with the HEAD baseline module (schema fixture, repair-prompt fixture, campaign planner, NBIS fixture, external facts, structured fallback and cash-flow derivation). The changed availability assertion intentionally retains an institutional missing-data note when only unverified feed events exist. No full-suite pass is claimed.

Released generator code in `ce303c52`; final reviewed correction payload in `2fab6590`. [Fly workflow 37408847032](https://github.com/moore11j/congress-tracker/actions/runs/37408847032) succeeded; all four machines run `release-2fab6590d7fe8d434519cf0d0fdf3f484876d390`. Readiness and database checks passed.

The live NVIDIA collector returned two independently sourced comparisons in a bounded two-manager check: BlackRock added 16,385,212 reported shares; Vanguard Capital Management added 2,265,976. Both retained Q1/Q2 SEC table links and actual filing dates. This validates the deployed input path, not model prose quality or a market-wide ranking.

The twelve-article dry run matched every reviewed hash; the guarded update applied all twelve. Public HTML for all twelve returns 200, contains the dated correction and its existing canonical URL. Production backup comparisons verify original publication dates, identities and access settings unchanged for every article. A second dry run reports zero changes. No email, new article or social publication occurred. The three previously reviewed NVIDIA articles were excluded.

No new model-generated article was produced in this task; manual review of future generated prose remains required. Full historical ingest/event correction remains open. Local public audit/backups and recalculated source pairs are in ignored `artifacts/ownership-quality-2026-10-05`; tracked correction patches and this report retain the durable conclusions.
