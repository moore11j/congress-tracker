# Research quality and activation review — October 5, 2026

## Scope and state

The owner approved the next-step recommendation: review the nine remaining SEC amendment conflicts, review two live writer samples, and inspect the visitor → stock → signup → save journey. Keep the hero and ten-page SEO cohort stable.

Completed the bounded source/impact review and journey audit. Two new review drafts are saved in production, neither scheduled nor published. Small writer fixes and regression tests are **local, uncommitted and undeployed**. Production workers were not modified. Samples were verified in a separate short-lived process using the local fixes. Existing subscriptions/configuration remain unchanged; no email, social post, payment, or new compute purchase.

Baseline: repository `c97c0c5f`; previous backend release receipt `6c5453b2`. This task did not perform a new rollout.

## 1. Nine amendment conflicts: bounded review complete, reconciliation open

Re-read each original/amendment cover and compared source information-table rows using the existing merge guard. All nine covers say **NEW HOLDINGS**, while their tables overlap original positions inconsistently. All nine remain rejected by the current research/ingest merge guard. None was reclassified as a restatement by inference.

| Manager | Period | Stored positions | Stored changes | Holder activity events | Source issue |
|---|---|---:|---:|---:|---|
| Arbiter Partners Capital Management | Q2 2026 | 42 | 32 | 4 | One overlapping security changes shares; cover remains NEW HOLDINGS. |
| Rathbones Group | Q1 2026 | 646 | 0 | 0 | Four overlapping conflicts, including 3,768 versus 8,054,943 shares. |
| BSN Capital Partners | Q1 2026 | 26 | 0 | 0 | Amendment cover says one entry/value zero; information table has 14 rows and eight conflicts. |
| Anson Funds | Q1 2026 | 133 | 116 | 26 | Overlap differs in issuer name rather than numeric amount, but table membership changes. Metadata-only difference does not prove restatement semantics. |
| Zazove Associates | Q1 2026 | 198 | 49 | 48 | 112 overlapping numeric conflicts. |
| Wealthquest | Q1 2026 | 159 | 0 | 0 | 139 overlapping numeric conflicts. |
| NWI Management | Q1 2026 | 52 | 48 | 29 | One conflicting security: 757,500 versus 55,500 shares. |
| Avidity Partners | Q1 2026 | 52 | 51 | 14 | 39 conflicting securities, many reported-value differences. |
| StemPoint Capital | Q1 2026 | 82 | 74 | 14 | One conflicting security: 195,981 versus 525,639 shares. |

Total: **1,390 stored positions, 370 changes, 135 holder-specific events**. All 135 events currently have `feed_visible=false`. A bounded published-brief name scan found no references to these managers. That does **not** establish that old holdings or derived aggregates are clean; historical positions remain unresolved and were not changed here.

Review priority: the recent Arbiter Q2 filing, Rathbones' broad position coverage, BSN's cover/table discrepancy, then Anson's potentially metadata-only guard case. Resolve from explicit filing evidence or design a scoped exclusion with downstream aggregate/score verification. Do not infer replacement totals or rewrite recorded results.

Source accessions, paired with CIK:

- Arbiter: `0001513193` / `0001062993-26-004507`
- Rathbones: `0001351991` / `0001140361-26-024580`
- BSN: `0001911876` / `0001911876-26-000019`
- Anson: `0001491072` / `0001193125-26-229328`
- Zazove: `0001009012` / `0001420506-26-001189`
- Wealthquest: `0001632968` / `0001632968-26-000003`
- NWI: `0001103887` / `0001103887-26-000005`
- Avidity: `0001791827` / `0001791827-26-000004`
- StemPoint: `0001952142` / `0001420506-26-001177`

The earlier [19-snapshot reconciliation](institutional-feed-reconciliation-2026-10-05.md) remains separate and was not rerun or undone.

## 2. Live research samples and reproduced writer defects

Both use the configured `gpt-6.1-sol` model and are available under [Research Briefs → Drafts](https://app.walnutmarkets.com/admin/research-briefs).

| Draft | ID | Final review state |
|---|---|---|
| ANET institutional ownership: Who added shares in Q2 2026? | `rb_1791264460541_5fc511` | Passed; draft, unscheduled. Six matched SEC quarter pairs, named increases and reductions, mixed conclusion. |
| Accenture stock analysis: do cash flow and valuation justify buying ACN? | `rb_1791264576663_415b77` | Passed; draft, unscheduled. Cash flow, valuation, date distinctions and a concrete Financials workflow. |

ANET names State Street (+1,403,812 shares), Vanguard Capital Management (+418,143) and Geode (+276,473), alongside FMR (−4,956,761), BlackRock (−523,799) and Vanguard Portfolio Management (−30,929). It limits conclusions to those six verified filers and distinguishes quarter-end holdings from current trades. Manually changed the overall judgment to Mixed and added ticker Research/free-watchlist navigation with plan/settings qualifications.

Accenture originally turned into another ownership brief despite a cash-flow/valuation request. The cause was concrete: the phrase “different topic from institutional ownership” in additional editorial instructions triggered ownership detection. Local code now derives topic from the requested angle/question/keywords/intent, not incidental editorial instructions.

Other local fixes address false style failures:

- Markdown table separator rows and URL punctuation no longer count as prose dash overuse.
- Normal finance compounds such as free-cash-flow and balance-sheet are not automatically banned.
- Analysis of verified SEC comparison holders counts as specific information; it need not repeat the ticker in every paragraph.

Both original samples triggered these overly broad checks and unnecessary revision calls. Corrected samples pass in an isolated verification process. **The normal production generator still runs the old code until a deployment**, and saving through that older validator can reintroduce false warnings.

Accenture's quoted numbers match its saved evidence packet. Independently checked packet arithmetic: operating cash flow 3,095,399,000 minus capex 250,257,000 equals free cash flow 2,845,142,000; SEC current assets/liabilities round to 1.34. Its May 31 balance sheet, fiscal Q4 figures, October 4 valuation snapshot and October 6 quote are labeled separately. Provider financial inputs were not independently audited against a fresh earnings release. A self-overlap warning referenced this draft's own earlier version; removed only that self-reference during review. Both retain nonblocking missing-body-disclaimer/fallback-image warnings; publication still requires editorial review.

Chrome verified both saved titles and the Accenture editor's passed validation/model. A `?draft=` URL did not automatically select a draft; use the Drafts tab. This navigation issue is recorded, not fixed in this task.

## 3. Activation and measurement

Fresh first-party 30-day report captured October 6 at 05:28 UTC (October 5 local):

| Metric | Observed count |
|---|---:|
| Eligible external accounts, all time | 19 |
| New external accounts / verified new accounts | 3 / 3 |
| Active external accounts in window | 4 |
| Positive live paid invoices / paid accounts | 0 / 0 |
| Zero-value invoices excluded from payment counts | 4 |
| Recorded signup starts / sessions | 4 / 3 |
| Recorded signup completions | 1 |
| Attributed new accounts / unattributed | 2 / 1 |
| Attributed new accounts that returned / recorded a first save | 2 / 0 |
| Upgrade clicks / sessions / checkout starts | 4 / 2 / 0 |
| Google organic sessions → stock-research sessions | 44 → 4 |
| Reddit sessions → stock-research sessions | 34 → 4 |
| Referral sessions → stock-research sessions | 35 → 3 |

Report examined 2,930 events and was not truncated. Direct traffic is heavily affected by unknown/bot/internal contamination; do not turn it into a human conversion-rate denominator. Consent and attribution gaps explain why account counts and telemetry are not interchangeable. This is a small, separate rolling window, not proof of a growth trend or fresh GA4 receipt.

Live checks:

- Homepage NVDA search and Analyze a Stock navigated correctly in Chrome.
- Guest ticker content and paid gates rendered in an isolated browser.
- Follow intent preserved `/ticker/NVDA?follow=1` through the signup URL.
- Signup shows email/password and Google; invalid empty submission has visible validation.
- No real test account, OAuth identity, payment or email was created.
- Local integration test verifies new Free account → first follow → deduplicated second follow, preserving an explicitly disabled email preference.
- Frontend emits signup completion after actual success and first-watchlist event only when the server says `added`; provider receipt remains separately unverified.

Actionable next work, not implemented:

1. Make **Continue to NVDA** (or selected stock) the prominent welcome action when signup already carries that intent. The current welcome page asks the user to search again and relegates the preserved journey to a text link.
2. Resolve follow-copy/preferences mismatch before promising no email subscription: the prompt says following does not subscribe to email, but model defaults enable watchlist email and following creates an active subscription when that preference is true. The new test verifies opt-out preservation, **not** safe default opt-in behavior. No customer preferences changed.
3. Improve the first useful guest research result and inspect source/missing-financial-data wording while preserving entitlements. Guest NVDA showed many upgrade gates, missing cash-flow data and unavailable provenance text.
4. Verify the next legitimate consented conversion at the provider. Do not manufacture a paid transaction or claim full live OAuth/signup/follow coverage from component tests.

## 4. Checks, deployment and observation

- Backend Python 3.14.2: 30 focused editorial/ownership/signup tests pass. Separate SEC snapshot coverage passed in the initial 29-test group.
- Frontend: 20 focused signup/acquisition/follow/analytics checks pass. Updated the signup test harness to load the real auth-recovery dependency; no frontend application behavior changed.
- Broader brief suite: 110 pass, seven fail. All seven failures reproduced against unchanged HEAD in an isolated module: schema session bind fixture, manual-generation repair fixture, campaign replan fixture, NBIS readiness, revenue discovery fixture, CXW forward-P/E fallback and SEC cash-flow YTD derivation. No full-suite pass claimed.
- Live saved draft status/validation verified; packet arithmetic reviewed. No backend deployment or commit performed.
- Local artifacts: `artifacts/next-steps-2026-10-05/` (ignored, operational evidence only). This tracked report preserves durable conclusions without customer records or credentials.
- Hero, sitemap submissions and existing ten-page cohort unchanged. Existing weekly checks October 12–November 2 remain the observation plan; this audit does not promise indexing or ranking improvements.

Next release candidate is the small topic/style fix with its regressions. Next conversion change should address the selected-stock handoff and truthful email preference behavior. Historical amendment resolution remains a separate source-integrity task.
