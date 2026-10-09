# Walnut Markets product roadmap

**Updated October 4, 2026.** Code baseline: `80aaa7f7` (local and GitHub `main` matched at review). This is the current planning source; [AGENTS.md](../AGENTS.md) holds working context and [the context log](context-log.md) records subsequent tasks.

## What changed since the previous roadmap

The main roadmap had not changed since **February 11, 2026** (`7b5b097a`). The separate strategy-monitoring roadmap was last changed on **August 2** (`1fb8d8f0`). September feature/release reports and October fixes are much newer. The original main roadmap is preserved [here](archive/roadmap-2026-02-11.md).

Walnut now has a broad stock-research product with Free, Premium and Pro access. Many previously planned capabilities have implementations: institutional and contract data, confirmation scoring, monitoring, strategies, backtests, research, billing and growth tooling. The next phase should make the existing discovery → research → save/monitor → return/upgrade journey reliable and measurable.

This review inspected code, test sources, Git history and dated repository reports. **Implemented does not mean production-verified today.** No fresh live-product audit or application test run was performed for this documentation update. Priorities and acceptance targets below are recommendations, not delivery-date commitments or permission to implement/deploy every item.

## Capability inventory and evidence

| Area | October 4 assessment | Evidence and remaining work |
|---|---|---|
| Congress and insider feed | Implemented; maintain integrity | [Events API](../backend/app/routers/events.py), [public activity](../backend/app/services/public_activity.py), [Congress eligibility](../backend/app/services/congress_outcome_eligibility.py). Preserve filing versus transaction dates, identity, corrections and freshness. February's “insiders in progress” is obsolete. |
| Member/insider/institution profiles and outcomes | Implemented; coverage needs continued attention | [Member performance](../backend/app/services/member_performance.py), [outcome ledger](../backend/app/services/outcome_ledger.py), [integrity audit](outcomes-integrity-audit-2026-09-03.md). Do not promise universal coverage or tradable historical alpha. |
| Institutional holdings / 13F | Implemented with paid boundaries | [Institutional API](../backend/app/routers/institutional.py), [activity service](../backend/app/services/institutional_activity.py). Delayed holdings snapshots are not a real-time trade tape. |
| Government contracts and departments | Implemented | [Contract service](../backend/app/services/government_contracts.py), [department pages](../frontend/app/departments/page.tsx), [ingest runbook](runbooks/government_contracts_ingest.md). Verify award/action dates versus reporting dates and entity attribution. No longer a placeholder. |
| Ticker intelligence, financials, valuation and comparison | Implemented; reliability follow-up | [Ticker page](../frontend/app/ticker/[symbol]/page.tsx), [decision layer](../backend/app/services/ticker_decision_layer.py), [valuation](../backend/app/services/ticker_valuation.py), [compare](../frontend/app/compare/page.tsx). Cold navigation/search repaired in `06f3990e`; measure remaining gaps. |
| Confirmation and cross-source evidence | Implemented, versioned; predictive value unproven | [Scoring](../backend/app/services/confirmation_score.py), [canonical context](../backend/app/services/confirmation_context.py), [agreement/coverage decision](confirmation-agreement-coverage-2026-09-26.md). Current code supersedes older methodology headers; `512875a8` excludes stale macro inputs. |
| Screener, saved screens and Top Stock Ideas | Implemented with server-side tier projection | [Screener](../backend/app/services/screener.py), [ranking](../backend/app/services/top_stocks.py), [access projection](../backend/app/services/ranking_access.py). Entire cached universe scored before ranking; ties use score, market cap, average volume, symbol. Web results cap at 10. September 26's older Pro-25/acceleration description is not the current web contract. |
| Watchlists, custom rules, inbox and digests | Implemented; delivery quality needs continuing verification | [Rule API](../backend/app/routers/custom_alert_rules.py), [monitoring](../backend/app/services/monitoring_alerts.py), [Top Ideas delivery](../backend/app/services/top_ideas_digest.py), [runbook](runbooks/email_digest_delivery.md). Preserve opt-in, tier limits, deduplication and method-change rebaselining. |
| Curated strategies and following | Deployed repairs; current prices and opted-in receipt verified | October 4 [release `9fbd61c1` receipt](strategy-reliability-audit-2026-10-04.md): 20 models refreshed, default 25/hard cap 50, 84 valid matured positions priced and 30 pending session. Eighteen accepted emails; both named strategies confirmed in inbox. Continue scheduled-run/source coverage observation; historical curves and closed-trade gaps remain separate. |
| Backtesting and performance assumptions | Partial against realistic-execution promise | [Engine](../backend/app/services/backtesting/engine.py), [models](../backend/app/services/backtesting/models.py). General v1 assumptions explicitly exclude costs and slippage. Strategy metadata does not fulfill scenario-band/market-impact criteria. |
| Public research briefs and editorial controls | Implemented; ongoing evidence-quality work | [Brief service](../backend/app/services/research_briefs.py), [editorial logic](../backend/app/services/research_editorial.py), [correction/deployment evidence](seo-content-evidence-2026-09-27.md). Citation display and corrected filing comparisons exist; retain claim-level source/date review. |
| Research Memory and Company Developments | Partial end-to-end product | [Thesis service](../backend/app/services/research_memory.py), [matching](../backend/app/services/research_claim_matching.py), [pilot](research-memory-live-pilot-2026-09-20.md). Premium creation, structured theses, evidence/matches and market context exist. The serialized phase notice still says continuous monitoring is future work; activation alone does not close the gap. |
| Options / retirement calculators | Implemented educational tools | [Options documentation](options-calculator.md), [options API](../backend/app/routers/options_calculator.py), [retirement UI](../frontend/components/tools/RetirementCalculator.tsx). Keep assumptions and data limitations visible. |
| Historical options activity / live options flow | Historical sampling implemented; live directional flow deferred | [Activity service](../backend/app/services/options_activity.py), [flow adapter](../backend/app/services/options_flow.py), [plan copy](../frontend/lib/planBenefits.ts). Unsigned call/put premium cannot establish buying intent; historical activity cannot confirm direction. |
| Billing, access and upgrade journeys | Implemented | [Entitlements](../backend/app/entitlements.py), [accounts/billing](../backend/app/routers/accounts.py), [September 30 release record](conversion-release-2026-09-30.json). Validate role/plan/downgrade paths without weakening server enforcement. |
| Acquisition and paid-conversion measurement | First-party reporting implemented; external verification incomplete | [Product analytics](../frontend/lib/productAnalytics.ts), [conversion follow-up](analytics-conversion-followup.md), [HeyCatch record](heycatch-conversion-fixes-2026-09-26.md). Dated reports leave GA4 paid credentials/acknowledgement and provider event/funnel receipt unresolved; current configuration was not inspected. |
| SEO and public discovery | Implemented with documented repairs; growth outcome unproven | [Public-render release evidence](seo-content-evidence-2026-09-27.md), [SEO snapshots](../backend/app/services/seo_snapshots.py). September 27 verified live render and sitemap repairs; September 30 fixed a ticker soft 404. Measure fresh indexing/cohorts rather than repeating completed repairs. |
| Research/tutorial social videos | Implemented operational tooling | [Automation](../backend/app/services/growth_video_automation.py), [tutorials](../backend/app/services/growth_feature_tutorial.py), [layout/mix](social-video-layout-and-mix.md). October 4 improved readability/date claims. Tie visual/source review and delivery to actual publication authorization. |
| Customer API keys / outbound webhooks | Deferred; entitlement placeholder only | [Plan copy](../frontend/lib/planBenefits.ts), [strategy sub-roadmap](strategy-monitoring-roadmap.md). Internal APIs and Stripe/Postmark callbacks are not customer developer access. |
| Lobbying, dedicated dark-pool tape, enterprise organizations | Deferred / no complete product found in reviewed paths | Carry forward as discovery options only. Institutional holdings and market pressure do not satisfy dedicated dark-pool ingestion. No delivery date or available-plan claim. |

## NOW — trust, reliability and measurable activation

### R1 — Verify core journeys and data freshness (P0)

**Starting point:** October 2–4 changes repaired ranking coverage, market data, stale macro evidence, cold ticker loading and search recovery. Older reports record database-health incidents; they do not establish a current outage.

**Work:** establish a current baseline across known/cold/dotted symbols, search, ticker, ranking, public evidence and save/follow. Inspect source/job freshness and API/database recovery. Fix failures reproduced on the current release.

**Done when:**

- Dated desktop and effective 390px mobile checks cover guest, Free, Premium and Pro using safe fixtures/test accounts; no blank-page or horizontal-overflow failures in critical paths.
- Cold, cached, unavailable and provider-timeout cases show useful content or honest recovery states; public endpoints do not leak private fields.
- A report records p50/p95 latency, errors and data age over a defined seven-day window, with sample size and environment. Each source has an expected refresh cadence and stale/failure threshold.
- Ranking and ticker scores agree under the same methodology/context, incomplete universes are disclosed, and stale macro data cannot contribute confirmation. Regression cases cover each repair.

**Dependencies:** deployed revision, source schedules, safe role fixtures. **Outcome:** fewer failed first stock checks and confidence in recency.

### R2 — Close acquisition and paid-measurement gaps (P0)

**Starting point:** September 30 reporting separates stock-before-signup, saves, returns, checkout and verified payments. Earlier provider setup is partly unresolved; first-party reporting must remain useful independently.

**Work:** reconcile notes with current settings, validate anonymous → signup → first save, and compare authoritative payments with eligible provider events. Resolve the GA4 prerequisite through the appropriate owner decision if it still applies.

**Done when:**

- One safe deployed test journey preserves ticker/follow intent, consent and acquisition across marketing/app/OAuth and records each expected step once.
- Reports reconcile eligible external accounts and positive live Stripe payments, including internal/test exclusions, event-cap warnings, unattributed counts and consent limitations.
- The next legitimate consented paid event has documented first-party/provider reconciliation, or a named outstanding blocker. No real purchase or historical replay is fabricated for verification.
- HeyCatch funnel predicates and GA4 receipt are checked against provider evidence; local SDK attempts are not labeled received conversions.
- A clean baseline reports counts plus rates for activation, D1/D7 return and paid conversion. Set improvement targets after adequate baseline data.

**Dependencies:** R1, safe billing test environment, provider access/owner acknowledgement where required. **Outcome:** prioritize using actual activation and revenue evidence.

### R3 — Keep research claims, SEO and distribution accurate (P0/P1)

**October 4 creative/editorial follow-up — deployed `fdafb86f`:** the
[writing and search-demand report](research-editorial-quality-2026-10-04.md)
records a Sol research default, shared writing contract, historical demand
comparison and source-bound video direction that shows the finding first.
108 focused tests and TypeScript checks pass; broader pre-existing test failures
are documented. Vercel/Fly rollout, effective API/cron/video models and Sol API
access were verified October 4. Two voice auditions completed; live generated
writing evaluation and voice selection remain outstanding. The earlier
[video comparison](social-video-layout-and-mix.md#october-4-creative-quality-audit-and-local-comparison)
is an offline prototype; retained Zeely footage still needs presenter-crop repair.

**Work:** maintain repaired public pages, review current research/source-date claims and video output, and measure post-release discovery. Grow useful examples around the stock-analysis journey.

**Done when:**

- A fixed public ticker/brief sample shows initial HTML evidence, correct canonical/indexability, eligible sitemap membership and equivalent public access for humans/crawlers.
- Material claims in sampled briefs/videos are traceable to a source and period; award, filing, publication and future-milestone dates are distinguished.
- Video output passes real mobile caption/headshot/control-overlay inspection before publication under the existing authorized workflow.
- Search Console and acquisition reports record actual crawl/index status and comparable-period outcomes; rendering fixes and impressions are not presented as conversion uplift.

**Dependencies:** R1/R2, source evidence, editorial tools. **Outcome:** defensible research and qualified discovery without speculative homepage rewrites.

## NEXT — complete retention and research depth

### October 4 ticker UX and Signal Mixer — deployed, partial capability

The owner authorized the competitive follow-up and ticker simplification. The
[implementation and validation report](ticker-ux-and-signal-mixer-2026-10-04.md)
records a chart-first layout, in-chart ranges, clearer expandable research
categories, a sourced fundamentals snapshot and a Premium historical Signal
Mixer with explicit source timing, cost inputs and 30/90/365-day SPY comparisons.
Release `bfb7602d` deployed October 4; Vercel and Fly workflow `37242718354`
succeeded, with both frontend versions, all four backend images, readiness,
anonymous mixer denial and signed-in ticker layout verified. Source baseline:
`98e65467`. Live AAPL cash flow remains null and the public projection hides
provider identity; source wording/data completeness remain follow-up work.
Broad live study windows initially hit explicit source/company limits; the
standalone follow-up below repairs those limits and verifies a useful default.
The mixer uses contract observation dates where public timing is unknown and
exposes sample/coverage limitations. Saved mixer rules and live alerts remain
unimplemented; general portfolio execution realism in R6 remains partial.
Advanced Charts migration and historical forward-P/E averages remain deferred.

### October 4 follow-up — standalone Mixer and tool formatting, deployed

The owner reported that even a short study failed and requested a dedicated Mixer
page plus calculator-style Backtesting, Compare and Screener formatting. The
[repair and deployment report](signal-mixer-and-tool-refresh-2026-10-04.md)
replaces raw-record/company rejection with complete streamed queries and price
batches, separates disclosure dates from outcome maturity, and adds explicit
no-match/pending states. The analyst-upgrade → insider-purchase default completed
a read-only production-data check in 15.71 seconds: 55 matches, with 38/26/10
completed 30/90/365-day outcomes. The September short window completed in 2.33
seconds. These timings are candidate-service checks. Release `ab8642a7` then
deployed successfully on Vercel and Fly (workflow `37246391059`); signed-in live
default and September-window runs returned 55 and seven matches respectively,
without capacity errors. Both frontend revisions, four backend images,
readiness, old-link redirect and anonymous denial were verified.
49 backend and 22 frontend focused tests passed; local desktop/mobile layouts
were reviewed. The live Free/Premium account matrix remains unchecked. Contract
observation coverage, broad longest-window latency and realistic portfolio
execution remain open; this does not complete R6 or enable live rule monitoring.

### R4 — Finish Research Memory monitoring (P1)

**Work:** connect permitted evidence ingestion and matching to a reliable per-thesis monitoring lifecycle and useful alerts. Improve source completeness before claiming comprehensive coverage.

**Done when:** create → review/edit → activate → new evidence → support/contradiction/invalidator match → inbox/opted-in delivery → pause/resume is demonstrated in an isolated end-to-end test. Pausing stops delivery; retries do not duplicate notifications; access/ownership and delivery preferences hold. UI shows last successful check, source coverage and failure/staleness. A labeled sample includes risk, guidance, product milestones and contradictory evidence, with precision/coverage findings recorded. Verify populated mobile UI visually.

**Dependencies:** R1, worker locks, source availability/permissions, matching quality and existing delivery systems. **Boundary:** storing `status=active` is not continuous coverage.

### R5 — Improve monitoring and strategy-follow usefulness (P1)

**Work:** build on existing watchlists, rules, saved screens, confirmation alerts, strategy events and delivery records. Evaluate noise and failed/retried delivery before adding channels.

**Done when:** follow → eligible event → inbox/email → useful return action is verified; downgrade, opt-out, deleted source and method-version changes behave correctly. A report distinguishes queued, attempted, delivered and acted-on events and records duplicate/failure rates. Published holdings include as-of/refresh state; only eligible active prospective versions generate actionable follow events.

**Dependencies:** R1/R2 and existing subscription/scheduler machinery. **Boundary:** no broker-order or copy-trading launch.

### R6 — Add realistic execution assumptions and fresh validation (P1)

**Work:** extend backtests with fees, slippage and a bounded liquidity/impact model; produce scenario comparisons with versioned inputs. Keep predictive research separate from descriptive scoring.

**Done when:** reproducible runs expose disclosure-based entry timing, benchmark, universe, data/code versions, missing-price/corporate-action treatment and configurable costs. Base/best/worst scenarios include sensitivity checks. Transaction-date simulations are labeled theoretical. Any predictive score proposal has a prespecified fresh forward evaluation with frozen rules; the consumed September holdout is not reused as untouched evidence.

**Dependencies:** licensed prices/history and point-in-time data. **Boundary:** a better-looking curve or stored cost fields do not establish predictive returns.

## LATER — reduce dependency risk and expand selectively

### R7 — Reduce FMP cost with a smaller core product (implementation started)

**October 8 guarded SEC corrections, local:** [repair package](form4-cutover-validation-2026-10-08.md#guarded-form-4-correction-package-october-8) qualifies 76 complete filings/228 existing-row corrections, with identical repeat state. 92 tests and six private PostgreSQL scenarios pass; final exact-code checks, deployment and fresh live repair remain pending. Ambiguous filings are held in full, originals archived and alert read state retained. SEC/FMP ownership and billing unchanged.

**October 8 SEC availability and alert validation:** [current report](form4-cutover-validation-2026-10-08.md), `5afef5fb`, deployed/all four workers and 181 tests verified. Fresh 325-filing replay corrects 334 records locally and projects 98 transactions without repeat growth; 74 source/eight publication holds remain. Ninety are older IRIX purchases newly disclosed October 7. Actual 98/12/8 no-send activity/monitoring/daily previews pass, including separate trade/filing labels and correct arrival-window alerts. SEC ownership/corrections remain unapplied; broader prices/rankings/content and all-cadence validation remain gates. Both Congress scheduled publishers now observed, zero new events/known holds and legacy suppression verified. Source backlog, Starter and billing remain open.


**October 8 House production cutover:** [live report](house-source-cutover-2026-10-08.md), `a93232dc`, all four workers verified; 106 checks pass. Both Congress feeds select official generation 2. House adopts seven filings, corrects thirteen dates with availability/IDs/history retained, and repeats without new trades. No-send digest/Top 10/price checks pass; protected 42 positions, thirteen outcomes/jobs and one alert unchanged. Three House digital conflicts/three scans and the Senate bond exception remain held. Post-cutover :17 collection and first :29 publication are observed; known holds remain, no new events, and original times/fingerprints/zero recorded legacy calls verify at 00:41 UTC. SEC publication, fresh whole-product inputs/alerts, Starter and billing remain incomplete.

**October 8 availability release, deployed:** [date/availability report](congress-availability-dates-2026-10-08.md) rehearses four House filings/12 trades without backdating entry dates or changing saved history/alerts. Six isolated PostgreSQL cases and 378 exact-release checks pass; `26541a80` deployed/all four workers/readiness/access/eight hashes verified. Read-only live plans qualify three staged filings/11 trades; no public House corrections. First scheduled Senate publication now observed at 23:29 with zero inserts/emails and known bond filing held (partial/exit 1), superseding the earlier pending status. House ownership/source conflicts, fresh replacement-driven rankings/all alerts, Starter and billing remain gates.

**October 8 House scan release:** [review and live receipt](house-scan-review-2026-10-08.md), `0f3e2171` deployed/all four workers healthy. Exact-source reviewed transcription resolves four stock rows with separate ticker provenance; 102 tests pass in both checkouts and live source/repeat checks pass without public changes. Four date-only candidate filings/12 trades require preservation of observed availability before correction; remaining scans/canonical conflicts keep House on FMP. Senate remains official; first scheduled publisher pending. Full shutdown, Starter and billing remain unfinished.

**October 8 live Senate cutover:** [release/repair receipt](direct-congress-repair-rollout-2026-10-08.md), `9079343c`, verified on all four workers/readiness. Same-day Congress collection default is live, 65 final checks pass. Official Senate selected generation 2; five source stock rows adopted with existing IDs, ten duplicate events/thirteen extra transactions archived, five queued jobs retired; saved positions/runs unchanged and repeat zero changes. Bond-only mismatch held. Legacy Senate request path skips under transport barrier. Public APIs pass; anonymous cache correction `b57c1af5` deployed and rendered correctly. Actual five-event no-send replay/current-score refresh pass with Top Stocks/prices/history preserved. First scheduled publisher remains pending. House/SEC, fresh replacement rankings/prices, research-feature scope and billing are still incomplete. Massive snapshots 403 at 22:26:58 UTC; Starter/login pending. This is a partial production cutover, not full FMP shutdown.

Use the [FMP exit audit](fmp-exit-audit-2026-09-26.md) to sequence direct Congress/SEC/contract sources, local fundamentals and licensed market inputs. The audit did not establish a free full-parity replacement.

**Done when:** each migrated source has discovery/amendment handling, identity mapping, coverage/freshness comparisons, price/corporate-action parity where applicable, usage rights, cost estimates and rollback. Compare old/new inputs before cutover and document intentional feature reductions. Do not cancel FMP first.

### R8 — Customer API and outbound webhooks (P2)

Extend the canonical strategy event stream after monitoring/delivery is dependable; follow [the strategy sub-roadmap](strategy-monitoring-roadmap.md).

**Done when:** scoped revocable keys, per-account rate limits, entitlement-safe pagination and versioned schemas are tested. Webhooks have signatures, timestamp/replay checks, secret rotation, bounded retries/dead letters, delivery logs and authorized replay. Payloads retain source/run/methodology identifiers and timing assumptions. Change future-feature copy only after end-to-end validation.

**Dependencies:** R5, developer demand, redistribution constraints. Brokerage execution remains outside scope.

### R9 — Additional datasets and enterprise features (discovery only)

Consider live directional options flow, dedicated dark-pool feeds, lobbying and organization controls only with customer demand, affordable permitted data and clear interpretation. Historical options volume is not directional trade intent.

**Done when, before implementation scheduling:** each proposal has a user problem, source/rights/cost review, coverage/latency sample, entitlement design, success metric and explicit go/no-go decision. No plan currently promises delivery.

## Measurement and release rules

### October 4 competitive proposal assessment — recommendations only

Source review at `c7bb0b56` clarifies the owner's TradingView/Koyfin proposal; this is not an approved implementation schedule or a fresh production verification.

| Proposal | Current source evidence | Recommended extension / dependency |
|---|---|---|
| Embedded technical charts with alternative-data overlays | [PremiumTickerChart](../frontend/components/ticker/PremiumTickerChart.tsx) already uses TradingView Lightweight Charts, Congress/insider/contract markers, moving averages, RSI and MACD. | Improve existing event exploration/discoverability first. Consider Advanced Charts only for demonstrated drawing/indicator needs, subject to suitable access terms and integration costs. |
| Contextual fundamentals | [Financial panel](../frontend/components/ticker/TickerFinancialsPanel.tsx) already includes revenue trends, FCF, leverage and source-qualified forward P/E; [valuation](../frontend/components/ticker/TickerValuationTab.tsx) supplies model context. | Proposed compact summary near existing evidence: growth, cash generation, leverage and valuation, each with source/as-of/missing states. Historical forward-P/E comparisons require comparable historical estimates and coverage; do not substitute trailing multiples silently. |
| No-code signal mixer | October 4 [deployed event study](ticker-ux-and-signal-mixer-2026-10-04.md) supports trigger + confirmation + time window, costs, SPY and horizon outcomes. [Deployed follow-up `ab8642a7`](signal-mixer-and-tool-refresh-2026-10-04.md) adds a dedicated page and repairs capacity/default usability; live default and short-window studies passed. | Observe concurrency and broad source/window coverage before expanding to save/monitor. R6 dependencies still include historical coverage, realistic portfolio execution and fresh validation. Retain sample sizes, benchmark-relative returns, losses and overlap diagnostics. Do not promise instant arbitrary combinations or proven alpha. |

Official sources checked October 4: [TradingView product comparison](https://www.tradingview.com/charting-library-docs/latest/getting_started/product-comparison/) says widgets cannot accept custom data and libraries supply no market data; [FAQ](https://www.tradingview.com/charting-library-docs/latest/getting_started/Frequently-Asked-Questions/) excludes Pine Script; [introduction](https://www.tradingview.com/charting-library-docs/latest/introduction/) describes public/non-paywalled and attribution conditions for free Advanced Charts. These require evaluation before adopting it for paid Walnut surfaces. [Koyfin fundamentals documentation](https://www.koyfin.com/help/global-equities-fundamentals-valuatiion/) supports its depth in statements and valuation, but does not establish that competitors cannot reproduce Walnut workflows.

Recommendation: preserve R1/R2 foundations, favor the compact fundamentals extension for near-term scope, and develop the mixer incrementally with R6 and R5. No pricing change, chart migration or community launch is approved by this assessment.

| Outcome | Measure | Acceptance / interpretation |
|---|---|---|
| First useful research | External sessions reaching ticker/evidence; elapsed time | Retain original <60s median as an aspiration; establish baseline and denominator first. |
| Activation | New eligible accounts saving/following a stock | Report count/rate, acquisition and plan; test intent continuation. |
| Retention | D1/D7 returning activated accounts | Define windows/eligibility; separate internal traffic and missing telemetry. |
| Paid conversion | Positive live Stripe payments | Reconcile first-party records; distinguish grants, trials, test invoices and provider receipt. |
| Reliability | Journey success, p50/p95 latency, data age, job failures | Record environment, sample, cadence and thresholds; unavailable is not zero activity. |
| Alert usefulness | Eligible delivery → return/action, failures, duplicates | Respect consent/tier; attempt is not confirmed inbox receipt. |
| Research quality | Source/date traceability and labeled match precision/coverage | Show limitations; no return-prediction claim. |
| Search/distribution | Indexed pages, qualified visits, resulting saves/signups | Compare equivalent periods; live eligibility is not actual indexing. |

Move an item to **complete** only with acceptance evidence, relevant regression checks, effective mobile QA where needed, and preserved source/access/date semantics. Record tested revision and deployment evidence separately. Historical reports contain known suite failures; do not describe the whole project as green without a current full run.

After each relevant prompt, update affected roadmap statuses and the memory/log. Preserve dated reports as history and explain superseding evidence rather than copying old “not deployed” or “not implemented” headers into current status.

## R7: FMP replacement and operating cost, October 8

**Partial, shadow rollout prepared.** Owner authorizes continuing through reliable FMP shutdown, selects Massive personal Stocks (Starter pending), and excludes Quiver. [Scoped release](direct-feed-shadow-rollout-2026-10-08.md) adds isolated hourly official collection with public publishers disabled. Release dbf0b1b9 is deployed with successful schema/health checks; first live collection stores ten revisions without public writes. A reproduced House PDF dependency mismatch is being corrected; scheduled reliability remains unverified. Retain FMP until source publication, fresh prices/fundamentals/rankings and no-send monitoring/watchlist/daily/weekly parity pass. Then verify zero FMP requests and billing cancellation before the approximate November 1 monthly renewal.

**Verified 20:58 UTC:** corrective c2b97e80 deployed, all four images and repeated schema/health checks pass. Both live House PDFs now extract; one existing-record mismatch held. Twelve revisions with zero-repeat growth/public writes, Senate unattended discovery stable. 109 focused checks; Massive still 403. Scheduled observation and full retained-product cutover remain partial. [Evidence](direct-feed-shadow-rollout-2026-10-08.md).

**October 8 R7 ownership follow-up:** [scoped backend release](feed-ownership-rollout-2026-10-08.md) prepares all four feed writer guards, disabled publishers, queue/SEC-label protection and the public member directory. 437 focused checks plus one portfolio check pass; one baseline assertion excluded. Not yet deployed/activated. Scheduled collection, public publication/repairs and fresh price/ranking/digest checks remain gates.

**October 8 ownership deployed:** 29c354c5 verified on all four workers; committed source hashes/directory/disabled CLIs pass. Live eight-row 13F whitespace defect is corrected locally with 97 passing tests and exact source bytes; rollout/retry pending. No public source selection or FMP/billing switch. [Evidence](feed-ownership-rollout-2026-10-08.md).


**October 8 R7 scheduled observation:** 94a9e07f deployed on all four workers; actual hourly Congress run completes with twenty retained source revisions and zero public writes. Four House scans held pending OCR. SEC backlog is actively processing. A second queue protection prevents retired-job reenqueues; 57 focused tests pass, rollout pending collection completion. FMP remains on; public publication/repair, Starter access and fresh ranking/digest checks remain gates. [Evidence](feed-ownership-rollout-2026-10-08.md).


**October 8 R7 final verified receipt:** 2cea0aae/workflow 37847832482 deployed on all four workers; repeated migration/readiness/privacy and both queue code hashes pass. SEC October 7 backlog drained (325 Form 4/86 13F attempted), all 418 source hashes verified, unchanged 426-document/418-revision repeat and zero public writes. Final holds: 150 Form 4, five quarantined/four failed 13Fs, four House scans. Massive still 403 at 21:37 UTC; Starter/account login pending. No canonical repair/public source activation or billing change. Next: hourly SEC observation, reconciliation/publication, OCR and fresh ranking/no-send alert coverage. Goal remains active. [Evidence](feed-ownership-rollout-2026-10-08.md).

**October 8 R7 guarded repair:** [report](direct-congress-repair-rollout-2026-10-08.md), release b634cf3f. Production-capable repair/history notices pass 78 backend and eight isolated PostgreSQL cases; both sites/all four workers deployed and verified; index builds timed out on old snapshots and invalid remnants were removed; transaction-lifetime diagnosis/index creation/live repair pending. Scheduled SEC run 8 verifies 762 source revisions but exposes 3,674 pending older Form 4 documents across the full-week window. Canonical holds, publication, fresh digest/ranking checks, Starter and billing remain open.
