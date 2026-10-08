# Project context log

Read [AGENTS.md](../AGENTS.md) first. Keep current decisions and open loops there; append compact task records here. Record actual outcomes, not intended work. Newest entries first. Do not store secrets or private customer data.

## 2026-10-08 — Guarded Congress repair and actual SEC schedule

Continued the owner-authorized FMP-off goal. Added explicit PostgreSQL repair with source/directory/population/job hashes, paused generation, writer/table locks, atomic archive/retirement and strict staging reconciliation. Saved portfolio UI retains withdrawn-duplicate notices without recalculating returns. Three read-only live previews timed out over the large events table; concurrent expression indexes now support complete bounded lookup without narrowing identity guards. Eight session-private PostgreSQL cases pass, with no public reads/writes and verified cleanup. All 78 focused backend tests pass in primary and exact release; frontend 21 pass/one known baseline, TypeScript/build pass. Release b634cf3f pushed, Vercel success, backend workflow 37851343447 in progress. Indexes/production repair not yet applied. Actual SEC run 8 processes 344, retains 762 verified source revisions and exposes 3,674 pending older Form 4 documents in the full-week window. FMP active, no emails/billing changes. Next: indexes/live plan, publication/coverage, Starter/login. See [report](direct-congress-repair-rollout-2026-10-08.md).

Final receipt: b634cf3f deployed on both public sites/all four workers; workflow 37851343447 and health/access checks pass. Concurrent index create and recovery both hit old-snapshot lock waits. Both invalid nonunique remnants were verified and atomically removed with an immediate NOWAIT lock; no sessions terminated, no canonical records changed, no build remains running. Live repair/source activation remains held. Next is transaction-lifetime diagnosis and a successful indexed read-only review. Goal active; Starter and FMP login still pending.

## 2026-10-08: Ownership rollout verified and SEC whitespace correction

Release 29c354c5/workflow 37844550789 succeeded; migration/all four images/readiness/access and nineteen committed source-file hashes verified. Bundled directory hash matches; every publisher CLI returns disabled. Twelve staged revisions, no source selections/publications. Read-only public SEC capture proves the held filing uses trailing SH whitespace on all eight rows; strict trimmed validation and exact-file regression pass with 97 focused tests. Correction prepared, live retry pending. [Report](feed-ownership-rollout-2026-10-08.md). FMP/billing unchanged; scheduled collection and full cutover remain open.

## 2026-10-08: Feed ownership release prepared

Continued the active FMP-off goal with a scoped backend release of shared canonical ownership, disabled direct publisher CLIs, queue claim protection, SEC digest labels and backend archive context. Bundled 539-person public directory retains identical resolution on 33 captured reports. 437 focused checks plus one portfolio check pass; initial 60 missing-fixture/output-directory failures repaired, one prior baseline assertion excluded. [Report](feed-ownership-rollout-2026-10-08.md). Not yet deployed/source-selected; existing emails and FMP retained. Next: all-worker rollout verification, scheduled collectors and controlled publication readiness; Starter still pending.

## 2026-10-08: Corrective shadow rollout verified

Release c2b97e80/workflow 37842927148 succeeded, repeat schema migration/all four images/health/access verified. Live retry parses both House PDFs using pypdf 6.19.0; one canonical mismatch held. Twelve revisions with verified source hashes, repeat processes zero, no public source selections/publications. 109 focused release checks pass; full saved corpus remains 27 revisions/264 rows with identical repeat. Massive snapshot probe remains 403. Scheduled receipts, 401 SEC backlog items, exceptions and full public/price/alert cutover remain open. [Report](direct-feed-shadow-rollout-2026-10-08.md). FMP and billing unchanged, no test emails; active goal continues.

## 2026-10-08: Live shadow rollout and PDF correction

Release dbf0b1b9/workflow 37842069681 succeeded: additive migration, four images, readiness/database and Premium denial checked. Initial bounded live run stored ten revisions; Senate discovery works unattended, 401 pending reports, two House failures and one 13F failure held, zero public writes. House failure reproduced on pinned pypdf 6.13.3; correcting to locally validated 6.19.0 with an exact PDF regression fixture. Follow-up release/live retry pending. FMP and billing unchanged; full cutover still incomplete.

## 2026-10-08: Scoped shadow collection release prepared

Owner requests work through reliable FMP shutdown. Prepared a separate release checkout from b7989115 with eight isolated tables and bounded hourly official House/Senate/Form 4/13F collection. Public publishers remain disabled; existing FMP delivery unchanged. 100 focused tests, repeat schema check and Fly configuration validation pass; an unchanged legacy ingestion test has a collection syntax error. [Report](direct-feed-shadow-rollout-2026-10-08.md). No deployment or provider/billing switch at this checkpoint. Next: verify live scheduled receipts, then finish source publication and fresh price/ranking/alert coverage.

## 2026-10-04 — Approved strategy deployment and operational repair

- **Request:** owner approved the reviewed strategy release and live cap/price/delivery verification.
- **Release:** pushed `9fbd61c1`; Fly workflow `37246026041` and Vercel succeeded. Frontend versions, all four images and readiness verified. Current concurrent release `ab8642a7` retains these changes. Other task changes preserved.
- **Result:** all 20 strategies evaluated October 4, none failed; Insider Open-Market Buys 494 → 24 valid positions after cap/placeholder repair. All models ≤25, hard maximum 50. First capacity reductions were policy events rather than a flood of sale alerts. Blue charts and explicit historical/current dates are live.
- **Prices/delivery:** bounded canonical repair refreshed 61 symbols; 84 valid matured positions priced, 30 await Monday, zero matured gaps. Filled 71 remaining null current opening records. Eighteen opted-in deliveries accepted with no failures; receipt from both named strategies confirmed in Gmail. One invalid N/A position quarantined; its 4% allocation remains cash until the next evaluation. No preference changes or historical backlog replay.
- **Checks:** 48 focused Python 3.14.2 tests, two frontend checks, TypeScript and exact staged frontend production build passed. Production price coverage and delivery queries verified. First repair interrupted by concurrent deployment; resumed successfully after a transient database connection failure. No full-suite/comprehensive visual/source-coverage claim.
- **Files/next:** [release receipt](strategy-reliability-audit-2026-10-04.md), roadmap, sub-roadmap and memory updated. Observe next scheduled refresh/delivery; historical curves, closed-trade price gaps and prospective performance accounting remain unfinished. No brokerage execution.


## 2026-10-04 — Approved Signal Mixer and tool refresh deployment

- **Request:** owner approved the reviewed implementation for commit/deployment.
- **Result:** pushed `ab8642a7`; Vercel and Fly workflow `37246391059` succeeded. Both frontend versions, all four backend release images and readiness/database checks passed. Preserved separately deployed strategy release `9fbd61c1` and unrelated local edits.
- **Live checks:** old Mixer link redirects to its standalone page; default run returned 55 matches with 38/26/10 complete outcomes. Exact September 1–October 4 run returned seven matches, no mature priced outcomes and clear pending/coverage text. Neither hit a capacity error. Separate Backtesting controls loaded. Anonymous API returned 401 and page redirected to login.
- **Validation/state:** reused 49 backend/22 frontend checks, TypeScript/build and prior desktop/mobile layout review because application code was unchanged. Deployment receipt: [report](signal-mixer-and-tool-refresh-2026-10-04.md). No full-suite claim, source-data mutation, billing changes or email. Remaining: live Free/Premium account matrix, concurrency, longest-window/source coverage and price completeness.

## 2026-10-04 — Signal Mixer reliability and standalone research tools

- **Request:** fix capacity failures on default/short studies; make Mixer its own tool; align Backtesting, Compare and Screener with the retirement/options calculator design.
- **Result:** dedicated authenticated route and navigation, old-link redirect, shared visual hierarchy, complete streaming/batched studies, disclosure-window versus outcome cutoff separation and explanatory no-match/pending states. Analyst-upgrade → insider purchase is the initial recipe because it has stored historical coverage, not because it outperformed. Existing feature/access boundaries retained.
- **Evidence:** candidate ran in memory against production under read-only transactions. Original contract default now completes but has zero matching pairs; analyst default completes in 15.71 seconds with 55 matches and 38/26/10 mature outcomes. Short September window completes in 2.33 seconds, with seven matches and no mature outcomes. Full details and source timing in [the repair report](signal-mixer-and-tool-refresh-2026-10-04.md).
- **Checks:** 49 focused Python 3.14.2/SQLite tests and 22 frontend checks pass. Actual routes/components reviewed at 1440px and 390px using synthetic UI fixtures; those fixtures are not the production-data evidence. TypeScript and the production build passed after isolating generated preview types. No full-suite claim.
- **State/next:** local implementation only; no commit, deployment, production writes, emails, paid data or provider hydration. Temporary QA login route removed and fixture services stopped. Concurrent strategy edits preserved. Next: separately authorized deployment, default/short public HTTP and access verification, then concurrency/long-window observation.

## 2026-10-04 — Strategy freshness, alerts and practical portfolios

- **Request:** investigate stale Cleo Fields/Insider charts, absent subscribed alerts, oversized portfolios and missing records; cap positions and match ticker-chart blue.
- **Evidence/decisions:** read-only production audit confirms October 2 evaluation of 20 active strategies and additions in both named models, but zero strategy deliveries. Local fixes address queue starvation/timing, repeated rebalances, missing entry reconciliation, false historical fallback and hidden prospective transactions. Default 25/hard maximum 50; preserve historical returns and disclose dates. Shared chart uses cyan and UTC dates.
- **Files/result:** strategy services, portfolio policy/price helper/repair job, cron, UI/API types and regression tests. [Audit and rollout acceptance](strategy-reliability-audit-2026-10-04.md). Roadmap records verified delivery failure with local repairs awaiting deployment.
- **Checks:** 47 strategy pytest passes on Python 3.14.2/isolated SQLite; two frontend route passes, TypeScript, production build and diff checks pass. No full-suite or comprehensive visual QA claim. A bounded broad source query timed out; source completeness remains unverified.
- **State/next:** no commit, deployment, production data edits, emails or provider hydration. Concurrent edits preserved. Next: reviewed deployment, cap application, price coverage and opted-in receipt verification; no indiscriminate backlog email replay or manufactured live performance curve.

## 2026-10-04 — Commit and deploy ticker UX and Signal Mixer

- **Authorization/result:** owner approved commit and deployment after preview. Pushed `bfb7602d63fbc18a1a00aa5ad47e51f9d1f2e2a6`; [Vercel](https://vercel.com/moore11js-projects/congress-tracker/7d5G8MrU6fJHVxxr8hSMkPL9d7Sz) and [Fly workflow 37242718354](https://github.com/moore11j/congress-tracker/actions/runs/37242718354) succeeded. Both public version endpoints and all four backend machine images match. Readiness/database and anonymous mixer 401 checks passed.
- **Validation:** reused completed build, TypeScript, 66 backend and 99 frontend passes with five reproduced baseline failures; no application code changed since validation. Signed-in AAPL chart-first layout, sidebar, research cards, chart range and preserved marker choice verified. Local screenshot: `artifacts/ticker-ux-20261004/production-chart.png`.
- **Live study:** September 1–October 4, Congress trigger plus government contract within seven preceding days, zero fees and 10 bps slippage: 236 trigger rows, 155 without prior confirmation, 79 duplicate symbol/day rows, two matched setups. Both setups pending at 30/90/365 days; no fabricated shortened outcomes. Completed through the signed-in production UI. No mature live-return validation claimed.
- **Limits/next:** refreshed AAPL cash flow is null and public projection strips provider identity; source wording and data completeness need follow-up. Broad live mixer studies hit documented source/company limits; practical defaults and longer windows need work. No full-suite or comprehensive live-data pass. No account, subscription, delivery or voice changes.

## 2026-10-04 — Ticker preview image

- **Request/result:** show a preview image; inspected and presented the existing `artifacts/ticker-ux-20261004/research-cards.jpg` screenshot, explicitly labeled synthetic sample data.
- **Checks/state:** image opens and shows colored research cards; no code changes or new application tests, no deployment. No durable product decision changed. Next remains user review and any authorized release/coverage verification.

## 2026-10-04 — Ticker UX, contextual fundamentals and Signal Mixer

- **Request:** implement the competitive recommendations and make the ticker easier to scan; move the chart above research tabs, keep side cards and chart markers, move time ranges inside the chart, remove redundant outer source/side controls, and distinguish catalysts/risks/watch items without deleting research features.
- **Result:** chart-first layout and chart-local ranges with preserved overlay choices; icon/color/border hierarchy and expandable full research text; sourced growth/cash/leverage/valuation snapshot; Premium Signal Mixer combining explicit purchase disclosures with earlier purchases, contract observations or analyst upgrades, optional buyer clustering/SMA50, costs and 30/90/365-day benchmark-relative diagnostics.
- **Boundaries:** no scoring change, new chart library, price-plan change or live mixer monitoring. Contracts use observation timing when publication timing is unknown. The existing portfolio backtester still lacks cost/slippage modeling. Historical forward-P/E comparisons remain deferred. Details and code links: `docs/ticker-ux-and-signal-mixer-2026-10-04.md`.
- **Checks:** production build and TypeScript passed; build API bases explicitly targeted the local fixture service. 66 Python tests passed in isolated SQLite on 3.14.2. Focused frontend selection: 99 passed/five failures; all five reproduced on unchanged `98e65467`. Desktop and 390px mobile component/interaction QA used synthetic local data and confirmed no page-level horizontal overflow. Temporary preview route removed; test/build logs and screenshots remain local artifacts.
- **Delivery:** local implementation only; no commit, push or deployment. A read-only public cached AAPL context probe returned a cache miss; no production data/settings changed. Prior editorial release and its receipt were preserved.
- **Next:** authorized release and production-shaped source-coverage/authenticated layout checks. Saved rules/live alerts and R6 execution realism remain open.

## 2026-10-04 — Commit and deploy editorial/video improvements

- **Authorization:** owner explicitly requested commit and deployment of the implemented improvements.
- **Preflight:** GitHub main matches local `c7bb0b56`; all four Fly machines run `80aaa7f7`. Existing API/cron/video sizes match `fly.toml`, including the already-running performance video worker. API model override variables are unset. Preserve the separate competitor recommendation notes as documentation, not feature implementation.
- **Checks:** reuse the completed 108 focused tests, six model/generation checks and TypeScript pass; known broader baseline failures remain documented in the editorial report. Diff whitespace check passed.
- **Verified release:** committed/pushed `fdafb86fd855552ebc897b6da64397d098c93c1c`. [Vercel production](https://vercel.com/moore11js-projects/congress-tracker/H8Pe7CWECoXLzb2wDnhaMmMvPjuA) succeeded; both public `/api/app-version` endpoints returned that revision. [Fly workflow 37240643427](https://github.com/moore11j/congress-tracker/actions/runs/37240643427) succeeded, including readiness and anonymous Premium-route denial checks. All four API/cron/video machines run `release-fdafb86fd855552ebc897b6da64397d098c93c1c` and are started.
- **Runtime checks:** API/cron/video imports returned `gpt-6.1-sol` and `research_brief_v9_native_story`; video direction model also resolves to Sol. Authenticated read-only model lookup returned HTTP 200 for Sol without generating content. Backend `/ready` returned database/status `ok`. Compute sizes, subscriptions, voice selection and publishing authorization are unchanged.
- **Receipt/next:** this documentation-only release receipt may advance frontend main; backend source is unchanged. Live draft/video quality evaluation and voice/crop follow-up remain open. No new content or messages were generated/sent by deployment.

## 2026-10-04 — TradingView and Koyfin proposal assessment

- **Request:** assess the owner's pasted Google proposal for embedded charts, contextual fundamentals and a no-code signal mixer.
- **Evidence/result:** inspected current source at `c7bb0b56`, roadmap and chart audit. Lightweight Charts already powers Congress/insider/contract markers and technical indicators; financial panels include revenue trends, FCF and source-qualified forward P/E. Backtesting supports multiple source types and SPY comparison, but is not an arbitrary temporal signal mixer and explicitly excludes costs/slippage. No live capability audit was performed.
- **Recommendation:** retain R1/R2 foundations; improve discoverability/context of existing fundamentals as the smallest feature extension, then scope a bounded event-sequence mixer tied to R6 validation and eventual monitoring. Do not infer defensible alpha or competitor inability from feature positioning. No new implementation, pricing or provider decision was approved.
- **External evidence:** official TradingView product comparison distinguishes widgets (no custom data) from libraries; its FAQ excludes Pine Script, and current introduction limits free Advanced Charts use to public, attributed, non-paywalled implementations. Koyfin's fundamentals documentation supports its financial-analysis positioning. Links and source paths are recorded in the roadmap assessment.
- **Files/checks:** updated `AGENTS.md`, this log and `docs/roadmap.md`; reviewed documentation diff, checked newly linked local targets and ran `git diff --check`. Application tests were not run for this assessment.
- **Delivery/next:** local documentation only; no commit, deployment, purchase or external message. Preserve all existing uncommitted editorial/video changes. Product scope selection and current production verification remain open.

## 2026-10-04 — Approved research writing and video direction improvements

- **Request:** implement approved creative improvements and upgrade the research writer; learn from successful finance writing, measured search interest and native Walnut data.
- **Decisions/results:** Sol becomes the local default, with explicit mini choice and existing overrides preserved. Added a shared original-writing contract to first drafts and revisions, real ticker-tab navigation, compact historical demand comparisons and a bounded Sol video selector. New research videos show the finding before ticker → Research navigation; exact published evidence and old approval hashes remain protected. Existing name/buyer/edit-history guards and topic diversity remain.
- **Evidence:** live admin Keyword Planner showed substantially more demand for natural valuation/ownership phrases than repeated long 13F questions. Historical averages are not current trends or organic difficulty. Finance-writing sources, measured phrases, changed behavior and limitations are recorded in `docs/research-editorial-quality-2026-10-04.md`.
- **Files:** research/editorial/keyword services, admin model fallback, daily video and new direction service, Sol cost estimate, focused tests and roadmap/memory. Prior local video compositor/audit changes preserved.
- **Checks:** 108 focused tests on Python 3.14.2 and TypeScript passed. Broader research: 137 passed, seven failed; all seven reproduced on isolated unchanged `c7bb0b56`. Frontend six passed/one unchanged archive-helper expectation failed. No full-suite pass, live Sol brief evaluation or new full video render claimed.
- **External actions:** bounded Keyword Planner lookup; two completed Runway voice auditions, 10 existing free credits, no new purchase or subscription. Production model/voice settings unchanged; no email, publication, commit or deployment.
- **Next:** authorized deployment with effective-model/API verification, review diverse live drafts and one rendered video, choose voice after listening, and repair the retained Zeely crop defect. Signed media URLs and credentials are not stored in this log.

## 2026-10-04 — Social video creative audit and editing comparison

- **Request:** study viral TikTok/Instagram examples and improve Walnut/Zeely storytelling, visuals, narration and writer-model quality without excessive token cost.
- **Evidence/result:** reviewed official TikTok guidance, Motion finance ad samples, documented organic finance examples, provider documentation and current video code. Paid examples do not establish organic virality. Current daily scripts are extractive; the generic creative contract restricts hooks. Live model overrides were not queried. Recommended a bounded GPT-6.1 Sol creative pass, separate licensed voice auditions and evidence-led editing; these production integrations remain proposed.
- **Files:** detailed findings and sources in `docs/social-video-layout-and-mix.md`; local compositor `scripts/render_social_evidence_review.py`; roadmap and memory follow-up. Historical Google/Grace comparison MP4 is under ignored `artifacts/video-creative-audit-2026-10-04/`, so it is local-only. It changes the upper evidence panel and preserves existing narration. A presenter-crop defect remains in retained source and must be repaired before publication.
- **Checks:** final eight-scene render completed, full FFmpeg decode passed, source/output audio hashes matched, and sampled frames inspected. No full listening review, new voice audition or application regression suite. No measured performance uplift.
- **Delivery:** local changes only; no commit/deployment, live configuration change, paid generation, subscription, upload or publication. Next: review the editing direction, repair source face tracking and audition voice before activating the new creative contract.

## 2026-10-04 — Commit and deploy the documentation

- **Request/authorization:** owner said “commit and deploy.” Scope is the seven roadmap/context documents from the previous task.
- **Preflight:** memory re-read; working tree contains only intended documentation changes; GitHub main remains `80aaa7f7`. Existing GitHub deployment history confirms Vercel Production deployments follow main commits.
- **Release plan:** commit and push to `main`, verify the resulting Vercel production deployment and both public app-version endpoints. Backend source is unchanged, so no Fly restart is needed; check backend readiness without deploying it.
- **Checks:** documentation link/archive checks and `git diff --check`; no application regression suite for Markdown-only edits.
- **Verified release:** committed and pushed all seven documents as `104aa38224c3953c619b8f4e0817d866f5041aea`. [Vercel Production deployment](https://vercel.com/moore11js-projects/congress-tracker/tN3mm5tgNUzHM6qtfuReHH1ngQfG) reported success; both `walnutmarkets.com/api/app-version` and `app.walnutmarkets.com/api/app-version` returned that exact revision. Backend `/ready` returned `status: ok`, `database: ok`.
- **Receipt:** this log and `AGENTS.md` record the verified release in a follow-up documentation commit, which may advance the frontend revision. No product capabilities, pricing, account settings or delivery preferences changed. Backend was not redeployed because its files are unchanged.

## 2026-10-04 — Roadmap reconciliation and project memory

- **Request:** find the latest roadmap, analyze the codebase, update the roadmap and establish a read-before/update-after memory routine for every prompt.
- **Baseline:** local `main` and GitHub main both `80aaa7f7104d3fcccf5b507b9fdf93f5a8c9c4a5`; working tree initially clean. Git history dates the main roadmap to February 11 (`7b5b097a`), strategy-monitoring roadmap to August 2 (`1fb8d8f0`), and latest product-brief change to June 20 (`3b0f5adb`). The brief's roadmap section still describes early phases.
- **Evidence reviewed:** current routes, services, models, entitlements, pricing copy, job/deployment configuration, relevant test sources, September release/audit reports, and October 2–4 commits. No new live product/analytics audit was performed. GitHub main was read directly, not inferred from a stale remote-tracking branch.
- **Decisions:** use `docs/roadmap.md` as the current plan; retain the original February document as an archive. Distinguish implemented capabilities from complete product promises and deployed evidence. Keep Free/Premium/Pro terminology. Proposed priorities: reliability and measurement, then retention/research monitoring, then broader expansion. Priorities are not an approved delivery schedule.
- **Changes:** root `AGENTS.md` stores concise durable memory and mandates a fresh read before each prompt and an update before the final response. This log stores history. Product brief and strategy documents point to the reconciled status. Scope is this repository; global Codex settings are unchanged.
- **Important gaps:** continuous thesis monitoring, execution-cost backtesting, live directional options flow, customer APIs/webhooks, current provider/funnel receipt verification, and FMP replacement parity. No verified improvement in predictive investment performance is claimed.
- **Validation:** all 80 local links across the six current documents resolve; the archived roadmap matches the original tracked content; `AGENTS.md` is below 16 KB; `git diff --check` passed. Application tests were not run for documentation-only changes. The seven changed/added files include the preserved archive.
- **Delivery state:** local documentation changes only; no commit, push, deployment, subscription changes, messages or payments.
- **Next:** owner can select R1/R2 from the roadmap; reverify runtime state before acting on dated operational findings.

## Entry template

`YYYY-MM-DD — Task`: request; durable decision/result; changed files or evidence; checks and limitations; commit/deployment state; open next step. For a prompt with no durable changes, record that briefly and refresh the latest-task checkpoint in `AGENTS.md`.


## 2026-10-08: Scheduled Congress receipt and queue retirement continuation

Continued FMP-off execution. Release 94a9e07f deployed/verified all four machines; scheduled Congress run 4 finishes at 21:17 UTC with twenty total revisions, zero public writes. Four House failures are scanned PDFs requiring OCR. SEC backlog run 5 is active; no duplicate execution. Both enqueue paths preserve repair tombstones under locks and retain unflushed changes, 57 queue/P&L/repair tests pass in release checkout. Code prepared, no further deployment while collection runs. FMP/account billing active, Starter and account login pending. Next: complete backlog/repeat observation, deploy queue protection, production-capable repair/publication and fresh retained-product alert checks. See [receipt](feed-ownership-rollout-2026-10-08.md).


## 2026-10-08: Final ownership and backlog verification

2cea0aae/workflow 37847832482 deployed on all four workers; repeated migration/readiness/privacy and both queue code hashes pass. SEC October 7 backlog drained (325 Form 4/86 13F attempted), all 418 source hashes verified, unchanged 426-document/418-revision repeat and zero public writes. Final holds: 150 Form 4, five quarantined/four failed 13Fs, four House scans. Massive still 403 at 21:37 UTC; Starter/account login pending. No canonical repair/public source activation or billing change. Next: hourly SEC observation, reconciliation/publication, OCR and fresh ranking/no-send alert coverage. Goal remains active. [Detailed receipt](feed-ownership-rollout-2026-10-08.md). Fifty-seven final queue/P&L/repair tests pass; earlier ownership set has 437 checks plus one portfolio check and 97 SEC fixture checks. No full-suite claim. No customer emails or canonical production repair.


## 2026-10-08: Production Senate repair and source activation

Request: keep going until FMP is off. Applied source-bound canonical repair after guarded recovery of two idle read snapshots and successful indexes. Deployed `8db83b2d`, exact four workers/readiness pass; 77 initial and 29 final overlapping backend checks pass. Senate official source selected generation 2, five stock rows adopted without new IDs; ten duplicates/thirteen extra transactions archived, five queued jobs retired. Fifty affected positions/five runs unchanged, repeat zero changes. Bond-only mismatch held; all other sources still FMP. Real legacy Senate ingest makes zero HTTP requests under a transport barrier, zero recorded successful usage since switch at 22:43 UTC. Public API/portfolio verification passes. Browser found stale day-long member cache; `b57c1af5` reduces it to five minutes and versions requests, seven checks/TypeScript pass; both sites and rendered table verified. Actual-event no-send replay yields five monitoring/watchlist/daily items, zero-repeat alerts, daily/weekly Top Stocks unchanged. Three current score bundles/four market tiles refreshed with persistent backup, prices/history preserved. No emails, purchase or billing cancellation. Files: repair rollout report, member API/cache test, memory/roadmap/checklist. Next: scheduled publisher receipt, current score/no-send checks, remaining sources/coverage, Starter access and eventual full shutdown. Goal active after substantive progress.
