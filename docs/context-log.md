# Project context log

Read [AGENTS.md](../AGENTS.md) first. Keep current decisions and open loops there; append compact task records here. Record actual outcomes, not intended work. Newest entries first. Do not store secrets or private customer data.

## 2026-10-04 — Commit and deploy editorial/video improvements

- **Authorization:** owner explicitly requested commit and deployment of the implemented improvements.
- **Preflight:** GitHub main matches local `c7bb0b56`; all four Fly machines run `80aaa7f7`. Existing API/cron/video sizes match `fly.toml`, including the already-running performance video worker. API model override variables are unset. Preserve the separate competitor recommendation notes as documentation, not feature implementation.
- **Checks:** reuse the completed 108 focused tests, six model/generation checks and TypeScript pass; known broader baseline failures remain documented in the editorial report. Diff whitespace check passed.
- **Release:** commit/push to main for Vercel; explicitly dispatch the existing production backend workflow. Record completion, exact revisions and runtime checks after rollout. No change to subscriptions, voice selection, approval requirements or publishing authorization.

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
