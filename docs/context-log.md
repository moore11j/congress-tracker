# Project context log

Read [AGENTS.md](../AGENTS.md) first. Keep current decisions and open loops there; append compact task records here. Record actual outcomes, not intended work. Newest entries first. Do not store secrets or private customer data.

## 2026-10-04 — Commit and deploy the documentation

- **Request/authorization:** owner said “commit and deploy.” Scope is the seven roadmap/context documents from the previous task.
- **Preflight:** memory re-read; working tree contains only intended documentation changes; GitHub main remains `80aaa7f7`. Existing GitHub deployment history confirms Vercel Production deployments follow main commits.
- **Release plan:** commit and push to `main`, verify the resulting Vercel production deployment and both public app-version endpoints. Backend source is unchanged, so no Fly restart is needed; check backend readiness without deploying it.
- **Checks:** documentation link/archive checks and `git diff --check`; no application regression suite for Markdown-only edits.
- **State:** release and verification pending. No product capabilities, pricing, account settings or delivery preferences changed.

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
