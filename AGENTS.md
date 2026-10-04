# Walnut Markets project memory

Last reconciled: **2026-10-04**. Product/code baseline: `80aaa7f7` on `main` (also GitHub main when checked). This is the project's durable context, not a claim about today's deployed revision.

## Required routine for every prompt

The owner requested this routine on October 4, 2026. Apply it to every prompt in this repository, including follow-ups and resumed work.

1. Before substantive work, read this file from disk, even if an earlier copy is in the conversation. Read applicable nested instructions, check `git status --short --branch`, and read the relevant linked document. For product/prioritization work, read [the roadmap](docs/roadmap.md).
2. Use this memory to recover context, then verify facts that affect the task against current code or current service evidence. Older notes and release reports are dated evidence; they do not override newer code or the user's current instructions.
3. Do the requested work within its authorized scope. A roadmap entry is not permission to deploy, send messages, spend money, or change production data. Do not infer approval from historical task records.
4. Before the final response, update this file's current context/open loops when they changed and its latest-task checkpoint after every prompt. Add a short dated entry to [the context log](docs/context-log.md), including request, decisions, files/result, checks, deployment state, and unresolved next step. If nothing durable changed, say so briefly in the entry rather than inventing a decision.
5. Update the roadmap when capability status, priorities, or blockers change. Distinguish implemented, historically deployment-verified, partial, proposed, and currently unverified. Cite a source or commit and a date for consequential claims.
6. Keep this file concise: retain durable decisions and current open loops here; keep historical task entries in the log and detailed evidence in the linked reports. Never store secrets, credentials, private customer records, or full transcripts in memory. Re-read before editing so concurrent changes are preserved. If filesystem access prevents an update, disclose that limitation in the final response.

`AGENTS.md` is the standard discovery filename; do not create a competing lowercase copy. Automatic discovery occurs at session start, so the explicit re-read above also covers follow-up prompts. This file governs this repository; it is not global memory for unrelated projects. [Official instruction-discovery documentation](https://learn.chatgpt.com/docs/agent-configuration/agents-md).

## Product and current direction

- Public brand: **Walnut Markets**. Repository/service names still use `congress-tracker`.
- Product loop: analyze a stock → inspect sourced evidence → save/follow → return or monitor → upgrade for deeper research. Public research/discovery supports Free, Premium, and Pro; admin is an operational role.
- [docs/roadmap.md](docs/roadmap.md) is the single current product plan. The February free-first roadmap is [archived](docs/archive/roadmap-2026-02-11.md). It must not be used to remove current paywalls or rename plans to Advanced/Enterprise.
- Existing areas include Congress/insider/institutional data, government contracts, ticker research, screening/comparison, leaderboards, strategies/backtests/outcomes, alerts, public research briefs, Premium Research Memory, options/retirement calculators, billing, analytics, SEO and growth-video tooling.
- Priority proposal from the October 4 audit: verify reliability and conversion measurement first; complete the research-to-monitoring loop; improve evidence quality and realistic backtesting; expand datasets/developer access only after dependencies are resolved. This is planning, not an approved implementation schedule.

## Code and operations map

| Area | Start here |
|---|---|
| Frontend | `frontend/app/`, `frontend/components/`, `frontend/lib/api.ts`; Next.js 15 / React 19 / TypeScript |
| API, models, storage | `backend/app/main.py`, `backend/app/routers/`, `backend/app/models.py`, `backend/app/db.py`; FastAPI / SQLAlchemy |
| Business logic and jobs | `backend/app/services/`, `backend/app/jobs/`, `backend/fly.toml` |
| Access and billing | `backend/app/entitlements.py`, `backend/app/routers/accounts.py`, `frontend/lib/planBenefits.ts` |
| Confirmation and ranking | `confirmation_score.py`, `confirmation_context.py`, `top_stocks.py`, `ranking_access.py` under backend services |
| Research | `research_memory.py`, `research_evidence.py`, `research_claim_matching.py`, `research_briefs.py` under backend services |
| Strategies | `strategies.py`, `strategy_versions.py`, `strategy_scheduler.py`, `strategy_subscriptions.py`, `backtesting/` under backend services |
| Tests | `backend/tests/` (pytest); `frontend/tests/` (Node test runner) |
| Deployments | Vercel frontend; Fly API/cron/video workers; PostgreSQL production. Backend workflow: `.github/workflows/deploy-backend.yml` |

Public hosts: `walnutmarkets.com`, `app.walnutmarkets.com`. Backend: `congress-tracker-api.fly.dev`. Development uses local services and isolated data; see [LOCAL_DEV.md](LOCAL_DEV.md), whose environment example is incomplete. Do not copy production credentials into local examples.

## Decisions and invariants to preserve

- Preserve canonical event identity, source provenance, transaction versus disclosure/filing dates, and point-in-time availability. Historical transaction-date performance is not a tradable disclosure-date result.
- One public Confirmation Score, shared across surfaces. Current source uses agreement, quality and weighted coverage; fundamentals have a 30-point allocation and stale macro evidence is excluded. Treat it as descriptive evidence, not a validated return probability. Read the current service constants before modifying methodology; version changes must rebaseline alerts without rewriting recorded history.
- The September research holdout has already been consumed; do not call it untouched or reuse it as fresh validation. See [research status](docs/confirmation-research-status.md); its older product-version header is superseded by current code and September 26 reports.
- Web stock rankings are capped at Top 10. Guest identities are restricted to ranks 3–5, Free sees 1–5, and paid evidence is projected server-side. Do not equate the digest helper's Pro limit with the web cap. Current tie-break order is score, market cap, average volume, symbol; older acceleration/cluster ranking notes are historical.
- Research Memory and prepared Company Developments require Premium or higher server-side; published briefs remain public. An `active` thesis status is not proof of continuous monitoring or alert delivery.
- Historical unsigned options activity is non-directional (`can_confirm=false`), not live options flow. Options flow, customer API access and webhooks are still advertised as future features. Existing Stripe/Postmark callbacks are unrelated to a customer developer API.
- Keep public SEO pages cache-backed, truthful and entitlement-safe. Indexable/live-test success is not proof of indexing or ranking. Do not undo September public-render fixes based on older audit findings.
- Opt-in and master delivery preferences govern email. Distinguish verified live Stripe payments from grants/test payments; exclude internal/test traffic from growth conclusions. A provider request or 2xx is not proof of analytics ingestion.
- FMP remains a broad input dependency. The exit audit is a proposal, not a completed migration; do not cancel/switch it based on that document.

## Open loops (reverify before acting)

1. **Reliability:** recent cold ticker/search, ranking-universe, market-data and macro-freshness repairs need an observed production baseline. Older reports mention database health trouble; current health is unverified by this documentation task.
2. **Measurement:** latest reviewed analytics notes leave GA4 paid forwarding/consent acknowledgement and HeyCatch funnel mapping/receipt unresolved. Recheck current configuration before declaring them blocked or fixed; never manufacture a payment for testing.
3. **Research Memory:** creation, evidence and matching exist; continuous monitoring and user-facing thesis-alert delivery remain incomplete. Source excerpts, transcript availability and populated mobile QA need explicit coverage evidence.
4. **Backtests:** the current general engine explicitly excludes transaction costs/slippage. Do not call realistic execution bands complete because strategies store cost metadata.
5. **Provider economics:** evaluate direct public-record sources and licensed replacement prices with parity/rollback criteria before a provider migration.
6. **Validation:** historical release reports contain pre-existing suite failures. No full-suite pass was established on October 4; verify relevant tests on the current revision.
7. **Editorial/video quality:** October 4 [release `fdafb86f`](docs/research-editorial-quality-2026-10-04.md) deployed Sol research defaults, a shared writing contract, historical search-demand comparison and source-bound video direction. API/cron/video effective models and Sol API access were verified. Two voice auditions completed; live generated-draft quality and voice selection remain open. The earlier [creative audit](docs/social-video-layout-and-mix.md#october-4-creative-quality-audit-and-local-comparison) found a retained Zeely presenter-crop defect; repair it before publication and label historical footage.

## Working checks

- From `frontend`: `npm.cmd test`; focused checks use `node --test tests/<file>.test.mjs`; build with `npm.cmd run build`; type-check with `npx.cmd tsc --noEmit` when appropriate.
- From `backend`: use the configured Python environment, isolated test data and `python -m pytest tests/<file>.py -q`. Production Docker pins Python 3.12; older local reports used 3.14, so record the runtime actually used.
- Documentation-only changes: review the diff, verify local links and run `git diff --check`; do not claim application regression coverage from these checks.
- Preserve unrelated edits. Inspect generated Next.js files before reverting anything. Treat ignored `artifacts/` evidence as potentially local-only; durable conclusions need tracked summaries.

## Latest task checkpoint

- **2026-10-04 — Editorial release:** owner authorized committing and deploying the implemented research/video improvements. Release preflight confirmed GitHub main `c7bb0b56`, four running Fly machines on `80aaa7f7`, and matching existing compute sizes. API model overrides are unset, so the new Sol default can take effect.
- Checks: prior 108 focused tests, six model/generation checks and TypeScript passed; broader baseline failures are documented. `git diff --check` passed before release. Separate competitor recommendations remain documentation only and are preserved in the context log/roadmap.
- State: `fdafb86fd855552ebc897b6da64397d098c93c1c` committed/pushed; Vercel succeeded and both public domains reported it. Fly workflow `37240643427` succeeded; all four machines run its release image and backend readiness/privacy checks passed. API/cron/video report Sol and the v9 writing prompt; read-only Sol model access returned HTTP 200. A receipt-only documentation commit may advance frontend main without changing backend code.
- Next: live generated-brief/video quality review, audition voice selection and retained presenter-crop repair. No new voice selection, content generation or social publication occurred during this release.
