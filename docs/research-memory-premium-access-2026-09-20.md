# Research Memory Premium access

> Release update (September 25): this historical implementation report is included in the approved rollout. See `product-growth-release-2026-09-25.md` for current validation and deployment details.


## Implemented

- Research Memory and prepared Company developments now require Premium, Pro, or the existing admin override. Public Walnut research briefs remain accessible to guests and Free users.
- Reused the existing green `ContextualUpgrade` presentation for Cross-Source Divergence and Your Research. Catalysts, Risks, What Changed, and What to Watch Next retain their blurred preview and now have accessible, unblurred upgrade actions. Similar Historical Setups is unchanged.
- Research routes and ticker cards share an entitlement boundary with loading, retry, and locked states. Company evidence is not fetched before permission is established. No new model calls were introduced.
- Server-side feature gates enforce the Premium minimum even when existing database feature-gate rows still say Free. Existing ownership checks remain in force. Prepared Company developments requires authentication, checks entitlements before reading evidence, and returns private/no-store responses.
- Matching excludes Free/downgraded owners before semantic matching or invalidator evaluation, preserving their saved theses and existing evidence. User lookups are batched. Entitlement resolution is read-only so it cannot commit away a worker's event lock.
- The five Research Memory capabilities use the existing configurable entitlement architecture with a Premium floor. Existing alert limits remain unchanged; this does not enable new alerts.
- Updated backend deployment smoke checks to expect unauthorized anonymous access to Company developments instead of treating it as public.

No database migration, data deletion, pricing checkout change, ingestion change, or new monitoring infrastructure is included. Both frontend and backend must be released for consistent enforcement. These changes have not been committed or deployed.

## Verification

Backend, from `backend`, with `DATABASE_URL=sqlite:///:memory:`:

```powershell
.\.research-ops-venv\Scripts\python.exe -m pytest tests/test_research_premium_access.py tests/test_research_memory.py tests/test_operational_intelligence.py tests/test_research_claim_matching.py tests/test_research_evidence.py tests/test_research_worker_lock.py -q --basetemp=.local/premium-access-final
```

Result: **64 passed**. Covers guest/Free denial, Premium/Pro/admin access, stale database gate floors, owned-detail and mutation denial, no unauthorized evidence lookup, private response headers, and no matching for downgraded owners, alongside the existing Research Memory regression suites.

An additional run including `tests/test_entitlements.py` returned **51 passed, 1 failed**. The failure, `test_premium_user_can_create_past_free_watchlist_limit`, is a missing `watchlist_alert_rules` table in that test fixture. It was independently reproduced using the pre-change HEAD version of `app.entitlements`; no test was skipped or weakened to hide it.

Environment limitation: the existing Python 3.12 environments reference an unavailable interpreter. Backend tests ran in the existing disposable `.research-ops-venv` using Python 3.14 and installed test dependencies, rather than the exact production dependency pins. Requirements files were not modified.

Frontend, from `frontend`:

```powershell
node --test tests/ticker-compact-layout.test.mjs tests/ticker-operational-overview.test.mjs tests/research-memory-phase1.test.mjs tests/ticker-decision-layer.test.mjs tests/confirmation-recalibration.test.mjs
.\node_modules\.bin\tsc.cmd --noEmit
npm.cmd test
npm.cmd run build
```

- Focused regression tests: **37 passed, 0 failed**.
- TypeScript: passed.
- Production build, including lint/type validation: passed. Existing stale Browserslist-data warning remains.
- Full frontend suite: **704 tests, 658 passed, 46 failed**, matching the previously reported baseline failure count. The full suite is not green; unrelated failures were not hidden or skipped.
- `git diff --check`: passed (Git line-ending notices only).

## Visual/accessibility QA

Chrome local development preview used the actual ticker components and exact Overview rendering, sample blurred values, and a real guest entitlement check. No production account permissions were changed and no private evidence was injected. Checked desktop and approximately 390px mobile layouts:

- Public brief links remain visible above the Company developments Premium CTA.
- Ranking and divergence show matching green upgrade components.
- Blurred decision sections retain readable, actionable upgrade buttons outside inert preview content.
- Your Research has the Premium CTA; its loading/error and paid/feature-disabled states are also covered by render tests.
- No document horizontal overflow at mobile width; tabs use their existing horizontal navigation.

Screenshot capture intermittently timed out or returned duplicated viewport tiles through the browser extension; successful captures and accessibility/DOM checks were used for verification. This was local component QA, not a production paid-account end-to-end checkout test. The temporary QA route and generated build cache were removed, and the browser viewport was reset.

## Material files

Backend: `app/entitlements.py`, `app/routers/operational_intelligence.py`, `app/services/research_claim_matching.py`.

Frontend: `lib/entitlements.ts`; `components/research-memory/ResearchMemoryAccess.tsx`; `components/ticker/TickerContextCard.tsx`, `TickerDecisionPanels.tsx`, `TickerOperationalIntelligenceProvider.tsx`, `TickerOperationalIntelligenceCard.tsx`, `TickerResearchMemoryCard.tsx`; `app/ticker/[symbol]/page.tsx`; both Research Memory index/detail routes.

Tests: backend `test_research_premium_access.py`, `test_research_memory.py`, `test_research_claim_matching.py`; frontend `research-memory-phase1.test.mjs`, `ticker-compact-layout.test.mjs`.

Deployment configuration: `.github/workflows/deploy-backend.yml`.
