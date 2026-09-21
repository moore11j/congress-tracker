# Research Memory live pilot — September 20, 2026

## Outcome

The bounded MU/NVDA/AAPL pilot exercised production news/press-release ingestion, structured evidence extraction, durable reuse, and public ticker presentation. A separate isolated fixture exercised real OpenAI claim matching without creating production user research. Reliability and coverage-copy fixes were committed and deployed as `be1fc129dc99be5312fd0511c1a8d618af69cb7a`.

This is pilot verification, not a claim of comprehensive source coverage, investment accuracy, or competitor superiority. Earnings-call coverage remains disabled in production. No thesis-health scoring or alerts were added.

## Live results

| Symbol | Initial documents attempted | Events stored | Initial extraction calls | Repeat extraction calls | Repeat outcome |
| --- | ---: | ---: | ---: | ---: | --- |
| MU | 4 | 5 | 4 | 0 | All four processed documents reused; 0.54 seconds |
| NVDA | 4 | 4 | 4 | 0 | All four processed documents reused; 0.47 seconds |
| AAPL | 4 | 1 | 4 | 1 | Three reused; one failed quotation validation again; 8.11 seconds |

The first pilot produced 10 evidence events from 12 documents attempted. Eleven documents completed; one Apple document failed validation. The repeat produced no duplicate events. The rejected Apple result was not persisted as evidence: `excerpt_not_in_source` means the proposed quotation could not be found in the supplied source text. Validation was not relaxed to increase apparent coverage.

The repeat's one extraction request was audited successfully: `gpt-5.4-mini`, HTTP 200, 1,617 ms, 828 input tokens, 178 output tokens, 1,006 total tokens. Provider request success is distinct from downstream evidence-validation success. Initial extraction usage records were missing because of the pool issue below; an exact cost total cannot be reconstructed from those missing records.

Production contained zero Research Memories/claims at preflight, so there were no genuine private theses to match. The separate in-memory SQLite test copied one real public Micron event and used explicitly synthetic supportive/contradictory claims. Both relationships were classified correctly, another user's query returned no matches, and repeating the comparison made zero further model calls. Two initial matching calls used 1,552 tokens in total. No production users, theses, or claims were created by that test.

## Fixes shipped

1. **Worker connection starvation:** the session-level PostgreSQL advisory lock occupied one of the cron pool's two connections; document processing occupied the other. Independent provider-cache and AI-audit writes then timed out. The lock now keeps its dedicated physical connection outside that pool with `detach()`, releases the advisory lock in `finally`, and physically closes the connection on exit. A two-slot pool regression test covers normal and exceptional exits. This uses one additional physical connection while the single locked worker runs; it does not increase the general pool size.
2. **Bounded source work:** a per-security/source extraction allowance defaults to five calls, still sharing the existing global run allowance. `RESEARCH_OPERATIONAL_MAX_EXTRACTIONS_PER_SOURCE` is clamped to 1–50 and falls back to five for invalid configuration. This limits a single source's ability to consume the run budget; it is not a guarantee of complete coverage for every security on each run.
3. **Safe diagnostics:** extraction failures expose fixed reason codes rather than raw model output or private text.
4. **Honest coverage UI:** partial analysis is labeled “Partially analyzed,” empty provider results are “No documents returned,” source checks include UTC times, releases are labeled “Press releases,” and the UI explains provider-excerpt and partner-release limitations.
5. **Repeatable operator pilot:** a manually dispatched workflow supports read-only aggregate diagnosis, a bounded three-symbol extraction pilot, and isolated matching verification. The pilot obeys the existing worker lock and production transcript licensing flag.

## Material files

- Workflow: `.github/workflows/research-evidence-pilot.yml`
- Pilot: `backend/scripts/research_evidence_pilot.py`
- Worker: `backend/app/jobs/refresh_research_operational_intelligence.py`
- Services: `backend/app/services/operational_intelligence.py`, `backend/app/services/research_evidence.py`
- Backend tests: `backend/tests/test_research_worker_lock.py`, `backend/tests/test_operational_intelligence.py`
- Frontend: `frontend/components/research-memory/ResearchCoverage.tsx`, `frontend/components/research-memory/ResearchEvidencePanel.tsx`, `frontend/components/ticker/TickerOperationalIntelligenceCard.tsx`, `frontend/lib/researchEvidence.ts`
- Report: `docs/research-memory-live-pilot-2026-09-20.md`

No database migrations, public API routes, billing changes, or new notification jobs were required for these fixes. Existing security identity, source-document/event storage, authorization, model abstraction, and private match queries were reused.

## Versioning

Existing extraction configuration in `backend/app/services/research_evidence.py` remains:

- Model: `RESEARCH_EVIDENCE_MODEL`, default and observed `gpt-5.4-mini`.
- Prompt: `evidence_extraction_v3_sections`.
- Schema: `research_evidence_schema_v2`.
- Processing: `research_evidence_processing_v2`.

Existing matching configuration in `backend/app/services/research_claim_matching.py` remains:

- Model: `RESEARCH_CLAIM_MATCHING_MODEL`, default and observed `gpt-5.4-mini`.
- Prompt: `claim_matching_v2_grounded`.
- Schema: `claim_matching_schema_v1`.
- Engine: `claim_matching_engine_v1`.

## Automated checks

From `backend`:

```powershell
$env:PYTHONPATH='.'
.\.pilot-venv\Scripts\python.exe -m pytest tests/test_research_worker_lock.py tests/test_operational_intelligence.py tests/test_research_evidence.py tests/test_research_claim_matching.py tests/test_research_memory.py -q --basetemp=.local/pilot-tests
```

Result: **48 passed**, two dependency deprecation warnings. The temporary local environment used Python 3.14; production uses its pinned deployment runtime. This is not a claim that the entire backend suite ran.

From `frontend`:

```powershell
node --test tests/research-memory-phase1.test.mjs
.\node_modules\.bin\tsc.cmd --noEmit
npm.cmd run build
```

Results: **6 passed**, TypeScript passed, production build passed (64 pages). `git diff --check` passed. There is no frontend lint script in `package.json`; no standalone lint result is claimed.

## Deployment and live runs

- [Initial pilot](https://github.com/moore11j/congress-tracker/actions/runs/35551757717): succeeded.
- [Isolated live matching](https://github.com/moore11j/congress-tracker/actions/runs/35552045184): succeeded.
- [Backend/cron deployment](https://github.com/moore11j/congress-tracker/actions/runs/35552483678): succeeded, including readiness, public intelligence, and anonymous-access rejection smoke checks.
- [Post-fix pilot](https://github.com/moore11j/congress-tracker/actions/runs/35552819225): succeeded; cache reuse and usage-audit persistence verified.
- Vercel's production version endpoint reported the implementation SHA, and Chrome displayed the updated source-coverage copy.

## Chrome QA and source review

The signed-in desktop Research Memory index rendered its truthful empty state. The MU ticker rendered its compact creation entry, company developments, source coverage, linked catalysts, and empty risk/opportunity/watch sections. Desktop screenshots were captured in the task conversation. Temporary database unavailability displayed a fallback rather than a blank screen; after recovery, the normal content loaded.

The Micron original release opened successfully and supported the extracted product claims. Importantly, its full text includes an expected second-half-2027 volume-production milestone absent from the provider excerpt. The full release also supplies context for the power-reduction comparison. The current pipeline cannot extract details it never receives. News excerpts in this sample were only 132–243 characters; release excerpts were 291–804 characters.

**Mobile visual QA is not verified in this run.** Chrome's requested 390×844 override did not change the effective 1920-pixel viewport. The override was reset. No mobile screenshot or pass is claimed. No production thesis was created just to obtain a populated private-page screenshot.

## Separate infrastructure observation

During deployment verification the API temporarily returned 503. The existing database watchdog was dispatched; PostgreSQL restarted, but Fly's health-check wait timed out with two of three checks passing. [Watchdog run](https://github.com/moore11j/congress-tracker/actions/runs/35552560182) therefore failed. Independent follow-up checks returned HTTP 200 for `/ready` (`database: ok`) and `/api/events?limit=1`, and the post-fix pilot completed database reads/writes. This establishes application recovery, not resolution of the remaining Fly health-check issue. No repeated manual restarts or database configuration changes were made.

## Remaining limitations and recommended next work

1. Diagnose the remaining PostgreSQL machine health check before increasing ingestion concurrency or source volume.
2. Add permitted full-text first-party release ingestion, preserving provenance, source revisions, quotations, and comparison context. Do not bypass paywalls or assume provider licensing permits arbitrary redistribution.
3. Verify transcript entitlement/availability before enabling earnings calls; test full-call and Q&A coverage, including guidance qualifications.
4. Review a labeled sample spanning actual risks, product milestones, operational problems, M&A, and guidance. This small pilot proves mechanics, not extraction precision/recall across those categories.
5. Complete mobile visual QA using an effective viewport, and later verify the populated private-thesis UI against explicitly authorized user research or non-production fixtures.
6. Inspect fail-closed validation outcomes and bounded retry behavior without inventing missing text, thresholds, or user beliefs.

These next steps are recommendations, not additional features implemented by this pilot.
