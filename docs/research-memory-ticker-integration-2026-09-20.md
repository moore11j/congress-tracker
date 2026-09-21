# Ticker Research integration — September 20, 2026

## Delivered behavior

- Detailed Company Developments now lives in the ticker **Research** tab, below related Walnut research briefs.
- Overview reuses the same prepared findings in **What Changed (30D), Catalysts, Risks, and What to Watch Next**. Each operating finding uses one concise AI-written headline, publication date, and links to the source/research detail when available.
- Opportunities are folded into Overview's Catalysts, without adding another Overview module. Within each bucket, duplicate evidence IDs/titles are removed. A recent development can appropriately appear in both What Changed and Catalysts.
- Existing confirmation inputs remain available. Operating findings do not change the confirmation score or divergence calculation.
- The Research detail grid uses two columns on desktop and stacks on mobile, retaining confidence, source excerpts, coverage, and the Build a thesis entry point.
- One ticker-scoped context shares the existing read-only operational-intelligence request across both tabs. No new AI call on render, tab switch, or research-detail navigation.

## Earnings calls and extraction reliability

Production had the earnings transcript analysis flag disabled. The configured FMP account was checked using a bounded, metadata-only access probe before setting `RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED=true` in `backend/fly.toml`.

Full transcript processing uses the existing chunked, resumable evidence pipeline, including Q&A. Existing source coverage, shared evidence storage, and private Claim Matching are reused. No new ingestion framework or database migration was introduced.

Live validation exposed two real failures that were fixed:

1. Model-written quotations were sometimes paraphrased or stitched together. The existing exact-source check correctly rejected these documents. Extraction now selects from bounded overlapping source passages instead.
2. Putting source quotations directly in the strict schema's enum caused an OpenAI HTTP 400 for Apple. The outbound schema now contains only safe `passage_N` IDs; source passages are supplied as untrusted input data. The server resolves the selected ID to the verbatim text, checks every required field, rejects unknown IDs/types, and still validates that the excerpt occurs in the source.

Only validated chunks are retained. Complete document results publish atomically; incomplete/invalid replacements do not supersede prior valid evidence. Character offsets remain pinned to the normalized source revision. The controlled processing-version change makes older extractions eligible for bounded regeneration.

The OpenAI documentation skill informed the strict schema design; provider schema compliance is not treated as proof of factual accuracy. Semantic interpretation remains an AI judgment requiring source review. See [OpenAI Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs).

Final extraction versions:

- Model: `gpt-5.4-mini`, configurable through `RESEARCH_EVIDENCE_MODEL`.
- Prompt: `evidence_extraction_v5_passage_ids`.
- Schema: `research_evidence_schema_v4_passage_ids`.
- Processing: `research_evidence_processing_v4`.
- Definitions: `backend/app/services/research_evidence.py`.

Coverage now distinguishes enabled-but-not-yet-checked from disabled. Retried or reused transcripts count as returned documents, avoiding a misleading empty status after successful extraction.

## Overview rules and cost controls

- Up to three operating findings per bucket, with a maximum of five entries including existing inputs.
- Material findings rank before lower-materiality findings, then by publication date.
- What Changed includes only actual publication dates in the preceding 30 days. Undated/future items are excluded.
- What to Watch Next uses explicit extracted future milestones, not invented dates or milestones.
- Date-only watch labels retain their associated development headline (or stored explanatory summary), so a date such as "calendar 2028" is not presented without company context.
- No client AI, private thesis text, or per-user source extraction cache.
- Existing worker advisory lock, budgets, source reuse, chunk resume, source identity, and ownership boundaries remain intact.
- Existing stored events continue into Research Memory's matching pipeline; this change does not create or activate user theses.

## Material files

### Frontend

- `frontend/app/ticker/[symbol]/page.tsx`
- `frontend/components/ticker/TickerContextCard.tsx`
- `frontend/components/ticker/TickerDecisionPanels.tsx` — new
- `frontend/components/ticker/TickerOperationalIntelligenceCard.tsx`
- `frontend/components/ticker/TickerOperationalIntelligenceProvider.tsx` — new
- `frontend/lib/tickerOperationalOverview.ts` — new

### Backend/configuration/operator tooling

- `backend/app/services/operational_intelligence.py`
- `backend/app/services/research_evidence.py`
- `backend/fly.toml`
- `backend/scripts/research_evidence_pilot.py`
- `.github/workflows/research-evidence-pilot.yml`

### Tests

- `backend/tests/test_operational_intelligence.py`
- `backend/tests/test_research_evidence.py`
- `frontend/tests/research-memory-phase1.test.mjs`
- `frontend/tests/ticker-decision-layer.test.mjs`
- `frontend/tests/ticker-operational-overview.test.mjs` — new

No migrations, schema columns, public endpoints, pricing rules, or confirmation-score changes.

## Verification

From `frontend`:

```powershell
node --test tests/research-memory-phase1.test.mjs tests/ticker-decision-layer.test.mjs tests/ticker-operational-overview.test.mjs
.\node_modules\.bin\tsc.cmd --noEmit
npm.cmd run build
npm.cmd test
```

- Targeted frontend tests: **19 passed**, including the final date-only watch-label regression.
- TypeScript: **passed**.
- Production build: **passed**, 64 pages.
- Full frontend suite before the final added watch-label regression: **642 passed / 46 failed / 688 total**. The same 46 failures were reproduced against the pre-change versions of overlapping files; they are existing source-inspection fixtures in navigation/feed/pricing/profile/leaderboard/watchlist/ticker areas. The full suite is not green; these failures were not skipped or hidden.
- No standalone lint script exists in `frontend/package.json`. Build/type checking and `git diff --check` passed.

From `backend`:

```powershell
$env:PYTHONPATH='.'
$env:DATABASE_URL='sqlite:///:memory:'
.\.research-layout-venv\Scripts\python.exe -m pytest tests/test_research_worker_lock.py tests/test_operational_intelligence.py tests/test_research_evidence.py tests/test_research_claim_matching.py tests/test_research_memory.py -q --basetemp=.local/layout-tests-v5
```

- **55 passed**, two Starlette/AnyIO deprecation warnings.
- Used a disposable Python 3.14 environment because the repository's old local Python 3.12 environment points to a missing interpreter. Production still uses its pinned runtime/dependencies; these local tests do not establish every production dependency combination.
- Regression cases cover quoted source passages, rejected unknown/null/non-string passage IDs, no invalid event/chunk persistence, reuse, full transcript resume, atomic replacement, transcript-only coverage preservation, and correct document counts.

## Visual QA

Chrome production QA used the signed-in MU page. The computer-use skill guided live navigation and visual inspection.

- Desktop: related briefs appear before Company Developments; detailed module absent from Overview; sources and earnings-call coverage visible; concise operating risks/catalysts appear in existing Overview panels.
- Research detail links work repeatedly after returning to Overview, including when the URL already has `#research`.
- Mobile at 390 × 844: stacked Research content and Overview risk/watch cards verified. Document scroll width was 375 px, with no horizontal page overflow. Temporary viewport override was reset.
- Desktop and mobile screenshots were displayed in the task conversation.
- No private production thesis was created for QA.

## Release and limitations

Core frontend/coverage implementation: `335025b4` through `4d663ed7`. Final passage-ID extraction fix: `f6c4c032`.

API and cron deployment [35555592979](https://github.com/moore11j/congress-tracker/actions/runs/35555592979) succeeded, including readiness checks and production route/privacy smoke checks. Vercel deploys the same main branch automatically.

The deployed frontend version endpoint confirmed `f6c4c03291c5506de4bcb6250ca7ab23dd5996af`. Final bounded transcript pilot [35555696622](https://github.com/moore11j/congress-tracker/actions/runs/35555696622) completed successfully:

| Ticker | Events written for final processing revision | Model calls | Transcript coverage |
| --- | ---: | ---: | --- |
| MU | 15 | 2 | Ready; 1 document |
| NVDA | 32 | 3 | Ready; 1 document |
| AAPL | 44 | 3 | Ready; 1 document |

All eight final extraction calls returned HTTP 200. The earlier HTTP 400 remains in historical diagnostics, not the final run. Earlier-version events are superseded on successful replacement rather than accumulated as active duplicates. Public APIs independently confirmed all three transcript sources ready; Apple still has one older rejected press-release extraction and truthfully reports partial press-release coverage. The transcript-only verification deliberately did not reprocess unrelated news/releases. No private matching calls were made because there were no active production theses.

News/press-release analysis still uses the provider's available excerpts, not guaranteed full articles. Transcript availability depends on provider coverage and access. Coverage is ticker/source-specific, not a claim that every ticker has complete coverage. A successful check with no findings does not mean a business has no risks.

Source grounding guarantees excerpt provenance, not perfect interpretation, direction, or materiality. The UI retains attribution, confidence and evidence review rather than presenting AI judgments as certainty. Existing quote/data quality limitations outside this task remain unchanged.
