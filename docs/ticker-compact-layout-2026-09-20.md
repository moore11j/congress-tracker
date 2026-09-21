# Compact ticker Overview and Research

## Scope

Implements the approved compact/editorial mockups. This is a frontend presentation change, not a new Research Memory monitoring phase. No backend, database, AI prompt, scoring, ingestion, or production configuration changes were made. Nothing was committed or deployed as part of this implementation.

## Layout changes

- Overview combines the 30-day confirmation score, source alignment, and interactive trend in one responsive summary band.
- Source alignment replaces the two large opposing-evidence boxes with wrapping source names, explicit direction counts, and a count bar. Neutral sources remain available. Missing evidence is not rendered as a fully bearish bar.
- Full confirmation interpretation, alignment explanation/methodology, recalibration descriptions, and historical comparison reasons remain accessible through native disclosures. Historical reasons are no longer truncated.
- Catalysts/What Changed and Risks/What to Watch Next use two independently flowing columns with thin dividers instead of equal-height nested cards. Mobile stacks these sections.
- Research briefs use editorial rows. Company developments use one compact coverage strip and one evidence list, with category filters and latest/material-first sorting.
- Rows sharing an evidence event ID appear once with all applicable category tags. Every category-specific interpretation remains in the expanded detail; unrelated events with identical headlines are not merged. No new item cap was introduced.
- Original source URLs, publication dates, confidence, materiality, summaries, excerpts, coverage limitations, and check timestamps remain available. Missing/disabled/partial coverage states remain distinct.
- Existing entitlement checks, feature flags, shared API fetch, private thesis entry points, unrelated ticker tabs, and right-hand sidebar modules are preserved. Locked confirmation content is inert for keyboard navigation.

## Files

- `frontend/app/ticker/[symbol]/page.tsx`
- `frontend/components/ticker/TickerSourceAlignment.tsx` (new)
- `frontend/components/ticker/TickerDecisionPanels.tsx`
- `frontend/components/ticker/DecisionTrendChart.tsx`
- `frontend/components/ticker/TickerContextCard.tsx`
- `frontend/components/ticker/TickerOperationalIntelligenceCard.tsx`
- `frontend/components/research-memory/ResearchCoverage.tsx`
- `frontend/lib/tickerResearchFindings.ts` (new)
- `frontend/tests/ticker-compact-layout.test.mjs` (new)
- This report.

## Verification

Commands run from `frontend` unless noted:

```powershell
.\node_modules\.bin\tsc.cmd --noEmit
node --test tests/ticker-compact-layout.test.mjs tests/ticker-decision-layer.test.mjs tests/ticker-operational-overview.test.mjs tests/research-memory-phase1.test.mjs tests/confirmation-recalibration.test.mjs
npm.cmd test
npm.cmd run build
```

- TypeScript: passed.
- Focused tests: **31 passed, 0 failed** (including nine new compact-layout tests).
- Full frontend suite: **698 tests; 652 passed, 46 failed**. The suite is not green.
- Baseline verification: existing tests were rerun using a temporary read-only preload that supplied `git show HEAD:frontend/<path>` for the six modified source files. The new test file was excluded from this baseline. Result: **689 tests; 643 passed, 46 failed**. The unique failing test names match exactly. No tests were skipped or hidden to obtain the working-tree result; the baseline helper was removed afterward.
- Production build: **passed**, including lint/type validation and all 64 static pages. The existing outdated Browserslist data warning remains. There is no standalone lint script in this repository.
- `git diff --check`: passed (line-ending notices only).
- Backend tests: not run; no backend code changed.

### Browser QA

Chrome desktop and narrow mobile screenshots were inspected. Checked the combined summary band, source wrapping, editorial brief rows, compact coverage, evidence list, filter buttons, sorting, expanded risk excerpt, keyboard activation of methodology details, and access to the last evidence row inside the desktop Research scroll area.

The 390 CSS-pixel mobile check reported document width 370 pixels with the scrollbar, so there was no page-level horizontal overflow. Desktop checks likewise reported document width below viewport width. The temporary viewport override was reset.

The local browser had no authenticated Walnut session. QA therefore used a clearly labeled development-only fixture with the actual redesigned components, copied Overview composition, sample confirmation/history values, and existing public MU company findings through the unchanged provider. It did not bypass the production authentication/entitlement gates. The fixture route was deleted after inspection. This was not a live production or authenticated end-to-end verification.

Screenshot capture initially timed out intermittently; viewport changes allowed successful desktop/mobile inspection. Screenshot artifacts were not saved as repository assets. The approved design references remain in `output/imagegen/ticker-ui-concepts-2026-09-20/`.

## Performance and limitations

- No additional API fetches, AI calls, database queries, jobs, or dependencies.
- Grouping/filtering/sorting is local over the already bounded API response. Upstream limits remain unchanged.
- Long source text expands on demand rather than being discarded or rewritten. The source evidence itself was not reinterpreted in this UI-only work.
- The full repository test suite has 46 independently reproduced pre-existing failures. They should be addressed in a separate maintenance task.
- Production deployment and authenticated production smoke testing remain outstanding.
