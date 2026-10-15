# Recover partially updated SEC companyfacts

October15,2026. Deployed as af59d62b; exact runtime and bounded live preparation verified below. Core financial selection remainsFMP.

## Reproduced failure

The original-filing collector used the newest date across all core companyfacts fields as proof that an entire report was available. A same-period balance-sheet or income observation could therefore suppress original recovery even when revenue history was still incomplete, leaving the complete financial projector unavailable.

Two simulations use actual retained Verisk and Sonoco originals. Add the exact current-assets observation from each original to its otherwise lagging companyfacts response. Before repair both acquire zero originals and fail with `No current complete trailing revenue period`. These are controlled partial-update scenarios, not evidence that the live SEC feed currently has that state. Saved companyfacts-only diagnostics also omit optional original/history supplements and must not be presented as current production coverage.

## Change

When the latest available periodic filing has the same date as the newest companyfacts observation, acquire its original if the trailing revenue history is incomplete. A complete history keeps the existing no-request path. Older reports, same-day/unavailable filings, unsupported taxonomies/currencies, identity mismatches and ambiguous filings retain their guards. No source amounts, calculation formulas, tag priorities, schedules, cache lifetimes or public selectors change. Normal cached preparation repeats remain request-free; a due incomplete scope may need one bounded original request.

## Validation

All367 selected Python3.12.3 tests pass, including inline acquisition, foreign currency/ASML/TSM, source precision, debt policy, preparation, warming, historical caches, continuity and financial consumers. The initial focused133 also passed; these overlap and are not added to the367 count.

Actual complete-original replay on Python3.12.3 restores exactly nine prepared Verisk metrics and eleven Sonoco metrics. Net-debt/EBITDA is2.427042079207921x and2.8041761239433374x respectively. Every existing projected value and all evidence are identical to the unmodified original-file path, except the expected companyfacts SHA change for the simulated partial input. Each recovery makes one original read. Real isolated SQLite preparation/repeat preserves values, one source cache, zero canonical financial rows/events and no new cached requests. Network access is blocked. This is retained-source evidence, not live publication.

Receipts: artifacts/partial-facts-baseline-oct15.json, partial-facts-replay-oct15.json, partial-facts-release-oct15.log. Actual source manifests: artifacts/maturity-debt-oct14/manifest.json and artifacts/debt-reconciliation-oct14/manifest.json. All source hashes verified. Tests and replay are terminal.

## Ordinary debt-policy propagation

Separate read-only aggregate evidence at06:02UTC shows33 current debt-policy caches:30 beyond GT/CPB/CMCSA explicitly warmed earlier, five available ratios and28 held. Four ordinary batches at05:46,05:51,05:56 and06:01UTC each completed all10 planned scopes with zero canonical writes/emails. One reportsok and threepartial; completion does not mean full source coverage. This verifies ordinary policy propagation, not complete financial replacement or sustained capacity. Receipt: artifacts/financial-policy-propagation-1792044149.json.

All broader financial coverage, institutional activation, transcripts/valuation/analysts, scheduled consumers, sustained capacity, finalMassive, zero-FMP and billing gates remain open. Pending CAT/institutional payload permissions and DCF preference unchanged. No subscription, customer delivery, video, landing or cancellation change.

## Deployed release and live verification, 06:10 UTC

[PR668](https://github.com/moore11j/congress-tracker/pull/668), source bfe10f377efb8d6eb5f78b5a53dae1b9afbf033b, merged as af59d62be623998207264b5d19cdaea3748c9392. Backend workflow38690477226/job116274955179 completed successfully; tested application files match main. Secret scan and Vercel preview pass. Unchanged dependencies retain33 Pillow advisory rows and five frontend production vulnerability groups (four high, one critical); no dependency manifests changed. The chat attachment limit is100, so the normal PR link is retained.

Production Python3.12.15 verifies four source hashes and exact local/runtime full acquisition output for a synthetic partial update. Complete revenue and older report cases make no requests. The verifier blocks network and uses no production records or writes. Receipt: artifacts/partial-facts-runtime-1792044501.json. The two complete public-original/cache replays also pass on Windows Python3.14.2, in addition to Linux3.12.3; the latest replay receipt records the Windows runtime.

Bounded normal production preparation for VRSK and SON makes six public SEC requests in4.376seconds. Each canonical debt ratio and full policy evidence matches the independently retained original; exact raw hashes bind to the received bytes, and cache repeats make no requests. Source-isolated preparation caches only; zero canonical financial/event writes, emails or production payload exports. Receipt: artifacts/partial-facts-warm-1792044577.json. This verifies current preparation, not an observed live partially updated SEC response or ordinary trigger of the repaired branch.

The first live verifier rejected Verisk content because its terminal delivery script is followed by a newline before the closing HTML tag. Independent public re-fetch confirms the only changed bytes are the volatile script URL. The verifier now recognizes that exact terminal layout while comparing every other byte and retaining exact received-file hashes. No production parser/hash rule was changed or weakened. The earlier failed comparison is not counted as successful evidence.

Immediate public readiness is200/databaseokay; Feed0.664s and Insights0.812s return200 without the server exception page. Receipt: artifacts/fusb-public-1792044498.json (existing probe filename). These samples do not prove sustained capacity. Core remainsFMP; no source selector, schedule, host, subscription, video, landing page, customer delivery or cancellation change. All new processes are terminal. Broader coverage, ordinary consumer verification and the full FMP retirement gates remain open.
