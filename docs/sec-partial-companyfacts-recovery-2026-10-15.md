# Recover partially updated SEC companyfacts

October15,2026. Implemented locally; release/runtime pending. Core financial selection remainsFMP.

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
