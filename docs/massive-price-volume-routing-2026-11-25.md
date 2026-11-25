# Selected price-volume routing

Prepared November25,2026. Not deployed; no source activation or pricing change.

## Reproduced problem

A read-only production audit at02:07UTC found60 `ticker:intraday-price-volume` FMP requests in the preceding six hours despite `STOCK_PRICE_PROVIDER=massive`. The legacy helper requested FMP minute bars directly and could reuse a legacy in-process snapshot after selecting Massive. Five installed files, including main, quote lookup, the Massive client and core/transcript selectors, match the inspected baseline after normalizing Windows/Linux line endings. The first raw-byte comparison failed on line endings and is not treated as runtime acceptance.

## Change

Public price-volume inputs now read the existing prepared Massive quote through the cache-only quote service. The exact price/observation/source binding,30-minute freshness limit and positive finite price must qualify. Missing, stale, future or unverified observations queue the existing background quote job and leave daily evidence intact. This page path makes no provider request. The retained FMP helper also refuses use when Massive is selected, before consulting its old process cache.

Fresh snapshots use the New York session date, retain their actual observation timestamp and identify the15-minute delay in the summary. Unknown current-session volume stays unknown rather than borrowing an older daily or fundamentals volume. A newer daily record cannot be relabeled as the minute quote. Legacy FMP-selected behavior and existing feature gates remain.

## Verification

Local candidate:134passed/7failed across Massive public callers, price core and ticker signals summary. All seven failures reproduce on unmodified HEAD2514d806 in the ticker summary suite,53passed/7failed. They concern existing confirmation/context-coalescing fixtures; no full broad-suite pass is claimed. The81 Massive checks pass, including nine new source isolation, persisted cache, missing/stale/future/unverified/invalid quote, zero/unknown volume and session-date cases. No actual data-provider call or customer send was used.

Production evidence is retained outside the PR under `artifacts/fmp-migration-readiness-nov25.json` and `artifacts/intraday-runtime-baseline-nov25.json`. This repair closes one observed call path only. The same six-hour audit recorded1220 FMP call events across eight categories; core fundamentals, transcripts, market-cap/sector consumers, institutional publication and final scheduled zero-FMP acceptance remain open. Telemetry events are not independently verified HTTP receipts.

Exact Linux CI and release approval, installed hashes and selected public/scheduled observation remain required before calling this production-fixed. The running institutional replay and its frozen inputs are separate and unchanged.
