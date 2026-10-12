# Issuer financial reader integration, October 12, 2026

## Scope

Continues [the deployed retained-source collector](issuer-financial-staging-2026-10-12.md). The selected SEC/free financial reader and the separately gated preparation reader can now use the reviewed AZO FY2026 annual statement when it is newer than the available periodic SEC financial statement. Public core selection stays FMP until the broader acceptance gates pass.

The reader verifies current company submissions still identify the reviewed earnings accession as latest, then recomputes the stored receipt from the retained originals in a read-only database session (PostgreSQL five-second statement bound). Missing, changed, held, future or superseded evidence cannot replace SEC values. Equal/newer periodic SEC statements retain priority. No new HTTP call is made by this reader.

When the annual period advances from May9 to August29, six source-derived metrics replace older values. Older cash-flow, balance-sheet and market-derived values are cleared, rather than relabeled as August data. Current/prior statement inputs, formulas, periods and availability remain explicit and support later Massive valuation calculations. Finnhub's earlier FY2025 EBITDA growth is held against this FY2026 basis. Full coverage is not claimed.

The existing earnings warmer now includes the reviewed issuer if its Security exists; its five-scope cap, serialization, cooldown and recurring timing are unchanged. AZO-only preparation/cache versions require refresh; other issuers' caches remain valid.

## EPS contract finding

The actual production FMP comparator has EPS growth4.912634408602152%, matching basic EPS156.11 versus148.80 in the annual release. The first standalone preparation used diluted EPS152.55 versus144.87, producing5.30130461793332%. These are different definitions, not transport rounding.

Parser version2 preserves both inputs, prefers basic EPS for the existing `eps_growth` field and explicitly labels a diluted fallback when basic EPS is absent. Other five metrics already matched. This supersedes the first standalone parser's diluted-only EPS choice, not its source evidence. No analyst-consensus estimate is inferred.

## Validation

Python3.14.2:163 focused financial, collection, combination, provider-baseline and alert checks pass; final14 reader checks include the new full alert-evaluator case and read-only reader guard. Initial fixture expectation used billions instead of reported thousands, corrected to2million without changing application units. First copied test path was wrong and collected nothing; corrected invocation passes.

Actual retained originals plus the captured production SEC/Finnhub/FMP preparations pass the real reader, free-source combination, canonical-cache adapter, screener and summary projection in isolated SQLite. All six new values equal the FMP comparator within1e-9; largest absolute delta1.78e-15. Two canonical fixture rows (FMP and free) remain identical on repeat; FMP is preserved, zero events/research documents/emails. Older cash-flow/current-ratio fields and FY2025 EBITDA growth are held. CPU0.21875seconds excludes fixture/source setup and is not a production capacity benchmark. Receipt `issuer-reader-replay-1791822305.json`; comparator `azo-financial-base-1791822061.json` was captured in an explicitly read-only production transaction.

The replay initially set its observation before creating the source preparation, correctly triggering the future-observation guard. Its clock was moved after collection; no guard weakened. The full evaluator test uses the actual reader/combiner: source/formula transition rebaselines without an alert, genuine later threshold change triggers once and repeat produces no duplicate.

## Release gates

Implemented and locally verified; exact deployment/runtime, production preparation refresh and ordinary scheduled observation pending. No public core activation, institutional activation, new subscription, host size, recurring time, landing or video change, or FMP cancellation. Broad issuer/period coverage, all score/ranking/digest/watchlist consumers, institutional publication/transcripts and final Massive validation remain open.
