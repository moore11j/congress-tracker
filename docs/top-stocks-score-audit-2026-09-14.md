# Top Stocks confirmation-score audit

Audited the production leaderboard and stored scoring inputs on September 13, 2026 PDT (September 14 UTC). The leaderboard snapshot was generated September 11 at 6:03 PM PDT (`2026-09-12T01:03:48.646617Z`).

## Answer

AMZN's current ticker calculation reproduces **100 / 100**. The leaderboard's **77** is an older snapshot produced through a different input-assembly path. It should not be presented as AMZN's current ticker confirmation score. This audit checks consistency with the existing ticker model; it does not recalibrate that model or treat 100 as a probability of success.

All ten entries in the screenshot disagree with freshly calculated ticker scores. The table below preserves the screenshot's original order; it is not the corrected ranking.

| Stock | Saved leaderboard | Reproduced ticker score |
|---|---:|---:|
| TSM | 82 | 86 |
| AMZN | 77 | 100 |
| BWFG | 77 | 72 |
| BSX | 72 | 75 |
| MSFT | 71 | 86 |
| BRK-B | 71 | 96 |
| BABA | 70 | 68 |
| APH | 70 | 100 |
| AON | 70 | 75 |
| BE | 69 | 74 |

These are fresh calculations from production inputs, not assertions that every previously cached ticker page already displayed these values. Some ticker caches were also older than their underlying data.

## Every additional stock in the filter tabs

There were 27 unique symbols across All Stocks, US, size, and sector filters. Of these, 26 differed from the saved leaderboard; CNXC was unchanged.

| Stock | Saved leaderboard | Reproduced ticker score | Current bullish-score qualification |
|---|---:|---:|---|
| AZN | 66 | 54 | Below 60 |
| GOOG | 65 | 78 | Qualifies |
| AAPL | 64 | 85 | Qualifies |
| MDB | 64 | 68 | Qualifies |
| WDC | 61 | 90 | Qualifies |
| SITM | 60 | 79 | Qualifies |
| MANH | 63 | 52 | Below 60 |
| OUST | 62 | 53 | Below 60; mixed |
| HQY | 60 | 85 | Qualifies |
| CNXC | 67 | 67 | Qualifies |
| KWY | 66 | 53 | Below 60 |
| DSP | 64 | 51 | Below 60 |
| SLDE | 61 | 65 | Qualifies |
| AVGO | 67 | 85 | Qualifies |
| AMAT | 67 | 86 | Qualifies |
| APP | 60 | 78 | Qualifies |
| BKNG | 60 | 78 | Qualifies |

Qualification does not guarantee a top-ten position. The candidate pool and each filter must be ranked again after scoring.

## Findings and repair

1. The old leaderboard persisted screener scores once per business day. It could retain those scores throughout a weekend while ticker inputs and caches changed.
2. Ticker pages merged technical price/volume evidence, disclosure-card inputs, and a 30-day contract context after the common lower-level calculation. The screener snapshot did not perform that merge. Using current inputs, AMZN was 97 before the ticker merge and 100 afterward. Thus refreshing the old leaderboard alone would not fix agreement.
3. Paginated/filtered signal cards could replace source evidence after scoring, potentially changing a later tier-redacted score. Completed shared score bundles now retain their scoring evidence independently of those cards.
4. The repair builds Top Stocks with the same 30-day ticker context, qualifies only afterward, and retains all discovery candidates. On reads it takes the currently valid canonical ticker-cache score when available, applies the same viewer-tier projection, and re-ranks every filter. It returns only leaderboard fields, never the private source bundles.
5. Authenticated score responses use `private, no-store` to avoid continuing to serve an old HTTP snapshot. Obsolete snapshots and old ticker-cache versions cannot silently supply incompatible scores.
6. An incomplete-fundamentals helper could perform a live provider fallback. Scoring now uses a direct stored-row read. The first full-universe dry run was stopped when this behavior was observed; the corrected preview blocks fundamentals-provider calls explicitly.

The fix is in the working tree. Deployment and a refresh of the prepared Top Stocks snapshot are required to change the live leaderboard. Existing historical score/outcome records are not rewritten by this repair.

## Evidence and verification

- Reusable audit: `backend/scripts/ops/audit_top_stock_scores.py`.
- Local production evidence: `backend/.local/top-stocks-production-audit.json`.
- Guarded ranking preview: `backend/.local/top-stocks-repair-preview.json`.
- Test logs: `backend/.local/top-stocks-tests.log` and `backend/.local/top-stocks-baseline-tests.log`.
- Regression coverage includes AMZN 77→100, qualification before/after scoring, re-ranking every filter, new entrants, removed mixed/below-threshold entries, expired/obsolete/redacted caches, viewer-tier agreement, and no provider hydration from incomplete fundamentals.
- Four unrelated ticker tests were reproduced failing on the unchanged `HEAD` implementation: stale memory-cache fallback, build coalescing, single-insider bearish classification, and conflicting-source classification. Their expectations were not changed to conceal the failures.
- The expanded backend run passed 148 checks. Four additional screener failures (CSV-export entitlement and three free-plan cap expectations) were also reproduced on the unchanged implementation, for eight pre-existing failures total. All eight selected frontend checks passed.
- The final production preview scored 500 candidates with zero incomplete bundles and zero fundamentals-provider attempts. Its top ten were AMZN 100, BA 100, APH 100, BRK-B 96, BBY 93, AMD 91, CDNS 88, CACI 87, MSFT 86, and TSM 86. This is a fresh unredacted calculation preview, not a deployed ranking; viewer tier and valid ticker caches can affect the displayed list.

Two separately loaded pages can straddle a real data refresh; their scores should be compared at matching update times. The repair removes the separate leaderboard calculation and long-lived HTTP score cache, while retaining bounded background discovery of the stock universe.
