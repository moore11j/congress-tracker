# Shared core-fact precision reconciliation

## Confirmed failure and repair, 2026-10-17

The retained RZLV June 30 statement repeats the same entity-wide NetIncomeLoss fact as -139,478,000 (decimals -3) and -139,500,000 (decimals -5). Both have the same period, unit and entity context. The old reader accepts precision-qualified auxiliary duplicates but rejects all unequal core duplicates, so this consistent narrative rounding blocks the complete original.

The shared reader now accepts an unequal core duplicate only when both reported precisions are known and the finer interval is wholly contained in the coarser one. It selects the actual finer source occurrence and retains its fact/context IDs, original URL and hash. A separate core comparison receipt records both values and precisions. No averaging, guessed tolerance, ticker list, price input or cross-period reconciliation is introduced. Every distinct reported precision is checked, preventing an intervening fine value from concealing two conflicting coarse values. Unknown accuracy, equal-accuracy disagreements, sign conflicts, nil conflicts and partially overlapping intervals remain held. Existing auxiliary behavior is preserved.

[XBRL International's duplicate-fact guidance](https://www.xbrl.org/WGN/xbrl-duplicates/WGN-2025-01-14/xbrl-duplicates-2025-01-14.html) distinguishes legitimate rounded duplicates from inconsistent facts and recommends retaining the most accurate consistent fact for downstream processing. Our containment rule is deliberately stricter than the guidance's interval-overlap criterion. This is not a calculation-linkbase validator.

Two complete public originals are retained with URL/hash/size manifests under backend/tests/fixtures/sec_core_precision. The complete statement parses 35 supported facts with repeat equality; changing the rounded loss to a genuinely inconsistent value fails. Source selection is separately held: the cover also links acquired-company statements and auditors' opinions. This parser change does not resolve that ambiguity or authorize publishing RZLV ratios.

## Wider coverage evidence

An offline audit of all 40 held members of the 49-source discovery cohort found possible statement/earnings links in 13 sources. Broadening the link regex indiscriminately also admits AGM results, future earnings announcements and other companies' statements, so that is not a safe replacement.

Eight complete, explicitly reviewed public SEC exhibits were retrieved serially within a ten-request and 40 MB total bound. Seven (CGNT, NIVF, NTB, WB, YI, YQ, ZEPP) contain financial HTML tables without numeric inline XBRL; RZLV contains tags and exposed the rounding defect above. The seven untagged exhibits still require a shared table reader with explicit period/currency/scale and issuer evidence. Quarter and half-year headers may coexist; several filing-directory dates describe disclosure events rather than statement periods. CNY releases contain USD convenience columns, and bank statements do not necessarily classify current assets/liabilities. These are unresolved source semantics, not merely missing aliases. No per-ticker implementation was added.

Research artifacts in the primary workspace:

- artifacts/document-set-cohort-oct17/exhibit-discovery-gap-audit.json
- artifacts/financial-exhibit-followup-oct17/state.json and table-layout-audit.json
- artifacts/financial-exhibit-followup-oct17/rzlv-netincome-precision-evidence.json
- artifacts/document-set-cohort-oct17/replay-core-precision.json

The full 49-source offline repeat retains all nine previously supported sources and 60 metrics exactly, including statement periods, currency and source evidence. Forty source holds remain unchanged. The repair does not claim a new admitted source or whole-universe coverage. Network requests and production writes in that repeat are zero; 24.234 local CPU seconds is not production capacity evidence.

## Validation and release state

Initial Windows validation had test-fixture errors (an unselected Assets tag and noncontained three-precision intervals) plus the older deliberate blanket-core-rejection expectation. Fixtures now use current asset and equity instant contexts and truly contained intervals; the old policy test now explicitly checks accepted containment and rejected real conflicts. Final Linux Python 3.12 validation passes 267 tests across core precision, inline parsing, leverage integration, independent fields, currency guards, document sets and native financial consumers. This includes source/score/watchlist recovery, repeat, genuine crossing and rollback cases. The unrelated frontend buildinfo hash remains A185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA. This is a focused suite, not full application or live scheduled acceptance.

PR716 at e230a888 remains open with all final-head CI successful. Its exact release verifier is pinned to that Git revision so subsequent work cannot silently change the expected deployed bytes. Production remains a5976245; no merge or deployment was attempted during this follow-up. Read-only receipt release-blocker-1792214621 confirms the protected portfolio cycle is genuinely live, 546/1135 scopes and zero failures; do not interrupt or deploy across it. Core FMP selection, institutions publication hold, schedules and host sizes are unchanged. Installed validation, ordinary propagation and broader financial/source/consumer/capacity/Massive/zero-FMP/billing gates remain open.
