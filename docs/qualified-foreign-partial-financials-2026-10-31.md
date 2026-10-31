# Qualified foreign partial financials, October 31, 2026

## Problem and repair

A complete public-source replay found that the standard USD US GAAP reader qualifies Adagene's current 6-K, but its missing-revenue fallback discards that selection and applies domestic-only filing rules. The shared fallback now revalidates the same original-bound foreign selection and retains its current period and provenance. It is not a ticker allowlist. Native-currency and domestic paths keep their existing rules.

Twelve whole public SEC originals (1,988,633 uncompressed bytes) are retained as hashed compressed fixtures: current companyfacts and submissions, six filing covers and four exhibits. Complete bounded discovery and original revalidation precede projection. Current ratio becomes 132,408,623 / 26,361,197 = 5.022860798013079. ROE becomes -20,511,005 / average(37,911,789, 89,453,418) * 100 = -32.208175973835615. Net income uses annual plus current half-year minus prior half-year; none of these values are annualized estimates. Missing current TTM revenue and capital expenditure remain missing, so revenue growth and free cash flow remain unavailable.

## Compatibility and validation

The exact deployed e53cbdb3 source reproduces the prior supported-period hold using the same originals. The candidate recovers only the two supported metrics, with complete repeat equality and source-bound provenance. A separate offline replay compares all 284 retained companyfacts/submission pairs against that deployed source: 116 successful complete outputs and 168 holds remain identical. No global cache version reset or methodology/score change is made.

Windows financial and consumer regression run:151 passed, followed by15 passing tests including the added preparation/cache-repeat check (152 distinct checks across these runs). Linux confirmation is pending. Tests cover missing/tampered source proof, currency, taxonomy, CIK, accession, filed date, report period, cover and selection; no bare unqualified 6-K admission. Complete-original discovery remains bounded to six logical reads per continuation. Score and watchlist checks verify recovery establishes a baseline, repeats do not duplicate, a genuine subsequent crossing triggers once, and rollback does not trigger. Local SQLite has zero canonical events or email deliveries; preparation writes only isolated source caches. These tests do not establish all scheduled production alert/digest consumers.

Receipts: `foreign-partial-parity-oct31.json`, `foreign-partial-windows-oct31.xml`, `foreign-partial-preparation-windows-oct31.xml`. The initial new test asserted the wrong exception hierarchy; the actual existing typed source rejection was correct and the assertion now accepts the established coverage/foreign-source error types. No source guard was relaxed to make tests pass.

## Wider source evidence and release state

The fresh NBIS, ADAG and WB local replay completes full discovery and repeated original revalidation for all three. NBIS still has only a supported current ratio and WB still lacks a supported core table ratio; both retain partial statement panels. Old production ADAG/WB qualification counts refer to earlier reader versions and are not current publication readiness. Recent attempt/backoff metadata does not justify a new scheduler change; no forced refresh or deadline change was made.

Implementation is local; PR, CI, deployment and installed verification are pending. Core financials, transcripts and prices remain FMP. Discovery collection stays enabled and its reader stays disabled. Broad coverage, ordinary operation, remaining consumers, institutional validation, final Massive activation and zero-FMP traffic remain gates. No new paid plan, compute, video, landing-page or publication change is included.
