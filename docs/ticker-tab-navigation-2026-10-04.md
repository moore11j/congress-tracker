# Ticker tab order and scrolling — October 4, 2026

The requested order is Overview, Chart, News, Events/Filings, Financials, Ownership, Congress, Insider, Government contracts, Signals, Macro Positioning, Valuations, Analysts and Research. Events/Filings again contains press releases, SEC filings and disclosure activity; old `#filings` links select the combined tab. Its empty state now accounts for SEC records too.

Research was never removed. It sat beyond the visible end of the overflowing row. The old arrow overlays had `pointer-events-none`, so clicking them activated the underlying tab. Ticker navigation now passes explicit handlers to real, labeled scroll buttons. Each moves the strip by three quarters of its visible width with native smooth scrolling, honoring reduced motion. Dedicated edge space prevents click-through; disabled boundary controls cannot select a tab. The existing global navigation indicator behavior is unchanged.

## Checks

- Four new executable/rendered regressions pass: scroll buttons and directions, reduced motion/boundaries, requested tab order and retained Research, combined source sections. The two existing SEC-title tests pass. Across the related lazy-tab/activity tests, 11 pass and the same two previously documented source assertions fail (signal-band label and old analyst empty-state copy).
- TypeScript passes with `--noEmit --incremental false`; local Next.js compiles the actual ticker route successfully. No full-suite pass claimed.
- Local browser using public AAPL data: right clicks move scroll offset 0 → 559 → 739 while Overview and the URL stay unchanged; Research then opens its published briefs. Left click moves 739 → 180 while Research and `#research` remain selected. Combined Events/Filings contains Press Releases, SEC Filings and Disclosure Activity. No authentication bypass or production data writes.
- Preserved the concurrent loading-race repair, now committed separately as `472fe668`, and unrelated shared documentation.

## Release

Frontend release pending verification. Backend, data, preferences and email are unchanged.
