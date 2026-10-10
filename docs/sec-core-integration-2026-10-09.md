# SEC core financial integration

This package prepares current SEC ratios for the existing cache, screener, snapshot, score and strategy consumers. It leaves `FUNDAMENTALS_PROVIDER` unchanged until market inputs, coverage and rebuilt rankings pass their separate activation gates. Historical financial panels already use a different selected provider flag.

SEC and FMP rows remain separate. Source evidence persists with financial snapshots. Missing SEC market fields clear instead of inheriting legacy FMP values; identical observations do not change timestamps. Score, outcome and strategy baseline versions include the SEC methodology, including combinations with the selected analyst methodology and future global FMP retirement. Historical records remain unchanged.

The package includes the opt-in Massive client imported by the SEC acquisition adapter. No price route is switched, no new live Massive request was used in these checks, and Starter activation remains the final paid step. SEC-selected leaderboard enrichment accepts only fresh market values bound to independently captured Massive evidence. Free-only SEC preparation therefore cannot supply market capitalization or average volume yet.

## Validation on October 10 UTC

- Forty-two focused financial, cache, snapshot, preparation and leaderboard checks pass on Python 3.14.2. A later overlapping group of 50 migration, combined-provider baseline, outcome and strategy checks passes.
- The consumer group passes 113 checks, including Top Stocks and monitoring. Six screener failures reproduce identically without this package: two existing intelligence-overlay expectations, Pro CSV eligibility, and three outdated Free plan pagination/cap expectations. This is not a full-suite pass.
- A real isolated PostgreSQL 16.15 schema verifies additive migration, repeat execution, legacy-row preservation, provider isolation, identical observation timestamps and snapshot evidence. The disposable schema was removed and the local server stopped.
- A free-only replay of 41 captured SEC responses for 20 companies passes the real acquisition/cache/summary/leaderboard path with HTTP and market-provider access blocked. Twenty isolated rows remain identical on repeat. Eight companies meet the unchanged three-metric classification minimum; seven have no supported current financial period. All market caps and average volumes remain absent. Captured financial periods are not a claim of current full-universe coverage or ranking parity.

The additive `app.jobs.migrate_fundamental_evidence` migration defaults to inspection. Its explicit `--apply` adds only nullable TEXT evidence columns to the two existing financial tables, rejects unexpected definitions, and uses bounded PostgreSQL statement/lock timeouts and an advisory transaction lock. Apply and verify this before deploying models that read the columns. No production migration, core selection or deployment has occurred for this package at this checkpoint.

Local evidence: `artifacts/direct-feeds/free-replacements-2026-10-09/sec-core-postgres-replay.json` and `sec-core-free-20-replay.json`. Broader prepared coverage, selected consumer operation, fresh score/ranking staging, strategy/alert rebaselining and final Massive market inputs remain required.
