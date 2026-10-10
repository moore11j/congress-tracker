# Bounded source availability probes, October 10

PR540 diagnostics captured AAPL signal loading5.54s during a7.22s build. A later bounded profile did not reproduce the entire outlier, but found a global institutional_symbol_summary count taking193/311ms while checking only whether data exists. Its plan traverses an estimated128060 entries. Government summary loading also counts all contracts solely for availability.

Replace those two boolean checks with SELECT id LIMIT1. Missing tables, empty datasets, absent symbols and populated summaries retain their distinct outcomes. The public government overlay still returns its exact indexed_row_count; true totals and per-symbol aggregates remain unchanged. No approximate count or cache lifetime change. No provider, score, event identity, entitlement, job cadence, schema or capacity change.

## Validation

146 tests pass across availability, institutions, contracts, canonical ticker projection, confirmation and existing daily/weekly alert parity. Four failures reproduce unchanged on the original two service files: institution profile holder-name lock expectation; government feed description key; projected external_id key; and the reserved NONE symbol expectation. They are not passes; no full-suite claim. New availability fixture initially expected route status inactive instead of service status ok/active=false; corrected to the existing service contract.

Actual production PostgreSQL read-only repeatable-read comparison,3s statement cap, covers AAPL/NVDA/MSFT/BRK.B/IBM/SOFI/MRNA and an absent symbol. Old/new responses and repeat are identical for both readers. Institutional fingerprint075e26beeaea6ecc151847b05b64a0c591322bc0ec8c3cc182c849f2d724595d; government fingerprint72b3607369cb5450ec52350de341ab0381a67b952dea70eef21026b56f5aa139. Whole-reader timings old/new645/46ms and63/16ms are sequential samples affected by cache warming, not a controlled end-to-end speedup claim. Each new probe's EXPLAIN ANALYZE returns one row using two shared blocks in0.014/0.013ms. No production writes or index changes. Artifact source-probe-parity-1791668920.json.

Deployment and exact runtime/public verification remain pending. The change removes measured unnecessary work but does not establish the entire root cause of intermittent multi-second ticker builds. Existing timing diagnostics remain; inspect further slow-stage evidence after release. FMP remains enabled and Massive remains last.
