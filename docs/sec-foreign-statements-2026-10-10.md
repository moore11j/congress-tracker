# SEC foreign annual financial statements

October10,2026. Implemented and locally verified; frontend/backend release and ordinary scheduled/public verification pending.

The selected SEC historical reader excluded foreign annual statements because it accepted only US GAAP 10-K/10-Q USD facts. This adds reviewed NVO/DKK, BABA/CNY and TSM/TWD annual reporting bases with exact CIK identity. It does not infer currency from the exchange or combine convenience USD translations across years.

The reader returns up to six completed annual periods from standard US GAAP or IFRS concepts and preserves input unit, accession, dates and source hashes. Same-period expenses from another accession are held after partial restatements. Quarterly results, current TTM figures, estimates and ADR EPS remain unavailable. Foreign amounts do not enter current USD fundamentals caches, scores or alerts. The financial charts display the reporting currency and an annual-coverage notice.

When companyfacts lags an available annual filing, preparation fetches at most one original 20-F inside the existing serial job. The original must pass namespace, CIK, form, date, unsegmented context, reporting-unit, numeric-transformation and conflict checks. Foreign originals have a16MB parser bound; existing domestic8MB limit remains. No schedule, source-concurrency, provider selection or request-loop changes.

## Original sources and replay

- [Novo Nordisk annual statements](https://www.sec.gov/Archives/edgar/data/353278/000035327826000012/nvo-20251231_d2.htm),10,592,967bytes, SHA256 5f37750bbdf98737cdd06f5d71e9db4252b9c41493a59c32e2655e2a7407869e. Reporting/functional currency DKK;2025 revenue309.064bn.
- [Alibaba annual report](https://www.sec.gov/Archives/edgar/data/1577552/000119312526231755/baba-20260331.htm),11,745,279bytes, SHA256 16813cd58a7c7d42ea53c45b1db0f5c9a792fa1843ea5d0acd260bd6d6c59ad1. CNY reporting basis; year endingMarch31,2026 revenue1.02367trn. Missing supported capex remains missing.
- [TSMC annual report](https://www.sec.gov/Archives/edgar/data/1046179/000162828026025362/tsm-20251231.htm),10,355,932bytes, SHA256 1971f40287b9b085f964e3acd60567fc35b792641ce5431d5fe5a65cae858bba. New Taiwan dollar reporting basis;2025 revenue3.8090543trn. Captured companyfacts stopped at2024; exact original2025 inline facts recover the newer annual period with18 standard facts and source/context provenance.

The captured three-company reader/cache replay prepares six annual periods each with seven total captured reads, identical cache/timestamp repeat and zero further transport/canonical writes/emails. A separate22-company core preparation replay preserves all22 existing metric dictionaries.

## Checks

Python3.14.2:115 focused financial/source checks pass. A further overlapping consumer group has73passes and one forward-P/E expectation failure reproduced identically using unchanged HEAD implementations. No full-suite pass. An initial nonexistent test-file path exercised no tests and was corrected.

Actual isolated PostgreSQL16.15 with one pooled connection verifies TSM's original annual recovery, nine refused overlapping invocations, no checked-out pooled connection or idle transaction during source reads, source429 cache preservation, advisory-lock release, ordinary demand preparation, unchanged queue identity/attempts and repeat without transport. Initial harness-only expected-read count was corrected from9 to6 at the second refresh; final full check passes. Disposable schema removed and local server stopped.

Two executable currency-format checks and four existing financial UI checks pass; TypeScript passes. Diff check passes. No rendered browser claim.

## Release order and remaining gates

Merge normally and verify production frontend contains reporting-currency support before dispatching the backend release. Then verify exact backend hashes, ordinary three-symbol preparation, repeated backend/site results and continued two-service health. Unsupported quarterly/ADR/forecast/current-core coverage remains a migration gate; this is historical statement recovery, not full FMP exit. Massive stays last; no subscription or cancellation.

Local evidence: foreign-statements-replay.json, foreign-demand-postgres-result.json, sec-foreign-core-22-replay.json and original source manifest under artifacts/direct-feeds/free-replacements-2026-10-09.
