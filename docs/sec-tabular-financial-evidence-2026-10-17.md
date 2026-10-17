# Shared HTML financial statement evidence

## Scope and result, 2026-10-17

Seven complete public SEC exhibits from the existing 49-source discovery cohort contain financial tables without numeric inline XBRL. The new shared `sec_tabular_financials` reader extracts source observations using table geometry and printed labels, rather than ticker-specific column positions. It is a preparation component, not yet connected to automatic acquisition, canonical calculations or public financial readers. FMP selection remains unchanged.

The complete-source replay prepares 141 distinct metric/period/currency observations across six sources. This count includes comparative periods and convenience translations; it is not 141 new product metrics or a whole-universe coverage measure.

| Source | Distinct observations | Important remaining boundary |
| --- | ---: | --- |
| CGNT | 0 | Bare dollar signs do not establish statement currency. |
| NIVF | 16 | Financial June 30 period differs from October 7 disclosure date. |
| NTB | 10 | Bank assets/liabilities are not automatically current assets/liabilities. |
| WB | 18 | Quarter and half-year statements remain separate. |
| YI | 36 | CNY and USD columns remain distinct; reporting versus convenience currency unqualified. |
| YQ | 33 | CNY and USD columns remain distinct; reporting versus convenience currency unqualified. |
| ZEPP | 28 | Parent and consolidated income remain separate observations. |

Twenty-one complete originals (statement, cover and issuer directory for each source) are stored as gzip fixtures with URL, byte count and SHA-256 manifest in `backend/tests/fixtures/sec_tabular_financials/sources.json`. No source excerpts substitute for originals. Repeated equivalent observations retain all source occurrences; conflicting observations are removed from the usable fact set and retained as explicit conflicts. No records, cache, score, alert or publication are written.

## Admission and normalization

Every source must match the issuer directory ticker/CIK, an available filing, the same SEC accession and an exact cover exhibit link. Tables require a matching issuer heading, explicit amount scale, printed period and a proved currency per column. The reader preserves colspan/rowspan coordinates, including blank and separate parenthesis cells. It bounds original sizes, tables, rows and spans. Source and companion hashes bind the extracted evidence.

Income statement, balance sheet and operating cash-flow observations are distinct. Net income within cash-flow reconciliations is not admitted as income-statement evidence. Adjusted, non-GAAP and pro-forma headings are excluded. Missing dashes remain absent, explicit zero remains zero, and unknown currencies or conflicting unit declarations are held. Financial periods cannot follow filing availability. Quarter, half-year and fiscal-year boundaries come from the printed statement, not the directory event date.

Testing caught unbounded currency aliases matching AUD inside unaudited and similar words. Currency codes now require token boundaries. It also exposed net income from a cash-flow reconciliation being counted as an income observation; classification now requires the actual statement kind. These failures were repaired without weakening source or value expectations.

## Validation and remaining work

Initial combined Linux run: 143 passed, 20 failed due to the currency regression. The next focused run: 51 passed, one obsolete occurrence-count assertion failed after removal of cash-flow evidence. That assertion now expects only the two income/comprehensive-income statements. The combined Linux Python 3.12 suite then passed all 169 checks across this reader, core precision, document acquisition and native financial consumers. Subsequent source-identity review required the closest statement heading to match the issuer, preventing an earlier parent mention from authorizing another company. All seven original replay results remain unchanged; final focused Linux Python 3.12 validation passes all 53 tests, including that additional guard.

The final seven-source artifact replay retains 141 observations with zero conflicts; CGNT stays unavailable. The first single-pass replay used 2.328 local CPU seconds; a later exact-repeat run with network explicitly forbidden used 7.109 seconds. Neither is a production-capacity estimate. Complete-source tests prohibit network calls and verify exact repeats, independent values, issuer/accession/availability checks, ambiguous currency/scale, conflicting duplicates and column-span bounds. The recorded artifact is `artifacts/financial-exhibit-followup-oct17/tabular-evidence-replay.json` in the primary workspace.

Next required integration: bind reporting currency and accounting basis to authoritative originals; qualify current and comparative statement selection, amendments, parent/common income and duration compatibility; connect bounded discovery and preparation to the existing financial contracts; verify historical panels, ratios, scores and alert lifecycle with exact source replay. No ticker-specific approval list or guessed currency should substitute for these rules. Automatic acquisition and public recovery are not completed by this evidence reader.

Separate PR716 and PR717 remain open and undeployed. Fresh aggregate release guard at 05:37 UTC confirms the protected portfolio refresh live at 756/1135 scopes with zero failures. Do not restart or deploy across that unfinished cycle. Full financial coverage, institutions, transcripts, remaining consumers, sustained capacity, Massive and zero-FMP/billing acceptance remain open.

The latest aggregate guard receipt release-blocker-1792215605 shows 757/1135 scopes, zero failures, no live PID at that read and an unfinished continuation with seven queued jobs. This is not completed operation or permission to deploy.
