# Shared HTML financial statement evidence

## Scope and result, 2026-10-17

Seven complete public SEC exhibits from the existing 49-source discovery cohort contain financial tables without numeric inline XBRL. The new shared `sec_tabular_financials` reader extracts source observations using table geometry and printed labels, rather than ticker-specific column positions. The initial preparation component is now followed by a local integration into bounded acquisition, canonical current-ratio preparation and public financial panels. This integration is not deployed or accepted for cutover. FMP selection remains unchanged.

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

Draft [PR718](https://github.com/moore11j/congress-tracker/pull/718) is pushed at5e5e669f, stacked on PR717; no deployment. Final53focused Linux tests pass. Artifact table-accounting-basis-audit.json captures explicit NTB interim US GAAP and YI/YQ convenience-translation language; other statement/accounting declarations still need qualification. Latest protected cycle is genuinely live at780/1135 with zero failures (receipt1792215813); release remains held.


## Accounting qualification and consumer integration, October 17

The shared normalizer reparses complete originals and requires explicit U.S. GAAP statement/performance/result declarations. A bare GAAP label, negated declaration or jurisdiction reference does not qualify. A single explicit statement currency is preserved; multiple currencies require an explicit from/to convenience-translation declaration. Native CNY observations are selected without converting or using the USD convenience values. Consolidated income/equity remain separate from parent/common-holder income/equity. Records use honest `html:` source labels and original row/column provenance, not manufactured XBRL tags.

NTB (10 USD observations), WB (18 USD) and YQ (22 native CNY of 33 original observations) pass source normalization. CGNT/NIVF/YI/ZEPP remain held for missing explicit accounting declarations in these particular originals; CGNT also lacks proved currency. Their raw observations are not silently promoted. Bank current assets/liabilities and TTM/common-share/EPS values are not inferred.

Acquisition shares the existing six-original budget with the inline/native paths, inspects at most three candidate covers and selects one financial exhibit per cover. A cover-row description supports numeric exhibit anchors. Explicit financial statements take priority over a general release; multiple statement links remain ambiguous. Older unqualified statements can be ignored only when their filing date is no later than the selected printed period; potentially newer holds remain blocking. Same-period/date ambiguous filings stay held. Source discovery remains bounded rather than a whole-universe completeness claim. Three additional public exhibits were retrieved in a bounded audit; twelve additional complete originals, including existing retained covers and companyfacts, are saved under `tests/fixtures/sec_tabular_reporting` with URL/hash/size manifest.

Local YQ core preparation yields current ratio 559,829,000 / 316,129,000 = 1.7708878337640648. Its public panel retains two quarterly CNY rows, including Q2 revenue90,092,000. WB retains two quarterly USD rows, including Q2 revenue453,826,000. Missing annual/TTM, unqualified parent earnings and per-share values remain empty. NTB source normalization succeeds but currently lacks supported canonical ratios/revenue panels; this is not a public financial recovery. Existing cache writes, TTL and read paths are reused; repeated reads do not refetch. Table proofs are checked before cached use and originals are recomputed before projection.

The complete retained49-source replay preserves all nine previously supported sources and60metrics exactly, including periods, currency and source evidence. YQ adds one metric and two quarterly rows. The replay uses no network or production writes;17 held sources encounter an original not retained in this offline audit, so it does not establish their acquisition status. Total local CPU47.859seconds versus the earlier narrower24.234second replay is not sustained production capacity proof. Artifact: `artifacts/document-set-cohort-oct17/replay-tabular-integrated.json`.

Initial integrated validation passes75Linux checks (22new plus53reader) and22Windows checks. A final broader run adds explicit source/score/watchlist recovery, repeat, real crossing and rollback checks plus adjacent native, inline, core, cache, provider-consumer and ticker readers. It is running at this checkpoint, not yet a pass. The Linux process was observed waiting on mounted-file I/O and was preserved, not restarted; the first run subsequently completed normally.

PR718 remains draft, with this integration local until final validation. PR716/717 remain undeployed; the protected portfolio refresh was genuinely live834/1135 with zero failures in receipt1792216494. No production source, schedule, resource or subscription changes. Remaining source coverage, annual/TTM comparability, full consumer/scheduled acceptance and final Massive/zero-FMP/billing gates still apply.


## Final local integration review, 2026-10-18 03:21 UTC

The broader Linux Python 3.12 run completed with264passes and one wall-clock-dependent legacy ticker test failure. The exact previous ticker-financial implementation from a5976245 reproduces the same forward-P/E failure; changing only its observation date to the June1fixture date makes it pass. The test now fixes that date and a separate October17case verifies an expired September estimate cannot supply forward P/E. Production estimate freshness and expected financial values are unchanged. Evidence: artifacts/check-financial-test-clock-oct18.json and tabular-integrated-oct17-junit.xml.

Review additionally binds public cached table evidence to the requested symbol and core evidence to the source CIK; reassigning a valid source proof to a different company is rejected. Accounting/source-period holds now explain those verification needs rather than reporting every failure as a currency error. Holds remain cached and do not refetch on repeat. Final Linux checks pass31table/cache cases and14ticker-financial cases, including the two new proof/hold cases and expired-forecast case. These overlap earlier suites and are not a full-application test claim. JUnit receipts: tabular-review-oct18-junit.xml and ticker-financial-clock-oct18-junit.xml.

PR716 and717 have now merged as fb2aea2e and fa19d886 and deployed through successful workflow39092986587. Exact backend/frontend/workflow parity with tested fe8cb300 was checked before dispatch. Installed native verification matches23hashes and three complete source sets/18metrics, periods, currencies and no-fetch repeats; installed precision verification matches seven hashes/twooriginals and preserves genuine-conflict and selection-ambiguity holds. Runtime Python3.12.15 uses5.020CPU seconds and0.706CPU seconds respectively. These isolated checks perform no production-record writes or provider requests and do not prove sustained capacity. Artifacts native-financial-runtime-1792293468.json and core-precision-runtime-1792293473.json.

The native verifier is deliberately repinned from PR716/e230a888 to the combined tested PR717/fe8cb300 so its inline-parser hash includes the reviewed precision repair, not the dirty HTML integration. Production configuration receipt1792293478 confirms fa19d886, one shared2CPU/2GB API, FMPenabled, institutionsfalse, paidFinnhubunset and IPOfinnhub. Protected refresh receipt1792293197 confirms no unfinished/live/continuing work and latest portfolio request succeeded before release.

Public spot receipt1792293649 returns Feed/Insights/AAPL/NVDA200 in0.306-0.572seconds without server-error markers. Ordinary public ADSE/ALC financial reads returnloading; NAMI remains an unexpired prior unavailable cache. Native ordinary receipt1792293483 remains pre-release/unavailable with scheduled deadlines; this is not ordinary recovery. No forced refresh, cache reset, schedule/resource/source selection or video action. PR718 integration remains undeployed pending its exact commit/CI/runtime and ordinary/public acceptance. All broad coverage, sustained capacity, remaining consumers, finalMassive and zero-FMP/billing gates remain.
