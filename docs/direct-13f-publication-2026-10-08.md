# Direct 13F publication and digest rehearsal

Later October 8 checkpoint: the [guarded worker and expanded real-pair evidence](direct-13f-worker-2026-10-08.md) supersede Samson's mapping hold and the implementation-only-in-memory limitation below. The job is implemented locally but remains disabled and undeployed; real qualifying public alerts and broader coverage remain open.

October 8, 2026. Continued the approved FMP replacement work while Massive Stocks Starter activation remains pending. This stage implements and exercises an **in-memory publication adapter**. No production database, subscription, scheduled job or provider flag changed; no customer email was sent.

## Follow-up: actual quarter pairs and independent evidence

The owner's subsequent “continue” authorized the next validation stage. The [bounded collector](../backend/scripts/collect_direct_13f_pairs.py) successfully fetched four public SEC submissions histories and four Q2 submissions, reusing four previously saved Q3 submissions. The eight complete originals contain 43 source rows. A second run with network disabled reproduces the same source identities and hashes. Initial sandbox socket access failed; the read-only elevated requests succeeded without an approval rejection.

The [real-pair runner](../backend/scripts/rehearse_direct_13f_pairs.py) checks the saved sources and histories, loads the copied public-record baseline into memory, and blocks network/mail during publication. It independently compares source share totals by CUSIP before asking the canonical pipeline to derive changes:

| Manager | Actual pair result |
|---|---|
| Tetrad | All three equity identifiers resolved from dated SEC N-PORT evidence. Q2/Q3 share counts are identical. Canonical processing writes three `unchanged` records and three summaries, with zero activity/feed events despite changes in reported dollar values. |
| Stephens Group | Four source share counts are unchanged. Canonical derived publication remains held for incomplete ticker mapping. |
| Samson Rock | Source share comparison finds a WBD reduction and three reported exits. Incomplete prior-quarter ticker mapping blocks canonical derived publication; these are not published alerts. |
| Lakehouse | Q3 is held before insertion. Eight CUSIPs have implied values about 990–1,000 times below independently reported same-quarter SEC holdings. No automatic unit conversion or public changes. Q2 is retained as historical evidence; its dollar-value plausibility is not established by this sample. |

Seven filings/31 holdings are inserted locally. Four historical filings generate no alerts; one current pair is processed, two await mappings and one is held for value discrepancies. Repeating all eight submissions preserves the complete canonical state. Receipt: `artifacts/direct-feeds/13f-pairs-2026-10-08/pair-rehearsal.json`, state SHA-256 `b363b0092ed846792a0548cdec58cc633b0235df651cce604dbd720bcba02473`. No delivery rows or production writes. This proves a real unchanged-share case, not successful live delivery or a complete real changed-position alert case.

### Free identifier source

[SEC N-PORT](https://www.sec.gov/Archives/edgar/data/1592900/000159290026002759/xslFormNPORT-P_X01/primary_doc.xml) explicitly links CUSIPs and tickers within each investment record. The new [evidence parser](../backend/app/services/direct_13f_evidence.py) accepts exact common-equity/share records from checksummed original submissions with matching header/XML/URL identities. It rejects derivative/invalid identifiers, preserves source rows and dates, and excludes evidence filed after the target 13F. Conflicting symbols remain unresolved. It is a bounded mapping source, not a complete ticker directory or universal security classifier.

Tetrad's EPD mapping comes from the June 1 filing above; BRK-B and USB come from the June 29 [Professionally Managed Portfolios filing](https://www.sec.gov/Archives/edgar/data/811030/000119312526287933/xslFormNPORT-P_X01/primary_doc.xml). These dates precede its July 6 Q2 filing. A March 26 [N-PORT filing](https://www.sec.gov/Archives/edgar/data/1650149/000089418926009017/xslFormNPORT-P_X01/primary_doc.xml) supplies WBD for the incomplete Samson pair. Three documents provide 112 parsed identifier observations before conflict/CUSIP selection. A fourth exploratory N-PORT download was not used in the rehearsal.

### Value validation boundary

The same evidence module compares complete original 13Fs by quarter and CUSIP, excluding options/principal amounts, the same manager and later filing dates. A hold needs at least two independent managers agreeing within 5% of the median, and a discrepancy of at least 100-fold. It does not transform the source amounts. The pair runner explicitly supplies the 35 saved daily-batch filings as comparison evidence; callers without comparison documents have **not** performed this check. Absence of a hold is not proof that a filing's values are correct or that every security has peers.

This supersedes the earlier Lakehouse acceptance in the broad 30-filing rehearsal below. That earlier run checked source-table consistency, which cannot detect a source consistently reporting the wrong unit. No earlier production import occurred. Real changed-position alert validation, wider security mapping, amendment reconciliation and scheduled production observation remain open.

Follow-up verification: **174 tests pass across nine files** on Python 3.14.2, including all 27 publication/evidence checks, SEC collection, institutional snapshot/correction regressions, actual email builders, prepared daily/weekly Top Stocks previews and price-reference checks. Existing framework deprecation warnings remain. The prior unrelated institution-profile assertion was not part of this nine-file run and remains unresolved. Python syntax, whitespace and local links pass. No FMP requests, customer messages, production writes, deployment or subscription actions occurred.

## Canonical holdings and changes

The [adapter](../backend/app/services/direct_13f_publication.py) reparses each checksummed SEC submission, requires a complete original information table and reconciles its aggregated shares and dollar values with the resulting canonical positions. Identical source lots accumulate instead of disappearing through the legacy row deduplicator. Options retain their classification and do not become ordinary equity changes. Portfolio weights use the complete reported table; issuer ownership percentages are not invented.

Existing exact accessions are reconciled, not inserted again. Conflicting holder/quarter accessions, orphan positions/changes/activity/feed records, amendments, confidential omissions, principal amounts and conflicting security classes remain held. Holder identity checks recognize zero-padded CIK variants. Each new filing and its downstream processing use one savepoint; a derived-builder failure rolls back the filing and positions.

New ownership changes require exactly one complete, verified prior quarter, unchanged position fingerprints, compatible CIK/symbol mappings and a prior filing date no later than the current filing. An explicit filing-date publication boundary keeps historical imports from becoming fresh alerts. A first observed filing cannot establish a new position or exit by itself. An out-of-order prior filing can unlock the current filing once; changing the prior evidence after publication requires reconciliation.

Verified pairs use the existing change/summary/activity/event pipeline. New holder events carry both SEC accessions, source URLs and source hashes, retaining filing-day event timing and stable canonical delivery identities. Any new cluster event identifies the verified pair as its triggering holder only; other cluster constituents are not thereby verified. Existing legacy filings without the new completeness receipt are left unchanged, including their derived pipeline state.

## Saved SEC corpus

The [offline runner](../backend/scripts/rehearse_direct_13f_publication.py) loads the saved read-only public-record baseline and 38 saved 13F submissions: 35 from the October 6 daily batch and three additional populated-comparison samples. Source files are checksummed, staging databases open read-only, canonical writes are confined to SQLite memory, and network/email sends are blocked.

| Result | Count |
|---|---:|
| Newly imported filings | 30 |
| Canonical aggregated positions | 4,210 |
| Existing matching filing left unchanged | 1 |
| Held filings | 6 |
| Rejected inconsistent cover/table | 1 |
| New changes, summaries, activity/feed events | 0 |
| Repeat inserts or other canonical state changes | 0 |
| Email deliveries / production writes | 0 |

All 30 accepted filings wait for a complete prior quarter absent from this copied baseline. Zero new events is the expected safety result, **not proof of real filing-pair or refreshed-ranking coverage**. The holds comprise two amendments, three filings containing principal amounts and one existing filing requiring source reconciliation. The rejected cover/table discrepancy and existing value discrepancy were already identified in the October 7 work; this adapter does not silently repair them.

Final receipt: `artifacts/direct-feeds/13f-publication-2026-10-08/final-receipt.json` (local artifact; initial receipt retained). Baseline SHA-256: `a0a35f8427b6330a412d750e3e6254723244360e09b91c5b7df3e18470ca96be`. Complete post-import state SHA-256: `77bfdede084825650fe832e201052ef554e0c6eb1e73c087c1221808e1af7b3d`; the repeat state is identical, including original row IDs and timestamps. The final replay includes filing-identity/date fingerprints and reproduces the same counts. These tracked counts preserve the finding if the local artifact is unavailable.

## Monitoring and email correctness

The real digest builders exposed two presentation defects for canonical 13F events. A reported exit became a generic “Sell,” while watchlist formatting shifted a date-only October 6 filing to October 5 in Pacific time. The [digest renderer](../backend/app/services/email_digests.py) now uses explicit 13F reported-change labels and the filing date. Amounts are labelled reported holdings (prior holdings for an exit), never execution proceeds, and execution price stays unavailable. Missing and zero values do not fall back to legacy amounts or prices. These changes apply to events explicitly marked as 13F reported holdings, including existing canonical events; unrelated activity retains its behavior.

The [synthetic complete-pair checks](../backend/tests/test_direct_13f_publication.py) exercise actual increase/new/exit calculations, eligible public events, monitoring copies and monitoring/daily/watchlist digest builders. Pro access is retained; Premium cannot receive institutional alerts. Replay preserves event/alert identities and complete rendered digest contents. Network calls and delivery sends fail the tests if attempted. Daily/weekly Top Stocks and price-reference regressions remain separate checks with prepared inputs, not refreshed replacement-source coverage.

## Remaining release gates

Validation used Python 3.14.2, not production's Python 3.12. The 12-file regression run passed 271 checks, failed one existing institutional-profile assertion and encountered one Windows pytest temporary-directory permission error. That temporary-file check passed when rerun under a new workspace artifact directory. The final adapter/digest file passes all **18 tests**, including four cases added after the combined run and the final filing-identity/date fingerprint guard. This gives 276 distinct passing checks across the runs; it is not a full-suite pass. Coverage includes institutional canonical processing, direct feeds, monitoring/digests, prepared daily/weekly Top Stocks and price-reference safety.

The remaining failure, `test_institution_profile_endpoints_are_locked_until_pro`, expects a locked profile's public holder name to be null but receives `Blue Ridge Capital`. It reproduces in isolation. Its router, institutional service, entitlements and test file match HEAD `b7989115`; the new adapter is not imported by that isolated check. No access-policy change was made to resolve this unrelated assertion. Existing framework deprecation warnings remain. Diff, Python syntax and local report links were also checked.

Validate real complete prior/current SEC pairs, missing/ambiguous security mappings and amendments; reconcile existing production records and source-backed alert copies; then exercise production-equivalent scheduling and no-send previews. Existing institutional cluster constituents, refreshed scores/universe and actual multi-day source reliability remain unverified by this adapter.

Massive Starter activation and live delayed snapshots, residual price callers, direct Congress collection, issuer/news/calendar coverage and FMP data disposition remain broader migration gates. FMP stays active until those retained-product requirements are demonstrated. No deployment, production repair, email, account declaration, purchase or cancellation occurred in this stage.
