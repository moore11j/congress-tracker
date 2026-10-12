# Institutional bounded PostgreSQL writes, October 12, 2026

Production remains on FMP for institutions, with direct publication disabled. This local rehearsal extends the deployed preparation-only measurement in [the production preparation report](institutional-production-preparation-2026-10-12.md). It does not prove production capacity or authorize cutover.

## Actual initial batch

The current managed application at production release `70bd3639` was exercised against portable PostgreSQL 16.15 on loopback port 61983, in a new disposable schema. Python is 3.14.2 locally; production uses 3.12.15. Captured public source evidence only was restored. HTTP requests and email sends were forbidden. No production database writes occurred.

The actual `publish_13f_batch` selector and writer ran with `limit=20`, `include_waiting=True` and staged references. All 718 captured parsed originals and 579 saved reference documents were staged. The batch selected the first 20 original targets, prepared the complete relevant September comparison group of 402 filings, and used 38 relevant references. No comparison subset was substituted for the source inventory.

Result: 19 waiting for complete prior-quarter evidence, one held, 19 inserted filings and 3,587 inserted positions, zero events or emails. The batch took **33.422 process CPU seconds / 46.360 elapsed seconds**. SQL statements included 596 SELECT, 77 INSERT and 38 UPDATE operations. Measured cursor execution totaled approximately 4.495 seconds for SELECT, 1.074 for INSERT and 0.044 for UPDATE; these figures exclude commit/network overhead outside cursor execution and are not server CPU measurements.

Working set was 208.49 MB before the batch and 233.23 MB after. The 707.72 MB process lifetime peak includes fixture restoration and must not be presented as batch-only peak memory. The concurrently running full SQLite replay also means elapsed time is not an isolated throughput benchmark.

Receipt: `artifacts/direct-feeds/free-replacements-2026-10-09/institutional-bounded-postgres-oct12.json`. The disposable schema was removed after success. The Windows sandbox could not start PostgreSQL because restricted-token creation failed; the same existing loopback-only instance started through a scoped escalation. No installation or service configuration changed.

## Positive publication measurement

The initial batch creates canonical holdings but no public events. A separate local scenario measures event-producing publication and an identical repeat. It selects up to 20 previously publishable current-quarter filings by descending position count, prepares their actual prior-quarter originals, and retains the full peer inventory and production byte bounds. This selection is explicitly a stress scenario, not the ordinary initial production queue. Actual publication, source verification and event logic remain unchanged. It is not a worst-case bound for all possible future filings.

First positive-scenario handle `13418` finished the batch but failed in the reporting helper before saving its result: the helper had not supplied its `backend` path to the generated code namespace. The disposable schema was removed. No acceptance is claimed from that failed run. The helper now passes the path and saves a truthful first-pass receipt before repeat checks. Corrected handle `11647` completed successfully, with final receipt `institutional-ready-batch-postgres-oct12.json`.

All 20 filings published: 3,490 positions, 3,557 changes and summary updates, and three public events with retained SEC proof. Six relevant saved references and the complete 402-filing September peer group were used. The initial batch took **82.172 CPU / 94.684 elapsed seconds**. It issued **30,108 SELECT**, 1,619 INSERT and 2,803 UPDATE statements; cursor time was 17.382 / 3.017 / 1.172 seconds respectively. Working set rose from 377.06 to 429.56 MB, while the 707.56 MB lifetime peak again includes restoration.

The repeated batch took 38.297 CPU / 40.573 elapsed seconds. All 20 results were existing, with zero additional filings, positions or feed events. The fingerprint over every application table is identical: `9b8964cb09b10ca120d811f3a37961875fee2df6c1954780ae9a0dba4b538d73`. Four measured publisher/source file hashes are retained in the receipt. No provider calls, production writes or emails occurred. Every disposable schema was removed and the local PostgreSQL instance was stopped; startup handle `75294` is terminal.

This is successful local PostgreSQL publication and repeat evidence, not production throughput. The measured query volume makes reducing publication query overhead the next performance task. Code inspection identifies per-symbol summary/event refreshes as a candidate; their individual cost is not yet profiled, and no application optimization has been implemented in this turn.

## Broader replay and ordinary collection

At 01:43 UTC the separate full replay handle `46444` remains live, now past its first pass. The saved first-pass receipt contains 574 document results, 120 complete current comparisons, and 119 generated/declared events. Every declared event retains its SEC provenance. Event types are 70 smart-money confirmations, 24 accumulation clusters, 24 distribution clusters and one holder exit across 88 symbols. Unresolved mappings, prior coverage, security transitions and distribution treatment remain held or waiting. Full-state repeat and consumer checks remain pending; first-pass evidence is not final acceptance.

The read-only production checkpoint `reference-queue-1791769270.json`, observed 01:41:10 UTC, confirms ordinary version-5 reference collection continues within two requests per run. Its three most recent runs stage six verified scopes without price requests, public writes or email sends. The newly verified `76009N100:2026-09-30` scope is one of the historical-alias cases addressed by PR595, needed by five captured filings. This verifies collection, not publication of those dependent filings. Publication flag remains false.

No provider selection, recurring schedule, hardware, subscription, landing-page change, customer email or FMP cancellation occurred. Remaining gates include the full institutional repeat/consumers, positive writer capacity and source coverage, Senate access, core financial definitions and coverage, transcripts, all alerts/rankings, final Massive entitlement and consumer checks, zero global FMP traffic and billing cancellation.

## Fresh financial preparation gate

The bounded read-only audit `paired-free-coverage-1791769607.json`, observed 01:46:46 UTC, applies the current deployed pure combination rules to the 1,751 attempted scopes and their retained FMP baseline. It currently accepts 126 SEC ratio preparations and combines 44 with current Finnhub metrics. Another 34 have verified issuer evidence without usable SEC ratios. There are 1,266 SEC identity/version rejections, 281 expired SEC caches and three absent SEC caches. These are current-version readiness counts, not raw provider coverage or numeric parity. Unsupported instruments and invalid historical input symbols exist in this attempted universe; it is not 1,751 verified eligible issuers.

Compact metadata independently verifies the version transition at 01:48:27 UTC: 1,543 SEC v4 records, one XOM v4 variant, three v1 records and 201 v5 records; Finnhub has 1,407 v1 and 52 v2 records. The last three ordinary SEC runs each finish ten scopes, with zero due Finnhub scopes and no quota stop. Code inspection shows broad SEC scopes rotate by oldest attempt; Finnhub scopes become due after 24 hours and preserve the actual cached observation time. Its earliest remaining v1 observation is October11 01:51 UTC, so the next ordinary due window remains to be observed. Receipt `financial-versions-1791769707.json`.

This invalidates using the older 1,225-combined-scope audit as present readiness after version changes. It does not establish loss of provider coverage or a public regression: public core financial selection remains FMP. No refresh budget, schedule or source selection was changed. Finish ordinary current-version rotation and then numerical/semantic and all-consumer checks; do not bypass version validation to make coverage look complete.

The next read-only receipt `financial-versions-1791769919.json` at 01:51:59 UTC verifies another ten ordinary SEC updates, raising v5 records to 211. Finnhub remains at 52 v2 records: the job began at 01:51:01, before its earliest old observation became 24 hours old at 01:51:16. The later due run remains unobserved, not failed. No manual refresh was triggered.
