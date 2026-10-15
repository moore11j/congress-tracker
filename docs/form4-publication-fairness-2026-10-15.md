# Form 4 publication backlog, October 15, 2026

## Confirmed production defect

Read-only production metadata at 22:37:58 UTC shows 200 parsed, eligible Form 4 filings with held receipts and 552 eligible parsed filings without any publication attempt. Fixed document-ID ordering selects all 200 held filings and zero new filings. The held population was discovered October 8-10; unattempted sources were discovered October 10-14. The most recent successful publication receipt was October 11. Receipt: artifacts/publication-backlog-1792103878.json. This confirms starvation, not simply source transport failure.

The 200 holds comprise 181 unmapped existing-event cases, 17 unnormalized provider candidates and two cross-accession owner/date cases. These are reconciliation safeguards, not permission to insert duplicate events. House has six eligible held records and no eligible unattempted backlog in this observation; its partial result is not evidence of the same active starvation incident. Two additional House holds are currently outside parsed eligibility.

Independent source metadata at 22:34:39 verifies 48 successful hourly Senate backup collections through 22:17, each with 135 filings/2,176 rows. Snapshot source clocks rolled forward; latest source check is 21:12:41 UTC. This extends ordinary freshness evidence, not proof of new filings or complete historical coverage. Latest watchlist, monitoring, intraday and top-ideas scheduler runs exit zero; this alone does not establish recipient eligibility, source quality or delivery. Receipt: artifacts/source-schedule-metadata-1792103679.json.

## Repair

Retry-enabled Form 4 batches select the least recently attempted source, using discovery time for unattempted sources and document ID as a stable tie-break. The limit remains 200 and the existing hourly schedule is unchanged. Holds rotate while new sources can reach reconciliation. Retry-disabled selection retains its original behavior.

An immutable-source mismatch on an existing held receipt now advances only its attempt timestamp. Source hashes, metadata identity, original report, generation and publication boundary are preserved. Normal attempts update their receipt after projection to preserve the clean-session requirement with production autoflush disabled. Per-filing locks, transactions, completed-day/publication-boundary filters, source validation, canonical deduplication and arrival dates remain unchanged. No old transaction is represented as newly disclosed on its trade date.

Read-only PostgreSQL comparison at 22:40:24 verifies the repaired ordering selects 200 unattempted sources and zero previously held sources for the next batch, versus the old 200-held/zero-new selection. No production records were changed by this query. Receipt: artifacts/publication-backlog-1792104024.json. Later metadata confirms the latest combined SEC collection has two errors; their exact cause remains unclassified, separately from the publisher defect (receipt1792104072).

## Validation and release state

Before repair, three new regression checks fail and one passes: a full held batch prevents a valid new source, single-slot retries repeatedly select the same hold, and changed-source holds never rotate. After repair, 114 selected tests pass on Linux Python 3.12.3, covering publication, alert parity, scheduling, non-market display and availability dates. Code review then identified a production autoflush compatibility issue before release; the timestamp change was restricted to the early-return mismatch branch, and a recovered-hold regression was added. The final focused 38 pass. These runs cover 115 distinct checks, not a full-suite pass. An initial command named two nonexistent test files and ran no tests; the corrected run uses actual existing files.

The runtime proof rehearses successfully on Windows Python 3.14.2 using four source hashes, actual publisher functions, isolated in-memory SQLite with autoflush disabled, 200 held synthetic sources, a valid fixture, repeat checks, and blocked network. It verifies four scenarios, zero customer emails and zero production record access. Helper: artifacts/verify-form4-fairness-runtime-oct15.py.

Local repair at this checkpoint. Exact reviewed release, installed-runtime verification and ordinary scheduled backlog progress remain required. Processing the backlog does not mean every source will publish: real reconciliation holds must be resolved separately. All core-financial, institutional, transcript, scheduled-consumer, sustained-performance, final Massive, actual zero-FMP and billing gates remain. FMP stays enabled; no new provider selection, subscription, hardware or video activity.
