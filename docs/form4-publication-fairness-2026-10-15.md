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

## Publisher deployed and installed proof verified, October15 22:49UTC

PR691 https://github.com/moore11j/congress-tracker/pull/691 tested a04ca1232c04a989306c034fab4016ded73eba72, merged5a3bc04d3cedfd2f9284b22389b8629b127f0b80. Merged tree equals tested tree; both dependency audits, secret scan and Vercel preview succeed. Guarded workflow38819347547/job116711826851 completes successfully, readiness/privacy pass22:47:51UTC. One diagnostics502 was followed on the same job, with no duplicate deployment. Existing unrelated submodule cleanup warning remains nonfatal. Chat attachment cap100 prevents attaching; ordinary PR link remains usable.

Installed Python3.12.15 verifies four source hashes and all four synthetic backlog/repeat cases with autoflush disabled, in-memory SQLite, blocked requests/sockets and zero production record reads/writes/emails. Receipt artifacts/form4-fairness-runtime-1792104536.json. The first verifier attempt stopped at a missing test-only httpx import; removing that unnecessary import retained network blocking and the subsequent proof passes. This is installed-code proof, not yet ordinary backlog processing. No source selector, schedule or hardware change. Unrelated TypeScript build hashA185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA preserved.

The separate SEC collection error is now identified: the October14 index includes a legitimate October13 filing, causing the old strict row-date check to reject discovery. See sec-delayed-index-2026-10-15.md. That collector repair is separate from this deployed publisher change.

## First ordinary repaired publication verified, October15 22:58UTC

The existing minute57 publisher starts22:57:00 and finishes22:57:51UTC, scheduler run123. It processes200 previously unattempted filings:96 published,104 held. It inserts243 transactions and243 events. Remaining eligible backlog declines from552 to352 unattempted; held population rises from200 to304. Thus the repaired ordering has demonstrated actual progress through the production backlog, not merely a synthetic pass.

Scheduler exit1 is the existing explicit partial-result contract because104 filings remain held, not a failed or interrupted batch. CPU6.5489seconds is the recorded scheduler measurement, not sustained-host capacity proof. Receipt artifacts/form4-schedule-1792105070.json. No manual publisher enqueue or changed schedule was used.

Consistent read-only PostgreSQL snapshot at22:58:12 verifies243 referenced events,243 distinct event IDs, zero missing events, zero incorrect insider/SEC provenance, zero mismatched source-normalized identities and zero arrivals backdated before this release. Source selection remains sec_edgar,generation2,October7boundary. Scheduler pending/started fields are empty after completion. Receipt artifacts/form4-ordinary-1792105092.json. Earlier mid-run receipt1792105057 records151 attempts/175events and is superseded by the terminal totals. These checks do not prove every historical economic record is unique; unchanged full-source deduplication safeguards and isolated repeated-publication tests provide the release-level protection.

Public rendered spot checks after completion at22:57:58 return200/no server-render errors: Feed0.303s,Insights0.417s,AAPL0.296s,NVDA0.263s. Receipt artifacts/public-render-1792105078800.json. A planned during-run check completed after the publisher, so do not call it concurrency or sustained-load proof. Prior post-second-deployment receipt1792104977358 also passes.

Remaining:352 eligible filings await their first attempt,304 source holds need genuine reconciliation, broader source completeness and alert delivery remain open. The separate delayed-index repair is deployed252899af with full543-original installed replay, but its next ordinary collection has not yet run. All wider FMP retirement gates remain. Both deployed repairs and this observed recovery are concrete progress; goal remains active. All current helper/test/deployment/observer handles are terminal. No private payload export, forced customer email, new subscription, source selector, schedule, hardware, video or landing change.
