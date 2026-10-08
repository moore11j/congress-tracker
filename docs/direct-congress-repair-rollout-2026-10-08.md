# Guarded Congress repair rollout

October 8, 2026. Work under the active owner-authorized FMP shutdown goal. FMP remains selected; no subscription or source cutover is claimed.

## Prepared repair and history labels

The production entry point defaults to read-only inspection and requires an explicit environment flag and apply argument to write. Application requires PostgreSQL, the exact captured source and directory hashes, reviewed canonical/job population hashes, and the expected paused source generation. A shared feed lock and immediate table locks protect inserts as well as updates; contention aborts. The transaction archives duplicate records, retires only verified queued P&L jobs, preserves surviving IDs and saved portfolio history, rechecks strict source reconciliation and releases the staging hold only on success. It never resumes a feed or sends email.

Saved portfolio chart markers now retain source status and display a notice when a recorded position refers to a withdrawn duplicate. Recorded returns, execution dates and positions remain unchanged; the archive reader and queue ownership guards were already deployed in 29c354c5 and 2cea0aae.

## PostgreSQL proof and lookup correction

The live read-only preview reached its 20-second query limit three times without writes. EXPLAIN showed a scan of approximately 338,837 events occupying 933 MB. The final lookup uses two additive expression indexes for transaction references and effective source-document basenames. Candidate lookup does not replace the strict official URL/member/transaction checks. Fixed SQL literals keep generic prepared plans compatible with the indexes. Concurrent index creation is an explicit operational command, with bounded timeouts and invalid-index detection.

Eight PostgreSQL cases pass with the exact final repair source SHA 028bd7aa1727f6aaffea8aed124a63998f8a1e46e85a810cc37b540d603838b2: success/repeat, stale population, unpaused feed, wrong generation, running job, injected archive failure, changed source, and an escaped transaction key on an event assigned to another member. Tests use only session-private temporary tables, pg_temp-only name resolution, captured public fixtures and unconditional outer rollback. Every test table is verified temporary and removed afterward; zero public data reads/writes or customer rows. Successful repair preserves all 50 captured portfolio positions and five runs, withdraws ten events/thirteen transactions, and retires five queued jobs. This is isolated validation, not a production repair.

Primary backend: 78 focused checks pass on Python 3.14.2, with two existing FastAPI warnings. The same 78 checks also pass against the exact release checkout. Frontend: 21 pass and one reproduced baseline member-tab assertion fails; TypeScript and the production build pass. No full-suite claim.

## Actual scheduled SEC observation

Run 8 starts from the real hourly schedule at 21:47:03 UTC and finishes at 21:50:55: 344 documents processed, 187 parsed, 157 quarantined, seven additional 13F failures, zero public writes. The configured seven-day discovery window exposes 3,674 older Form 4 documents still pending. This is broader than the completed October 7-only batch. A 21:56 read-only receipt verifies all 762 retained revision hashes, zero source selections and zero publication receipts. All publishers remain disabled. The backlog and source holds remain explicit gates.

## Delivery and next step

At preparation: code is local to the isolated release checkout; indexes and canonical repair have not been applied. Deploy the guarded path and history labels, create/verify lookup indexes, obtain a complete fresh read-only plan, then perform the authorized source-bound repair only after its guards pass. Congress scan coverage, canonical reconciliation/publication, fresh replacement rankings and all no-send digest checks remain open. Massive snapshots still returned 403 at 21:37 UTC; Starter activation and FMP account sign-in remain pending.

Evidence is saved locally under artifacts/direct-feeds/congress-repair-rollout-2026-10-08, including query plans, isolated PostgreSQL receipts and the actual hourly SEC receipt. This tracked report retains the durable conclusions.
