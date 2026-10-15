# Portfolio refresh continuation, October 15, 2026

## Problem and release state

Implemented locally on `codex/portfolio-refresh-continuation`, based on deployed8241a907 (PR652). Not deployed at this checkpoint. The previous daily refresh occupied the data lane for1201.57seconds before timing out, leaving three-year coverage incomplete and other imports waiting. See [the measured incident](portfolio-refresh-load-2026-10-14.md). Financial precision PR651 remains held separately.

## Behavior

The existing exact portfolio-refresh command now runs a persisted plan through the existing serialized scheduler. The plan freezes the original ordered member selection, configured lookbacks, benchmark, replace policy, methodology and UTC reporting date. It stores progress in the existing queue database with an additive migration and token/cycle ownership checks. A hash protects the immutable plan; oversized or invalid plans fail instead of being truncated.

Each batch uses the existing portfolio computation, price preflight, quality rules and persistence path. It yields after eight scopes, two CPU seconds or eight wall seconds, checked between scopes. These are soft targets: one expensive scope can exceed them and retains the existing1200second process timeout. Schema setup occurs once when planning, not on every batch. The existing import template supports only the exact reviewed command and still creates an isolated child with a fresh database pool.

News and other data jobs can run between batches. A yielded batch does not increment the logical run count or complete an external request. A new trigger during an unfinished cycle is retained for the next cycle. Separate cron/external portfolio cycles and the legacy all-job cannot interleave portfolio reporting windows. Repeating the same external request ID does not enqueue duplicate work.

A completed batch can resume after a scheduler restart with the same plan and a new execution token. An interrupted active transaction remains a visible failure; it is not blindly replayed. Guard holds defer work for60seconds; a24hour plan deadline fails visibly. Failed scopes remain failed in the final receipt even when later scopes succeed. The completed/attempted/planned counts and methodology/date are explicit.

No cron or GitHub trigger times, host sizes, subscriptions, public response fields, portfolio methodology, provider selection, alert opt-ins or delivery lanes change. The GitHub caller still waits30minutes and reports pending/non-success if the logical cycle exceeds that wait. This timeout is not evidence that the durable cycle has completed or stopped; ordinary full-cycle duration and external receipt completion remain release validation gates. No workflow timeout has been increased or pending work called successful.

## Validation

Linux Python3.12.3:216 affected tests pass, one platform-specific test skipped. After the final schema-once/receipt/doc adjustment, all28 continuation/preload checks pass. One obsolete workflow assertion was updated to match the already deployed October11 serialized request-ID dispatch; the workflow itself is unchanged.

Coverage includes legacy queue migration, ownership rejection, restart between batches, a retained later trigger, idempotent external request retries, honest partial/failed completion, plan corruption, deadline holds, cross-cycle exclusion, unchanged import isolation and a real forked worker. In the latter, news finishes between portfolio batches and all ten scopes across two members/five lookbacks finish under one logical run.

An actual public-member capture (93events,37annual holdings,16,634prices since2022) was replayed into isolated SQLite. All five persisted runs,1,220points and289positions are exactly equal before and after batching, excluding database-generated IDs/timestamps. The reporting date remains October14 across midnight, and a completed repeat does no work. Full normalized state SHA256:`c0382bcfcbac01b6df2fe2c29216f26b46c71340fbf83f5cd50ca2a186b1de51`. Network connections are prohibited; no production writes or emails. This bounded sample does not establish the full cohort, PostgreSQL capacity or sustained production performance.

Local evidence: `artifacts/portfolio-continuation-tests-oct14.log`, `portfolio-continuation-final-tests-oct15.log`, `portfolio-continuation-parity-oct15.json` and corresponding replay script. Durable conclusions are recorded here because these artifacts are ignored.

## Remaining checks

Review/release and exact runtime verification, ordinary full portfolio completion and external-request receipt, all lookback coverage, queued-source freshness and sustained API capacity remain open. FMP stays enabled; financial definitions/coverage, institutions, transcripts, all ranking/monitoring/watchlist/daily/weekly consumers, final Massive subscription/validation, zero FMP traffic and billing/access dates remain required before shutdown.

The owner's screenshot shows a separate Codex remote429. On this recovery check ordinary usage is allowed and reported weekly consumption is88%. The session works, but the historical throttle subtype and permanent service repair are not established. No account/model/retry configuration was changed.
