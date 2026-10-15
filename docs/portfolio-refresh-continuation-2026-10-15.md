# Portfolio refresh continuation, October 15, 2026

## Problem and release state

Originally implemented on `codex/portfolio-refresh-continuation`, based on8241a907 (PR652). Now deployed as c4eaa0f0 (PR653); the production receipt below supersedes local-only state. The previous daily refresh occupied the data lane for1201.57seconds before timing out, leaving three-year coverage incomplete and other imports waiting. See [the measured incident](portfolio-refresh-load-2026-10-14.md). Financial precision PR651 remains held separately.

## Behavior

The existing exact portfolio-refresh command now runs a persisted plan through the existing serialized scheduler. The plan freezes the original ordered member selection, configured lookbacks, benchmark, replace policy, methodology and UTC reporting date. It stores progress in the existing queue database with an additive migration and token/cycle ownership checks. A hash protects the immutable plan; oversized or invalid plans fail instead of being truncated.

Each batch uses the existing portfolio computation, price preflight, quality rules and persistence path. It yields after eight scopes, two CPU seconds or eight wall seconds, checked between scopes. These are soft targets: one expensive scope can exceed them and retains the existing1200second process timeout. Schema setup occurs once when planning, not on every batch. The existing import template supports only the exact reviewed command and still creates an isolated child with a fresh database pool.

News and other data jobs can run between batches. A yielded batch does not increment the logical run count or complete an external request. A new trigger during an unfinished cycle is retained for the next cycle. Separate cron/external portfolio cycles and the legacy all-job cannot interleave portfolio reporting windows. Repeating the same external request ID does not enqueue duplicate work.

A completed batch can resume after a scheduler restart with the same plan and a new execution token. An interrupted active transaction remains a visible failure; it is not blindly replayed. Guard holds defer work for60seconds; a24hour plan deadline fails visibly. Failed scopes remain failed in the final receipt even when later scopes succeed. The completed/attempted/planned counts and methodology/date are explicit.

No cron or GitHub trigger times, host sizes, subscriptions, public response fields, portfolio methodology, provider selection, alert opt-ins or delivery lanes change. The GitHub caller still waits30minutes and reports pending/non-success if the logical cycle exceeds that wait. This timeout is not evidence that the durable cycle has completed or stopped; ordinary full-cycle duration and external receipt completion remain release validation gates. No workflow timeout has been increased or pending work called successful.

## Validation

Linux Python3.12.3:216 affected tests pass, one test skipped because Supercronic is not installed locally. After the final schema-once/receipt/doc adjustment, all28 continuation/preload checks pass. One obsolete workflow assertion was updated to match the already deployed October11 serialized request-ID dispatch; the workflow itself is unchanged.

Coverage includes legacy queue migration, ownership rejection, restart between batches, a retained later trigger, idempotent external request retries, honest partial/failed completion, plan corruption, deadline holds, cross-cycle exclusion, unchanged import isolation and a real forked worker. In the latter, news finishes between portfolio batches and all ten scopes across two members/five lookbacks finish under one logical run.

An actual public-member capture (93events,37annual holdings,16,634prices since2022) was replayed into isolated SQLite. All five persisted runs,1,220points and289positions are exactly equal before and after batching, excluding database-generated IDs/timestamps. The reporting date remains October14 across midnight, and a completed repeat does no work. Full normalized state SHA256:`c0382bcfcbac01b6df2fe2c29216f26b46c71340fbf83f5cd50ca2a186b1de51`. Network connections are prohibited; no production writes or emails. This bounded sample does not establish the full cohort, PostgreSQL capacity or sustained production performance.

Local evidence: `artifacts/portfolio-continuation-tests-oct14.log`, `portfolio-continuation-final-tests-oct15.log`, `portfolio-continuation-parity-oct15.json` and corresponding replay script. Durable conclusions are recorded here because these artifacts are ignored.

## Remaining checks

Review/release and exact runtime verification, ordinary full portfolio completion and external-request receipt, all lookback coverage, queued-source freshness and sustained API capacity remain open. FMP stays enabled; financial definitions/coverage, institutions, transcripts, all ranking/monitoring/watchlist/daily/weekly consumers, final Massive subscription/validation, zero FMP traffic and billing/access dates remain required before shutdown.

The owner's screenshot shows a separate Codex remote429. On this recovery check ordinary usage is allowed and reported weekly consumption is88%. The session works, but the historical throttle subtype and permanent service repair are not established. No account/model/retry configuration was changed.


## Production release verified, 00:19 UTC

[PR653](https://github.com/moore11j/congress-tracker/pull/653), source1abd35da84bd4c5de330aa8986a553f3801d57a6, merged/deployed as c4eaa0f053006347458a4d6726f0aaf8c1bd3547. Workflow38659123042/job116175913210 finished successfully. One job-log fetch returned502; same-run terminal success was verified without redispatch. Nine deployed source/schedule hashes match. The existing two machines and sizes are unchanged.

An isolated fixture on deployed Python3.12.15 verifies five completed scopes across midnight, four intervening news claims, one logical completion, a no-work completed repeat and reopening the actual queue between batches. The fixture uses temporary/in-memory SQLite with network prohibited, zero production writes and zero emails. The production queue separately contains all three additive columns, retains earlier failure receipts and is empty at00:19:23. No full portfolio job was manually rerun; the next ordinary GitHub trigger remains00:45UTC, subject to GitHub scheduling/dispatch. Full ordinary cohort duration/completion, external receipt and three-year coverage remain unverified.

Public samples after restart: Insights0.660seconds, research present/no unavailable fallback; Feed0.735, news0.344/12items, research1.191/44cards; AAPL4.840 and NVDA3.792seconds. All return200. Ticker latency is still unsatisfactory and these samples do not establish sustained capacity. The probe's obsolete hard-coded release-string field is not used as release evidence.

Ordinary00:19 news finishes successfully with23/23 scopes and42 fresh company caches. Institutional/reference and research jobs finish with explicit partial/unavailable coverage receipts; their nonzero exits are not evidence of a new Python execution crash. The institutional universe is950 with489 pending/461 deferred scopes, still truncated. No institutional publication or source switch.

Secret scan passes. Exact CI logs confirm unchanged frontend production audit five vulnerability groups (four high/one critical) and Pillow10.4.0 with33 advisory rows; manifests unchanged. This is not an all-CI pass. PR attachment hit the app's100-item limit; the ordinary link remains usable.

Rollback must preserve the queue database and external request identities. At this verified empty-queue checkpoint there are no continuations to strand. Before any later return to an older scheduler image, recheck that no active or continuing portfolio cycle exists. If one exists, drain it with this release or prepare an explicitly reviewed failure/receipt reconciliation; an older scheduler does not understand continuation_at. Do not delete the queue, mark unfinished requests successful, or blindly replay an interrupted active transaction.

Evidence: primary artifacts/portfolio-continuation-runtime-1792023538.json, portfolio-cycle-1792023563.json, preload-receipts-1792023584.json and direct-feeds/free-replacements-2026-10-09/{insights-incident-1792023536.json,scheduled-job-cpu-1792023560.json}. All current local handles are terminal. Precision PR651 remains held. FMP and all remaining global cutover gates remain active.

## Interrupted ordinary run diagnosed, October15 06:40UTC

The approved minimized read-only aggregate receipt artifacts/portfolio-failure-category-1792046017.json establishes one terminated request, zero application-failure/deadline/unaccepted-yield receipts,38 attempted of1135 planned scopes and zero recorded failed scopes. No current active/continuing request remains. This exports only counts/booleans, no checkpoint payloads or member/provider records. The earlier full scheduler export remains unrun.

The required CI diagnostic tool then confirms Scheduled Ingest38685216877/job116258395722 selected portfolio-simulation-refresh at05:09:05UTC and ended with exit-15 at05:09:42 while the FUSB backend deployment38685205187 was in progress (05:08:49–05:10:03). The SSH transport retry reused the same request identity and correctly reported failure; it did not duplicate the run. The failure was a deployment interruption, not successful full-cycle validation.

After verifying the queue has no unfinished portfolio request and current deployment is terminal, one fresh normal workflow38693528033 was explicitly dispatched for portfolio-simulation-refresh. This retains the failed receipt and starts a new cycle through the fixed serialized command. Normal persisted-run replacement deletes/replaces matching portfolio outputs in the same transaction; it does not resume an uncertain checkpoint or send customer emails. Existing bounded price repair may still use FMP, so this is a scheduled-consumer validation rather than zero-FMP evidence.

Backend deployments are held until this fresh run is authoritatively terminal. Workflow caller completion is not enough if the durable continuation remains unfinished. Full scope completion, per-lookback coverage, site latency and source freshness remain unverified until actual receipts. Permanent coordination between deployment and durable portfolio cycles remains a reliability follow-up; a manual hold during this observation is not an implemented automatic safeguard.