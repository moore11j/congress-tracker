# Two-service runtime, October 10

Owner authorizes one performance database and one combined API/background host, manual videos, staggered imports and lower database load. June invoice target was about $26; latest reported invoice $61.94. Neither is the same as a full-month current-machine estimate.

## Measured production changes

PR529 / 52c0f9f8 deployed successfully via workflow 38080227573. Nine checked backend/schedule files match on each of four workers (36 hashes). Database machine 81721da9770131 changed from two shared CPUs/1 GB to one performance CPU/2 GB. The same encrypted 20 GB volume remains; Fly reports all three health checks passing. Rollback size is recorded locally.

Post-change public samples: readiness 0.316s, minimal 10-item feed 0.394s, NVDA profile 0.742s; before the change after the query fix they were 0.583s, 2.576s and 8.622s. Later NVDA/AAPL context bundles returned 200 in 1.842s/1.709s with current generation timestamps and populated identities. Samples are not full latency or completeness validation.

New automatic video job creation is disabled and read back false. A bounded query finds zero jobs in runnable, active or budget-retry states. Existing drafts are retained. Video host stopped after confirming zero runnable jobs; the final release subsequently removed it.

## Runtime design

Supercronic keeps the existing source-controlled schedules and timezone rules. Its commands enqueue lightweight requests into a local SQLite queue on /data. Repeated triggers coalesce, preserving the oldest wait and one additional pending request during execution. One data worker runs heavy imports/preparation; one delivery worker serves monitoring, watchlist and email duties. Each job keeps the existing command and flags, runs with the cron connection-pool limits and reduced CPU priority. Queue ordering prevents repeated frequent jobs from jumping ahead of older work.

An OS lane lock and SQLite transaction prevent competing workers from claiming the same lane. Job process groups and unique environment tokens support bounded shutdown and recovery without killing unrelated reused PIDs. Interrupted calls are recorded, not blindly replayed; a separately queued later schedule remains pending. Existing domain cursors, alert identities, consent and delivery idempotency remain authoritative. A 20-minute outer job limit bounds runaway work; application-specific lower budgets remain in force. Queue delay and exit codes are logged and inspectable.

The supervisor stops the service if an essential child exits so a healthy API cannot conceal a dead scheduler. Only the selected API machine may schedule work. Initial deployment leaves combined scheduling disabled so it cannot overlap the legacy cron host. Final consolidation requires draining legacy cron, enabling the selected API, observing jobs and public latency, then removing redundant process groups/machines. Keeping the API always on is required once it hosts the scheduler.

## Checks and release gates

Five Windows unit checks passed (Linux checks skipped). Nine isolated checks passed on actual Linux/Python 3.12.15, including native subprocess serialization, independent deliveries, recovery and supervisor-failure cleanup. Dummy commands use temporary files only, no product data/provider calls/emails. The generated full schedule preserves every command/calendar; Fly configuration validates. Docker is unavailable locally, so Linux validation used the idle video host with temporary files, not a production schedule.

Still required: exact release verification, graceful cron handoff, queue/backlog observation, consumer freshness/alert checks and reduced machine inventory/cost verification. Existing dependency audits fail in unchanged packages. This package does not claim full FMP cutover or a $26 final bill.

## Cost target

Current verified Toronto compute/RAM estimates: combined shared 2 CPU/2 GB host $14.93, performance 1 CPU/2 GB database $36.81: $51.74 per 30 days, plus storage/network/backups/taxes. Manual external Kling/Suno production needs no dedicated Fly video host. Lower database sizing may be reconsidered only after serialized work and query/cache repairs establish sustained performance. No eventual cost saving is asserted before the redundant machines are retired.


## Final handoff configuration

The final configuration retains only the app process group, enables its pinned scheduler, disables the video runtime/browser build, keeps the app always on, and sets a60-second graceful shutdown window. Deployment explicitly disables spare-machine creation. Existing cron must be drained before dispatching this release. The redundant second API is removed by its exact ID after the selected scheduler host is verified. Volumes remain retained for rollback until contents/snapshot retention are reviewed. Nineteen local schedule/queue checks pass (three Linux-only skips before the ninth Linux-only descendant-cleanup test was added); all nine Linux checks were executed separately.


## Verified cutover, October 10, 20:02 UTC

PR530/b17a3c1b deployed preparation with scheduling disabled. PR531/9fb664d3 deployed successfully through workflow38081800090 after the old cron host drained. Runtime script, schedule and feed-router hashes match. Combined scheduling is enabled only on8d1099c53ed378. Old cron807d42ce974018 and video87ed10c364e078 were removed by deployment; spare API863254be261935 was gracefully stopped, checked, then destroyed. All volumes remain retained; no customer data or drafts deleted. Video generation is manual/external, with no new Kling/Suno subscription or API integration claim.

Initial spare retirement was blocked by automatic approval review because logs showed503s/slow checkouts and nonzero job exits. Investigation proved those particular503s are explicit public_context_cache_miss responses on cached-only reads, with the existing frontend SEO fallback. Nonzero warm_sec_research and warm_institutional_reference receipts explicitly reported partial source coverage, not a crashed scheduler. They remain coverage gaps. New selected-host HTTP checks and queue progress resolved the operational concern; the reviewed retry succeeded.

Selected-host loopback checks: readiness0.021s,20-item feed0.016s, NVDA profile0.257s, cached AAPL/NVDA contexts0.101/0.105s. With the spare stopped, six public requests all200: readiness0.393s, minimal feed0.401s,20-item feed0.579s, NVDA profile0.504s, AAPL/NVDA contexts0.583/0.433s. These are samples and cached responses, not sustained peak-load or complete-data proof. Earlier newly generated contexts took1.7–2.0s.

Ten scheduled definitions observed; nine latest receipts exit0 and SEC research retains its explicit partial-coverage exit1. Insights, metadata, news, fundamentals preparation, research schedule checks and strategy delivery-queue work advance. Pending/running queues drained; available memory1497MB, load0.27 at20:01:44. No manual test emails. Saturday observations do not verify Monday intraday alerts or Friday weekly digest delivery; calendar/command preservation and existing domain deduplication remain intact. Observe the next due windows before claiming all consumers validated.

Current target compute/RAM is51.74USD/30days. Inventoried volumes total46GB,6.90USD/month at0.15/GB: active API5GB+DB20GB=3.75; retired API/cron10GB plus rehearsal11GB=3.15 retained. Compute plus these volumes totals58.64 before snapshot/rootfs/network/other usage/tax/credits. This is not a promised invoice and excludes existing data/creative subscriptions. It is3.30 below the reported61.94 invoice before those extras, not a37.33 saving against that invoice. The37.33 compute reduction compares identical continuous-runtime estimates89.07 and51.74. Historical bills remain unreconciled.

Further load work:16 invalid indexes remain across search/events/insiders/institutions/contracts, discovered read-only and not rebuilt during this handoff. Preserve a single serial maintenance lane and measure benefit before index maintenance. Smaller shared database sizing could be reconsidered after sustained evidence; no downgrade approved/applied here. Full ticker cache/source coverage and the broader FMP migration remain open; Massive last and no FMP cancellation.
