# Two-service runtime, October 10

Owner authorizes one performance database and one combined API/background host, manual videos, staggered imports and lower database load. June invoice target was about $26; latest reported invoice $61.94. Neither is the same as a full-month current-machine estimate.

## Measured production changes

PR529 / 52c0f9f8 deployed successfully via workflow 38080227573. Nine checked backend/schedule files match on each of four workers (36 hashes). Database machine 81721da9770131 changed from two shared CPUs/1 GB to one performance CPU/2 GB. The same encrypted 20 GB volume remains; Fly reports all three health checks passing. Rollback size is recorded locally.

Post-change public samples: readiness 0.316s, minimal 10-item feed 0.394s, NVDA profile 0.742s; before the change after the query fix they were 0.583s, 2.576s and 8.622s. Later NVDA/AAPL context bundles returned 200 in 1.842s/1.709s with current generation timestamps and populated identities. Samples are not full latency or completeness validation.

New automatic video job creation is disabled and read back false. A bounded query finds zero jobs in runnable, active or budget-retry states. Existing drafts are retained. No video machine is stopped yet.

## Runtime design

Supercronic keeps the existing source-controlled schedules and timezone rules. Its commands enqueue lightweight requests into a local SQLite queue on /data. Repeated triggers coalesce, preserving the oldest wait and one additional pending request during execution. One data worker runs heavy imports/preparation; one delivery worker serves monitoring, watchlist and email duties. Each job keeps the existing command and flags, runs with the cron connection-pool limits and reduced CPU priority. Queue ordering prevents repeated frequent jobs from jumping ahead of older work.

An OS lane lock and SQLite transaction prevent competing workers from claiming the same lane. Job process groups and unique environment tokens support bounded shutdown and recovery without killing unrelated reused PIDs. Interrupted calls are recorded, not blindly replayed; a separately queued later schedule remains pending. Existing domain cursors, alert identities, consent and delivery idempotency remain authoritative. A 20-minute outer job limit bounds runaway work; application-specific lower budgets remain in force. Queue delay and exit codes are logged and inspectable.

The supervisor stops the service if an essential child exits so a healthy API cannot conceal a dead scheduler. Only the selected API machine may schedule work. Initial deployment leaves combined scheduling disabled so it cannot overlap the legacy cron host. Final consolidation requires draining legacy cron, enabling the selected API, observing jobs and public latency, then removing redundant process groups/machines. Keeping the API always on is required once it hosts the scheduler.

## Checks and release gates

Five Windows unit checks passed (Linux checks skipped). Eight isolated checks passed on actual Linux/Python 3.12.15, including native subprocess serialization, independent deliveries, recovery and supervisor-failure cleanup. Dummy commands use temporary files only, no product data/provider calls/emails. The generated full schedule preserves every command/calendar; Fly configuration validates. Docker is unavailable locally, so Linux validation used the idle video host with temporary files, not a production schedule.

Still required: exact release verification, graceful cron handoff, queue/backlog observation, consumer freshness/alert checks and reduced machine inventory/cost verification. Existing dependency audits fail in unchanged packages. This package does not claim full FMP cutover or a $26 final bill.

## Cost target

Current verified Toronto compute/RAM estimates: combined shared 2 CPU/2 GB host $14.93, performance 1 CPU/2 GB database $36.81: $51.74 per 30 days, plus storage/network/backups/taxes. Manual external Kling/Suno production needs no dedicated Fly video host. Lower database sizing may be reconsidered only after serialized work and query/cache repairs establish sustained performance. No eventual cost saving is asserted before the redundant machines are retired.
