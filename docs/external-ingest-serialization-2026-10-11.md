# External ingestion serialization, October 11, 2026

## Measured issue

The combined host serializes its crontab data jobs, but .github/workflows/daily_ingest.yml still launches app.ingest_run directly through SSH. Its GitHub concurrency group only serializes GitHub runs, not server jobs. At20:23UTC, a12.32second process sample observed a core ingestion process running at nice0 without WALNUT_JOB_TOKEN alongside a scheduled institutional reference preparer at nice10. The core process had consumed61.78CPU seconds since starting. GitHub run38171594613 was then in progress and later completed successfully; the next previously pending ingestion run38171932760 started. No live process was killed or restarted.

API burst reserve fell from56.86CPU seconds at20:17 to approximately0.02 at20:22, when five-minute throttling reached0.6515CPU seconds/second. These observations establish a serialization defect and exhausted capacity, not that this one job accounts for all CPU use. See the separate Senate outage report for the source reliability finding from the same audit.

## Repair

Add fixed external-ingest job definitions for the nine existing workflow choices to the current data lane. A durable SQLite external request identity binds GitHub run ID and attempt to a specific queued execution. Retries after ambiguous SSH failures reuse that receipt rather than starting another import. Requests arriving during an active execution retain the next pending run; several pending requests may coalesce. Completion and exit status are committed atomically with the job result, so queued acceptance is never reported as successful execution and later runs cannot overwrite an earlier receipt. Wrong completion tokens and conflicting request identities fail closed.

The command refuses dispatch on a non-owner machine or without the existing queue. All product commands and data-lane scheduling/priority remain unchanged. The delivery lane remains independent. No new trigger, provider, resource allocation or customer message is introduced.

Release in two steps: deploy and verify the compatible backend helper first, then change the existing GitHub workflow to submit and wait through that helper using the same request ID on transport retry. Existing active ingestion must finish before restarting its host. Until the workflow change is verified, external ingestion is still unqueued.

## Validation status

Local Python3.14.2:10 scheduler checks pass; five Linux-only checks are skipped locally. Added cases cover lost-response retry before/after completion, next-run coalescing, immutable success/failure receipts, simultaneous connections, cron serialization/delivery independence, fixed-command validation and wrong-token handling. Exact-candidate Linux fixture execution passes: a barrier holds a cron data job while delivery completes, external ingestion remains queued until the barrier releases, then executes at nice10. Retrying its request after completion leaves runs=1 and no pending job. Source SHA256 ce3afbadd776c6923c7b475969082b5f87aa02a751a387f29c8c5fdf5eaa6291; receipt external-queue-linux-1791750634.json. No application imports, production database, provider calls or emails; fixture workers are terminated and the temporary directory is removed. Initial helper newline quoting and Windows command-length errors were corrected before this final barrier check. Both pre-existing GitHub imports38171594613/38171932760 are now terminal/success. No production deployment or workflow activation yet.
