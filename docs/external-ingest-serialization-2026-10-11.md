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


## Helper deployed; workflow routing prepared

PR579 merged3036bead271e156eba4199e3e3cc7d77b2801592 and backend workflow38172701161 succeeds. Automated code review was unavailable because the review account quota was exhausted; no independent automated review is claimed. Secret scan and Vercel preview passed, while existing dependency audits failed in unchanged dependency files. Scope was manually reviewed and focused/isolated runtime checks passed.

At20:35UTC, receipt two-service-verify-1791750948.json verifies20 runtime hashes, the external request schema, the same two approved host sizes and an empty queue. LF-normalized scheduler SHA256 is7fad9a3dabc4efa631bba98ba3a5008f7289882a3887e8b1ac927dc7418e14ce (the fixture records its Windows-file byte hash separately). Six public requests return200: feeds0.45/0.57s and fresh AAPL/NVDA contexts1.26/1.13s. Institutional publication remains false. These are post-restart samples, not sustained capacity proof.

The workflow candidate preserves all eight schedule expressions, nine manual choices and concurrency configuration. It calls dispatch-ingest with GitHub run ID plus attempt, using the identical identity on all transport retries, and waits for actual completion. Nineteen local Bash fixture cases pass: each schedule/manual mode, a simulated failed SSH response followed by retry, and rejection of an unknown schedule. Fly is stubbed, so no source/production request is made by those tests. An unavailable local YAML package was avoided by exact unchanged trigger-block comparison and executable Bash fixtures; no dependency was installed.

Workflow routing is not yet activated at this checkpoint. Next merge it, validate the actual GitHub-to-queue completion path with the already source-gated recent-congress job, then observe continuing capacity. Source reliability, including the Senate403 outage, remains separately gated.
