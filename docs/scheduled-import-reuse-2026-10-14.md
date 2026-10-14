# Scheduled import reuse, October 14, 2026

## Problem and scope

The combined API/data host exhausted its CPU allowance again on production dd89afda. October 14 14:58 UTC probes returned Feed 4.969s, AAPL 18.831s, NVDA 19.512s and Insights 4.893s with its research fallback. The database was mostly idle. A bounded reference diagnostic spent 2.7101 CPU seconds on imports versus 0.1410 on its cached planner. These samples do not quantify aggregate hourly savings.

The repair preloads model definitions, direct-source model definitions, requests and mapper configuration once in the single-threaded data scheduler. Six exact argument-free warmers (institutional reference, Finnhub news, SEC research, earnings, metadata and fundamentals) execute in fresh forked children. No task executes in the parent. Other commands and the delivery lane retain their subprocess path. Schedules, batch limits, canonical identities, source ownership, alert behavior and host sizes are unchanged.

Each child establishes its own process group, closes inherited queue/lock handles, resets its database pool and uses its own PID in PostgreSQL application_name. A two-pipe handshake prevents execution before durable boot/PID/starttime registration. Startup timeout, process identity verification, worker recovery, environment restoration and task isolation protect interrupted starts. Configuration SCHEDULED_JOB_PRELOAD_ENABLED=1 selects the path; 0 restores subprocess launching. Threaded/changed-environment/unsupported configurations fall back. Import failures disable the template. A child launch failure is never replayed automatically.

## Verification before release

- Linux Python 3.12.3: 33 scheduler/fork tests pass, including actual Supercronic v0.2.34 supervisor failure, lane locking, child state isolation, parent death before acknowledgment, startup timeout, descriptor cleanup, PID reuse and ordinary subprocess descendant recovery. Initial Linux run exposed inaccessible unrelated /proc environments; corrected and covered. Windows preliminary checks had 18 passes/10 platform skips before additional Linux cases.
- All 86 affected data preparation checks pass normally and all 86 pass again in a forked child after actual template preparation. Fixtures exercise source caches, leases, repeated data, holds and unchanged publication. No provider calls or production data writes.
- Import-only benchmark: three runs of each six modules. Total CPU 57.171838s cold versus 5.2783225s including the one-time 2.3377525s template, a 90.77% reduction for this measured component. Exact schema hashes match for each module in both modes (137 or 138 tables depending on module). This is not a whole-job or site-latency saving. Maximum combined parent/child PSS sample is 89,825 KiB versus 81,069 KiB cold. Idle template retention and production memory still require observation.
- Bounded read-only production diagnostic on Python 3.12.15: template CPU 1.526991s; two different child/backend PID pairs, correct child application_name and read-only transactions. Parent holds zero connections and its empty pool is unchanged. No data jobs, source requests, production writes or configuration changes. Candidate normalized SHA256 509fa9a8e2c83d942509823712c3903eab6155efa19abf0e04dce4cd959fe87f.

Evidence: artifacts/preload-benchmark-oct14/report.json, artifacts/preload-postgres-readonly-1791991730.json; reproducible benchmark and read-only diagnostic scripts in the primary workspace artifacts directory. Production libraries relevant to the probe include SQLAlchemy 2.0.36 and psycopg 3.2.3. Local benchmarking uses WSL Python 3.12.3 rather than claiming exact production microversion parity.

## Release state and remaining gates

Prepared October 14 15:25 UTC; not yet merged/deployed. Production remains dd89afda. Rollback is the launcher flag set to 0 and normal backend deployment. The change must show ordinary completed jobs with preloaded=true, retained delivery operation, bounded memory and sustained latency/CPU recovery before claiming capacity resolved. Webull PR630 is separate and held. Financial coverage, institutions, transcripts, all monitoring/alert/ranking consumers, final Massive activation, global zero FMP traffic and billing cancellation remain open. FMP stays enabled.
