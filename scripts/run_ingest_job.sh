#!/usr/bin/env bash
set -euo pipefail

job="${1:-${INGEST_JOB:-core}}"
timeout_seconds="${INGEST_JOB_TIMEOUT_SECONDS:-900}"

if [ "$job" = "daily-repair" ]; then
  timeout_seconds="${DAILY_REPAIR_JOB_TIMEOUT_SECONDS:-${DAILY_REPAIR_MAX_SECONDS:-300}}"
fi

started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "ingest_job_start job=${job} started_at=${started_at} timeout_seconds=${timeout_seconds}"

set +e
if command -v timeout >/dev/null 2>&1; then
  timeout "${timeout_seconds}s" python -m app.ingest_run --job "$job"
  status=$?
else
  python -m app.ingest_run --job "$job"
  status=$?
fi
set -e

finished_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
if [ "$status" -eq 124 ]; then
  echo "ingest_job_timeout job=${job} timeout_seconds=${timeout_seconds} finished_at=${finished_at}"
else
  echo "ingest_job_end job=${job} status=${status} finished_at=${finished_at}"
fi

exit "$status"
