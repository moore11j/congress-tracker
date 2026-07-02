#!/bin/sh
set -eu

case "${INSTITUTIONAL_LATEST_JOB_ENABLED:-false}" in
  1|true|TRUE|yes|YES|on|ON)
    ;;
  *)
    echo "institutional_latest_job_disabled status=paused reason=env_disabled"
    exit 0
    ;;
esac

max_seconds="${INSTITUTIONAL_LATEST_JOB_MAX_SECONDS:-2700}"
case "$max_seconds" in
  ""|*[!0-9]*)
    echo "institutional_latest_job_invalid_max_seconds value=$max_seconds status=failed"
    exit 64
    ;;
esac

if [ "$max_seconds" -lt 60 ]; then
  echo "institutional_latest_job_invalid_max_seconds value=$max_seconds status=failed"
  exit 64
fi

lock_dir="${INSTITUTIONAL_LATEST_JOB_LOCK_DIR:-/tmp/institutional_latest_job.lock}"
if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "institutional_latest_job_skipped status=skipped_locked reason=worker_already_running"
  exit 0
fi

cleanup() {
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "institutional_latest_job_start max_seconds=$max_seconds"
timeout "$max_seconds" python -m app.ingest_institutional_activity --job-run-once --require-job-enabled
