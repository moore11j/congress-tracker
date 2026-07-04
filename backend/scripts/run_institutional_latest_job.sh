#!/bin/sh
set -eu

case "${INSTITUTIONAL_SCHEDULED_INGEST_ENABLED:-false}" in
  1|true|TRUE|yes|YES|on|ON)
    ;;
  *)
    echo "institutional_latest_job_disabled status=paused reason=env_disabled"
    exit 0
    ;;
esac

max_seconds="${INSTITUTIONAL_SCHEDULED_INGEST_MAX_SECONDS:-900}"
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

if ! python -m app.ingest_institutional_activity --scheduled-latest-enabled-check --log-level INFO; then
  echo "institutional_latest_job_disabled status=paused reason=durable_or_env_disabled"
  exit 0
fi

if ! python -m app.background_job_guard --job institutional-latest; then
  echo "institutional_latest_job_skipped status=skipped reason=db_pressure_guard"
  exit 0
fi

lock_dir="${INSTITUTIONAL_SCHEDULED_INGEST_LOCK_DIR:-/tmp/institutional_latest_job.lock}"
lock_pid_file="$lock_dir/pid"
if ! mkdir "$lock_dir" 2>/dev/null; then
  lock_pid=""
  if [ -f "$lock_pid_file" ]; then
    lock_pid="$(cat "$lock_pid_file" 2>/dev/null || true)"
  fi
  if [ -n "$lock_pid" ] && [ -r "/proc/$lock_pid/cmdline" ]; then
    if tr '\000' ' ' < "/proc/$lock_pid/cmdline" | grep -q "app.ingest_institutional_activity"; then
      echo "institutional_latest_job_skipped status=skipped_locked reason=worker_already_running pid=$lock_pid"
      exit 0
    fi
  fi
  rm -f "$lock_pid_file" 2>/dev/null || true
  if ! rmdir "$lock_dir" 2>/dev/null; then
    echo "institutional_latest_job_skipped status=skipped_locked reason=stale_lock_not_empty"
    exit 0
  fi
  if ! mkdir "$lock_dir" 2>/dev/null; then
    echo "institutional_latest_job_skipped status=skipped_locked reason=worker_already_running"
    exit 0
  fi
  echo "institutional_latest_job_stale_lock_recovered status=recovered"
fi
echo "$$" > "$lock_pid_file"

cleanup() {
  rm -f "$lock_pid_file" 2>/dev/null || true
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "institutional_latest_job_start max_seconds=$max_seconds"
timeout "$max_seconds" python -m app.ingest_institutional_activity --scheduled-latest-once --log-level INFO
