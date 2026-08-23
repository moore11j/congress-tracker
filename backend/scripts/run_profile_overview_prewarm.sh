#!/bin/sh
set -e
set -u

case "${PROFILE_OVERVIEW_PREWARM_ENABLED:-false}" in
  1|true|TRUE|yes|YES|on|ON)
    ;;
  *)
    echo "profile_overview_prewarm_disabled status=skipped reason=env_disabled"
    exit 0
    ;;
esac

max_seconds="${PROFILE_OVERVIEW_PREWARM_MAX_SECONDS:-900}"
case "$max_seconds" in
  ""|*[!0-9]*)
    echo "profile_overview_prewarm_invalid_max_seconds value=$max_seconds status=failed"
    exit 64
    ;;
esac

if [ "$max_seconds" -lt 60 ]; then
  echo "profile_overview_prewarm_invalid_max_seconds value=$max_seconds status=failed"
  exit 64
fi

if ! python -m app.background_job_guard --job profile-overview-prewarm; then
  echo "profile_overview_prewarm_skipped status=skipped reason=db_pressure_guard"
  exit 0
fi

lock_dir="${PROFILE_OVERVIEW_PREWARM_LOCK_DIR:-/tmp/profile_overview_prewarm.lock}"
if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "profile_overview_prewarm_skipped status=skipped reason=worker_already_running"
  exit 0
fi

cleanup() {
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "profile_overview_prewarm_start max_seconds=$max_seconds"
timeout "$max_seconds" python -m app.ingest_run --job profile-overview-prewarm
