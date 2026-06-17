#!/bin/sh
set -eu

case "${FEED_PNL_REPAIR_ENABLED:-true}" in
  0|false|FALSE|no|NO|off|OFF)
    echo "feed_pnl_repair_disabled events_scanned=0 events_missing_pnl=0 reason=repair_disabled"
    exit 0
    ;;
esac

days="${FEED_PNL_REPAIR_DAYS:-3}"
limit="${FEED_PNL_REPAIR_LIMIT:-500}"
max_seconds="${FEED_PNL_REPAIR_MAX_SECONDS:-60}"

case "$days" in
  ""|*[!0-9]*)
    echo "feed_pnl_repair_invalid_days value=$days events_scanned=0 events_missing_pnl=0"
    exit 64
    ;;
esac

case "$limit" in
  ""|*[!0-9]*)
    echo "feed_pnl_repair_invalid_limit value=$limit events_scanned=0 events_missing_pnl=0"
    exit 64
    ;;
esac

case "$max_seconds" in
  ""|*[!0-9]*)
    echo "feed_pnl_repair_invalid_max_seconds value=$max_seconds events_scanned=0 events_missing_pnl=0"
    exit 64
    ;;
esac

if [ "$days" -lt 1 ] || [ "$limit" -lt 1 ] || [ "$max_seconds" -lt 1 ]; then
  echo "feed_pnl_repair_invalid_bounds days=$days limit=$limit max_seconds=$max_seconds events_scanned=0 events_missing_pnl=0"
  exit 64
fi

lock_dir="${FEED_PNL_REPAIR_LOCK_DIR:-/tmp/feed_pnl_repair.lock}"
if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "feed_pnl_repair_skipped events_scanned=0 events_missing_pnl=0 reason=repair_already_running"
  exit 0
fi

cleanup() {
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "feed_pnl_repair_start days=$days limit=$limit max_seconds=$max_seconds"
timeout "$max_seconds" python -m app.repair_recent_feed_pnl --days "$days" --limit "$limit"
