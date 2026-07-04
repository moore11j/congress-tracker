#!/bin/sh
set -eu

case "${FMP_BACKGROUND_REFRESH_ENABLED:-true}" in
  0|false|FALSE|no|NO|off|OFF)
    echo "data_enrichment_queue_disabled processed=0 succeeded=0 failed=0 skipped=1 reason=background_refresh_disabled"
    exit 0
    ;;
esac

case "${ENRICHMENT_QUEUE_ENABLED:-false}" in
  1|true|TRUE|yes|YES|on|ON)
    ;;
  *)
    echo "data_enrichment_queue_disabled processed=0 succeeded=0 failed=0 skipped=1 reason=enrichment_queue_disabled"
    exit 0
    ;;
esac

case "${DATA_ENRICHMENT_QUEUE_ENABLED:-false}" in
  0|false|FALSE|no|NO|off|OFF)
    echo "data_enrichment_queue_disabled processed=0 succeeded=0 failed=0 skipped=1 reason=queue_disabled"
    exit 0
    ;;
esac

batch_size="${DATA_ENRICHMENT_QUEUE_BATCH_SIZE:-10}"
max_seconds="${DATA_ENRICHMENT_QUEUE_MAX_SECONDS:-20}"

case "$batch_size" in
  ""|*[!0-9]*)
    echo "data_enrichment_queue_invalid_batch_size value=$batch_size processed=0 succeeded=0 failed=0 skipped=1"
    exit 64
    ;;
esac

case "$max_seconds" in
  ""|*[!0-9]*)
    echo "data_enrichment_queue_invalid_max_seconds value=$max_seconds processed=0 succeeded=0 failed=0 skipped=1"
    exit 64
    ;;
esac

if [ "$batch_size" -lt 1 ]; then
  echo "data_enrichment_queue_invalid_batch_size value=$batch_size processed=0 succeeded=0 failed=0 skipped=1"
  exit 64
fi

if [ "$max_seconds" -lt 1 ]; then
  echo "data_enrichment_queue_invalid_max_seconds value=$max_seconds processed=0 succeeded=0 failed=0 skipped=1"
  exit 64
fi

if ! python -m app.background_job_guard --job enrichment-queue; then
  echo "data_enrichment_queue_skipped processed=0 succeeded=0 failed=0 skipped=1 reason=db_pressure_guard"
  exit 0
fi

lock_dir="${DATA_ENRICHMENT_QUEUE_LOCK_DIR:-/tmp/data_enrichment_queue.lock}"
if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "data_enrichment_queue_skipped processed=0 succeeded=0 failed=0 skipped=1 reason=worker_already_running"
  exit 0
fi

output_file="$(mktemp "${TMPDIR:-/tmp}/data_enrichment_queue.XXXXXX")"
cleanup() {
  rm -f "$output_file"
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "data_enrichment_queue_start batch_size=$batch_size max_seconds=$max_seconds"

hard_timeout=$((max_seconds + 15))
set +e
DATA_ENRICHMENT_QUEUE_BATCH_SIZE="$batch_size" \
DATA_ENRICHMENT_QUEUE_MAX_SECONDS="$max_seconds" \
  timeout "$hard_timeout" python -m app.ingest_run --job enrichment-queue > "$output_file"
status=$?
set -e

output="$(cat "$output_file")"

if [ "$status" -ne 0 ]; then
  echo "data_enrichment_queue_failed status=$status processed=0 succeeded=0 failed=1 skipped=0 output=$output"
  exit "$status"
fi

counts="$(
  printf '%s\n' "$output" | python -c 'import json, sys; data = json.loads(sys.stdin.read() or "{}"); print("processed=%s succeeded=%s failed=%s skipped=%s" % (data.get("processed", 0), data.get("succeeded", 0), data.get("failed", 0), data.get("skipped", 0)))'
)"

echo "data_enrichment_queue_finished $counts"
