#!/bin/sh
set -eu

kind="${1:-}"

case "$kind" in
  monitoring|watchlist_activity|signals) ;;
  *)
    echo "email_digest_schedule_invalid_kind kind=$kind"
    exit 64
    ;;
esac

if [ "${EMAIL_DIGEST_SCHEDULE_ENABLED:-0}" != "1" ]; then
  echo "email_digest_schedule_disabled kind=$kind"
  exit 0
fi

lookback_days="${EMAIL_DIGEST_SCHEDULE_LOOKBACK_DAYS:-1}"
limit="${EMAIL_DIGEST_SCHEDULE_LIMIT:-100}"

case "$lookback_days" in
  ""|*[!0-9]*)
    echo "email_digest_schedule_invalid_lookback_days value=$lookback_days"
    exit 64
    ;;
esac

case "$limit" in
  ""|*[!0-9]*)
    echo "email_digest_schedule_invalid_limit value=$limit"
    exit 64
    ;;
esac

set -- python -m app.jobs.send_email_digests --kind "$kind" --lookback-days "$lookback_days" --limit "$limit"

if [ "${EMAIL_DIGEST_SCHEDULE_DRY_RUN:-0}" = "1" ]; then
  set -- "$@" --dry-run
fi

exec "$@"
