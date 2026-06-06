#!/bin/sh
set -eu

lookback_minutes="${EMAIL_ALERT_SWEEP_LOOKBACK_MINUTES:-60}"
limit="${EMAIL_ALERT_SWEEP_LIMIT:-100}"

case "$lookback_minutes" in
  ""|*[!0-9]*)
    echo "email_intraday_alert_invalid_lookback_minutes value=$lookback_minutes"
    exit 64
    ;;
esac

case "$limit" in
  ""|*[!0-9]*)
    echo "email_intraday_alert_invalid_limit value=$limit"
    exit 64
    ;;
esac

set -- python -m app.jobs.send_intraday_email_alerts --lookback-minutes "$lookback_minutes" --limit "$limit"

case "${EMAIL_ALERT_SCHEDULE_DRY_RUN:-true}" in
  0|false|FALSE|no|NO|off|OFF) ;;
  *) set -- "$@" --dry-run ;;
esac

exec "$@"
