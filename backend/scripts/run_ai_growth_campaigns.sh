#!/usr/bin/env sh
set -eu

lock_dir="${AI_GROWTH_CAMPAIGN_LOCK_DIR:-/tmp/ai_growth_campaigns.lock}"
max_seconds="${AI_GROWTH_CAMPAIGN_MAX_SECONDS:-900}"

if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "ai_growth_campaigns_already_running"
  exit 0
fi

cleanup() {
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

cd /app
if command -v timeout >/dev/null 2>&1; then
  timeout "$max_seconds" python -m app.jobs.run_ai_growth_campaigns
else
  python -m app.jobs.run_ai_growth_campaigns
fi
