#!/bin/sh
set -e
set -u

if [ "${RESEARCH_CAMPAIGNS_SCHEDULE_ENABLED:-0}" != "1" ]; then
  echo "Research campaign scheduler disabled. Set RESEARCH_CAMPAIGNS_SCHEDULE_ENABLED=1 to enable."
  exit 0
fi

python -m app.jobs.run_research_campaigns --limit "${RESEARCH_CAMPAIGNS_SCHEDULE_LIMIT:-10}"
