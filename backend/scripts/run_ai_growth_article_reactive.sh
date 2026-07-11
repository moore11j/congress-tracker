#!/usr/bin/env sh
set -eu

if [ "${AI_GROWTH_ARTICLE_AUTOMATION_ENABLED:-false}" != "true" ]; then
  echo "AI Growth article-reactive automation disabled; set AI_GROWTH_ARTICLE_AUTOMATION_ENABLED=true to run."
  exit 0
fi

cd /app
python -m app.jobs.run_ai_growth_article_reactive
