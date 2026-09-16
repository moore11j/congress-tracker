#!/bin/sh
set -eu
# One bounded process, independent of slow screener/confirmation calculations.
exec flock -n /tmp/walnut-watchlist-price-alerts.lock timeout 240 python -m app.jobs.run_watchlist_price_alerts
