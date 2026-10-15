#!/usr/bin/env bash
# Build and deploy only after reserving against the installed scheduler.
set -euo pipefail

owner="${GITHUB_RUN_ID:?}-${GITHUB_RUN_ATTEMPT:?}"
sha="${GITHUB_SHA:?}"
if [[ ! "$owner" =~ ^[0-9]{1,20}-[0-9]{1,6}$ ]] || [[ ! "$sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo 'Invalid release identity' >&2
  exit 2
fi
app='congress-tracker-api'
machine='8d1099c53ed378'
image_label="release-${sha}-${owner}"
release_completed=0

cleanup() {
  result=$?
  trap - EXIT
  if [ "$release_completed" -eq 1 ]; then
    if ! timeout --signal=TERM --kill-after=10s 45s flyctl ssh console -a "$app" --machine "$machine" \
      -C "python /app/scripts/scheduled_jobs.py release-reservation --owner $owner"; then
      echo '::warning::Release completed but reservation cleanup failed; its bounded expiry remains in effect.'
      if [ "$result" -eq 0 ]; then result=1; fi
    fi
  fi
  exit "$result"
}
trap cleanup EXIT

# A slow build must not prevent portfolio work or consume the reservation.
flyctl deploy --app "$app" --remote-only --build-only --push --image-label "$image_label"

# A failed/uncertain reserve refuses deployment. The reservation and a job claim
# are serialized on the host, rather than relying on an earlier empty snapshot.
timeout --signal=TERM --kill-after=10s 60s flyctl ssh console -a "$app" --machine "$machine" \
  -C "python /app/scripts/scheduled_jobs.py reserve-release --owner $owner --seconds 900"

# Bound rollout well below the hold duration. On failure leave the reservation
# to expire, since terminating this client may not stop an in-flight Fly action.
timeout --signal=TERM --kill-after=15s 330s flyctl deploy --app "$app" \
  --image "registry.fly.io/${app}:${image_label}" --strategy rolling --ha=false \
  --wait-timeout 180s --release-command-timeout 120s
release_completed=1
flyctl status --app "$app"
