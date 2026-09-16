# BMNR missed price alert — September 15, 2026

## Evidence and cause

Jarod's watchlist 1 had enabled immediate rule 2, "5% Price Decrease". Production rule state for BMNR was `current_result=true`, with no `last_triggered_at`. Its last evaluation at 18:20:51 UTC (11:20 AM Pacific) recorded -6.851708074534163%, using quote 23.995 and previous close 25.76. No BMNR decrease trigger or email existed. The September 15 closing bar was 23.60, a -8.385093167701864% move (rounded -8.39%). This was a failure before email delivery, not a rejected email or disabled user preference.

The evaluator suppressed initial/stale states as baselines, including daily-price rules after a two-hour gap. It could therefore persist a qualifying move as already matched without creating its trigger. Subsequent evaluations required a false-to-true transition, so the missed alert remained suppressed. The September 10 duplicate fix had not covered this opposite edge case.

The intraday price cache/evaluation also stopped updating before the last confirmation snapshot, because price rules shared the broader screener/confirmation refresh workflow. Historical logs available at audit time do not establish the exact reason every later scheduled refresh lacked a valid price observation; do not claim that they do.

The Confirmation monitor is separate: it records confirmation score and source-state transitions, not arbitrary percentage moves. BMNR's latest snapshot was bullish with score 76 and five sources; a price alert must not depend on that score flipping. Its latest confirmation event predated September 15. No artificial confirmation event was inserted during recovery.

## Fix

- Daily percentage-price rules qualify on their first matching observation, including new/stale states, gap openings, consecutive qualifying sessions, and a pre-existing matched state with no trigger. Existing durable per-rule/ticker/session and per-recipient delivery identities remain in force.
- Use exchange-local dates for price comparison and ensure a fallback closing-bar percentage displays the corresponding closing price, not a stale intraday quote.
- Add a separate five-minute price-alert job: live quote refresh, daily-price-rule evaluation, and price-only delivery. It bypasses expensive screener/confirmation calculations, preserves subscriptions/entitlements, catches unsent same-session records, and includes a short market-close grace period.
- Bound that worker with `flock` and a 240-second timeout. Respect delivery/dry-run/pause switches. Record missing observations and recipient failures explicitly.

## Verification

117 focused tests passed, including the actual BMNR prices, stale/new/already-matched states, consecutive sessions, UTC-midnight handling, confirmation-worker isolation, schedule coverage, subscription routing, and three repeated records producing exactly one provider send. One pre-existing requests dependency warning remained.

The release was built from tracked HEAD `29aa9601` plus only the scoped alert changes. Unrelated research files were excluded.

## Deployment and recovery

Fly image `deployment-01M2M2WB20EYPN4E48WFYMW96F` deployed successfully to both API machines and cron machine `850d53cd356458`. The new wrapper ran successfully on the cron machine and correctly returned `outside_price_alert_window` during the after-hours smoke test.

A rollback-only preview against Jarod's actual rule state generated exactly one BMNR trigger at 23.60 and -8.385093167701864%, with zero missing observations. A separate normal-idempotency recovery then created only that user's BMNR decrease alert. No saved rule scope or preferences were changed. The message explicitly said it was a delayed September 15 alert caused by a monitoring error.

Email delivery 752 was sent at September 16 03:11:26 UTC (September 15 8:11 PM Pacific). Postmark reported a non-sandbox `Delivered` event one second later. The production duplicate guard returned `duplicate_alert_already_sent` for the price-session key. This confirms receiving-server delivery, not inbox placement or that the user read it.

The next regular-market five-minute cycle has not occurred at audit completion. Scheduled timing during that future session is not claimed as already verified.
