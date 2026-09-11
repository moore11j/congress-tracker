# September 10 monitoring email incident

## Production evidence

Jarod (user 1) and Nancy (user 14) both had enabled daily watchlist subscriptions and separately enabled immediate 5% custom-price rules. No subscription settings were changed during this repair.

Daily delivery records 714 and 715 were sent at September 10 01:43 UTC, which was September 9 at 6:43 PM Pacific. Their old window keys ended in `2026-09-09:2026-09-10`. The September 10 scheduled close generated that same key and was therefore skipped as already sent. A production September 10 preview reproduced `duplicate_window_already_sent` despite qualifying activity.

NBIS alerts 7034/7037/7040 (user 1) and 7035/7038/7041 (user 14) all contained the same -5.245392136467643% move and 227.7427 quote. Six email deliveries, IDs 718–723, were recorded. The evaluator previously treated missing quotes as false conditions, allowing a subsequent valid reading to re-arm the alert; timestamp-based trigger identities and alert-row-based email identities then permitted repeats. Regression tests reproduce missing-quote recovery and actual same-day reset/re-cross sequences.

## Deployed changes

- Fix monitoring windows at 1:05 PM Pacific and identify reports by Pacific report date, independently of delayed execution time. Weekday retries run at 1:20, 1:35, and 1:50 PM.
- Preserve state on unknown observations. Serialize competing rule evaluations and restrict one-day price-percentage triggers to one per rule/ticker/market date. Delivery identity additionally merges equivalent rules across a user's watchlists.
- Isolate recipient failures. Retry explicit provider rejections; do not blindly retry ambiguous Postmark outcomes. Surface recipient failures as nonzero job exit status.
- Query newly discovered events when assembling daily watchlist activity. Use optional calendar cache enrichment without blocking email on live calendar requests.

Fly deployment `deployment-01M27E6E372KHYC115W0YPBW72` updated both API machines and the cron machine successfully. No database schema migration was required.

## Verification and recovery

The focused suite passed 171 tests across digest routing, price rules, delivery, monitoring, calendar, schedule, delivery matrix, and confirmation monitoring. Existing dependency/deprecation warnings remained.

The deployed identity function was also evaluated against all six original NBIS rows: each recipient's three rows now produce exactly one delivery identity.

Only the missing September 10 reports for users 1 and 14 were replayed, through normal subscription and idempotency checks, without force mode:

| Recipient | Items | Delivery record | Sent UTC | Provider result |
| --- | ---: | ---: | --- | --- |
| Jarod | 21 | 724 | September 11 05:17:18 | Delivered |
| Nancy | 24 | 725 | September 11 05:17:19 | Delivered |

Both messages were non-sandbox sends. Postmark's message-event records confirmed delivery, and the production duplicate guard subsequently returned `duplicate_window_already_sent` for both report keys. Delivery confirms acceptance by the receiving mail server, not inbox placement or that the recipient read it.

These fixes apply to all subscribers using these delivery paths. The next automatic close-of-day run has not occurred at the time of this audit; this is verified recovery and deployed regression coverage, not a claim that a future scheduled run has already succeeded.
