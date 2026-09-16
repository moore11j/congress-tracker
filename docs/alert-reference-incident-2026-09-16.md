# ONDS daily-price alert: stale reference close

## Evidence

- September 16 production trigger 52 (rule 2, user 1; monitoring alert 8311) recorded ONDS at $7.135 and a -6.364829396325463% change.
- The most recent cached ONDS daily close at evaluation was September 8: $7.62. The old one-day evaluator accepted the most recent available row before the current exchange date, without requiring the previous trading session.
- That calculation was `(7.135 / 7.62 - 1) * 100`, not a one-session change.
- A September 16 repair request to the configured historical provider returned an actual September 15 bar of $7.24. The correct observed change was `(7.135 / 7.24 - 1) * 100 = -1.450276%`. No 5% decrease alert should have fired.
- The screenshot's high-to-current comparison is not the configured daily-change baseline; the proper baseline is the previous trading session's close.

## Correction

- One-day percentage evaluation requires the exact previous US market session, including holiday/weekend handling. It does not substitute an older row.
- The independent five-minute price-alert worker refreshes missing references for enabled daily price rules using date-preserving historical bars. It checks that the required date actually exists afterward; a provider success response alone is insufficient.
- Reference refresh is bounded (20 requests / 60 seconds checked between requests). Unavailable references are logged and reported as a failed cycle while other valid alerts still proceed. Missing data is not interpreted as a threshold crossing.
- Each generated daily-price alert stores its reference date, reference price, current price, timestamp, and calculated percentage.
- Immediate delivery rejects queued daily-price alerts without valid calculation evidence. Verified emails explain the observed percentage and both prices.
- Existing once-per-session trigger and recipient delivery deduplication remain in place. Already delivered emails and historical trigger rows are preserved; this change does not recall email or send unsolicited correction messages.

## Verification

Regression coverage includes the original stale ONDS close, a nonqualifying -1.45% move, a subsequent genuine qualifying drop, one-per-day behavior, BMNR first-observation eligibility, weekend/holiday reference dates, queued-email rejection, reference-refresh failure, and continuing other deliveries when one reference is missing.

The production September 15 ONDS reference was repaired to $7.24 before release. The deployment is packaged from the currently deployed `6dce1228` baseline plus only this incident's files, excluding unrelated uncommitted research/application changes.

### Release results

- Isolated-release tests: **100 passed** across daily-price reference, custom-rule evaluation, price-worker, and email/digest suites. One pre-existing local requests dependency warning was emitted.
- Deployed image: `registry.fly.io/congress-tracker-api:onds-reference-20260916`, digest `sha256:c34c3e892f74f0d5adc6ada9ac7726523cb0d3d517262c5fc2dd46c1e3786d4d`. Both API machines and the cron machine passed deployment checks.
- Production reference check: 22 symbols required by enabled daily-price rules, 22 available, zero unavailable; three additional missing references fetched successfully.
- Production replay of the original quote through the new evaluator returned September 15 / $7.24 as the reference and **-1.4502762430939287%**. The replay substituted only an in-memory quote and did not write trigger state.
- Delivery guard for actual historical monitoring alert 8311 returned `unverified_daily_price_reference`.
- Verification did not send emails. Historical false-trigger records remain for audit, including their existing same-session deduplication effect; sent messages cannot be recalled by this change.
