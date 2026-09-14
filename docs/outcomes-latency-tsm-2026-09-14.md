# Outcomes latency and TSM coverage

Investigated September 13 Pacific / September 14 UTC, 2026. Production inspection was read-only; no historical records, prices, or observations were changed. The user authorized commit and production deployment after reviewing the fixes below. Deployment verification is recorded separately from the read-only investigation.

## Confirmed production findings

- The default overview returns 500 of 4,087 verified events. Neither TSM event was in that preview. Ticker-specific lookup already returns both; the page lacked a search control exposing it.
- TSM is the first ranked Top Stocks leaderboard item, with current confirmation score 82 and bullish direction in the September 12 UTC leaderboard snapshot. That current score is distinct from immutable event-opening scores.
- Snapshot 579: bullish, opening score 71, confirmation-v1, captured August 5, official entry August 6 at $409.54. The 7D result is measured; 30D is `missing_price`. Its calendar target is September 5, so its target trading session is September 8 after the weekend and Labor Day.
- Independent read-only provider lookup confirmed August 6 OHLC: open $409.54, high $423.91, low $407.99, close $418.20. The user's Yahoo screenshot showed July 13, whose provider OHLC matches the screenshot: open $433.82, high $437.99, low $420.29, close $421.58. The July 12/$409.54 pairing shown briefly in the local UI was a synthetic fixture error, not a production record. The local review was switched to captured production records; no test label is added to the product UI.
- Snapshot 16908: bullish, opening score 100, confirmation-v2, official entry August 24 at $413.87. The old projection published this as a separate event. The new projection keeps it as an internal score update to the August 6 event. No recorded TSM snapshot through September 13 was bearish; the latest stored confirmation was bullish, score 86.
- The current price lookup picks the first cached SPY date after the target: September 7. That holiday row has no source or adjustment metadata. It blocks materialization rather than advancing to the actual trading session. September 8 TSM and SPY rows also lack canonical adjustment metadata, so authoritative hydration is required as well.
- Read-only provider verification returned September 8 authoritative OHLC with closes of $439.00 for TSM and $765.96 for SPY, via `fmp:historical-price-eod/full+corporate_actions`; no split actions were returned for August 6 through September 8. These values were inspected, not written to production or counted as measured outcomes.
- The public summary has 22 completed 30D events and 3,772 completed 7D events. A separate database count found 1,158 verified entries overdue for 30D and 318 overdue for 7D without stored observations. These backlog counts are entry counts, not claims that all are immediately recoverable public events.
- Status reports 3,216 captured securities and 24,763 directional snapshots without canonical entries. Historical snapshots lacking reproducible evidence remain excluded; adding tickers merely to improve accuracy would be invalid.

## Why loads can be slow

The scheduler warms 100-event requests, while the frontend requests 500. Those are different cache keys. Persistent cache expiry was one hour, despite warming only twice per weekday. The warmed 100-event overview inspected in production had expired September 12 at 01:08 UTC. A separate 500-event overview had been generated September 14 at 03:05 UTC.

The already-cached 500-event API calls measured 1,007 ms and 640 ms from this workstation and returned about 877 KB. TSM's direct query took 709 ms. These are warm API timings, not cold-render or browser-load benchmarks. Cache misses still rebuild data in the request path. The frontend also previously bypassed the persistent Next fetch cache and serialized ticker lookup after overview lookup.

## Implemented fixes

- Cache warming twice per trading day matches the 500-event frontend request and prepares all five horizons. Cache freshness is 12 hours with four-day retention to bridge weekends, market holidays, and transient job failures. HTTP/Next freshness remains five minutes, and API cache headers identify memory, persistent, or miss paths.
- Scheduled refreshes run at 05:45 Pacific before market open and after the 16:41 Pacific price-hydration job completes. Weekday cron entries pass `--trading-days-only`, which checks the US market calendar using the New York date and skips holidays. Explicit maintenance without the flag remains available any day. Timestamp metadata makes prepared data freshness visible.
- Horizon targets resolve through the existing US trading calendar. Missing data on the target session remains missing, with no substitution of a later price and no future-session observations.
- All due canonical entries are processed from available cached prices before and after provider hydration, independently of the 100-entry provider-work cap. Existing observations are preserved.
- Provider hydration prioritizes the shared SPY benchmark and published leaderboard tickers. The score-capture universe also starts with leaderboard tickers, and the preview reserves representation for leaderboard tickers with verified events.
- Full-ledger ticker search, an Open Confirmations filter, explicit preview coverage, and pending/missing-price counts. Search uses database ticker filtering rather than filtering the 500-event preview. Ticker and overview requests run concurrently.

## Continuous events across scoring upgrades

The projection groups by security and calculation type, without splitting by scoring version. Only a qualifying bullish/bearish reversal starts another event; mixed/neutral states, elapsed horizons, and score changes retain the same event. This applies to existing history and future captures for every ticker.

Each continuous direction uses its earliest verified entry, preserving its actual opening date, price, score, and horizon observations. Older snapshots without verified entry evidence cannot hide that valid event or backdate its entry. Same-day updates also preserve the original entry; real same-day reversals remain separate events. Date filters run after projection so a filtered view cannot manufacture a new opening. Backtest and integrity-rebuild consumers use the same anchors.

Methodology IDs and immutable scoring snapshots remain stored internally. Public event/status payloads and the interface omit scoring-version fields, labels, and filters. An open event shows its latest confirmation score and direction separately from the opening score.

## Validation and rollout

Regression coverage includes TSM's Labor Day case, exact-session missing prices, idempotent repair beyond 100 entries, leaderboard representation, full-ledger ticker lookup, and a warmed overview that cannot invoke request-time computation. Browser verification used local synthetic fixtures with TSM excluded from the default preview: search retrieved TSM and the Open Confirmations filter retained it.

Focused backend run: 104 passed with the two baseline failures below deselected. Frontend outcome tests: 16 passed. TypeScript checking passed.

Two pre-existing backend tests fail under the current clock: `test_demo_seeder_populates_pending_snapshots_with_prices_and_skips_reruns` and `test_pending_snapshot_listing_skips_price_outcome_lookups`. Both failures were reproduced by loading the unchanged HEAD versions of the two outcome service modules. They are not caused by this patch.

After deploying the backend, run the existing `outcome-ledger-price-hydrator` job and then `outcome-ledger-cache-warm`. Verify the single public TSM event remains anchored to snapshot 579, receives its authoritative September 8 30D observation, and appears in the refreshed 500-event overview. Snapshot 16908 remains internal and must not appear as a second public event. The new cache namespace must be warmed before relying on post-deployment load timing. Deploy the frontend to expose search and the new coverage labels. Recheck the completed count after processing; its increase and resulting accuracy must come from verified observations, not a presumed favorable TSM outcome.

A local SQLite replay imported the actual TSM snapshots, entries, and observations, then hydrated the independently verified September 8 TSM/SPY OHLC. The production materializer created exactly one missing observation: TSM 30D +7.193436538555453%, versus SPY -0.5517975617039509%, excess +7.745234100259403%. Rerunning created zero observations. This validates the repair; production was not changed.

Final read-only production projection audit: 4,087 previous public events become 3,062 continuous verified events, with 2,110 open events and zero duplicate open security/calculation-type keys. Coverage increases from 2,454 to 2,455 securities; no previously visible ticker is lost. TSM has exactly one open event, snapshot 579. These counts are an audit of the new code against stored history, not a production deployment.
