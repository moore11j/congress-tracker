# Institutional production preparation capacity, October 12, 2026

## Current authoritative state

At00:29:59UTC, a PostgreSQL read-only inventory finds769 staged institutional documents:718parsed,17failed and34quarantined. All769 are unvisited by the direct publisher; ownership remainsFMP generation0. Parsed originals total82,758,139bytes, maximum3,328,754bytes, with zero missing originals. The first20 eligible unvisited documents total1,878,180bytes. This is original availability, not coverage acceptance or publication. Receipt `institutional-live-inventory-1791764999.json`. Initial query failed before inventory execution because the diagnostic used nonexistent discovered_at; that field was removed, then the terminal query rerun.

Fly CPU receipt `cpu-quota-1791764894.json` records121 one-minute samples with zero five-minute throttle rate over the prior two hours. API baseline remains0.125CPU; current user/system/nice/softirq sum is about0.111CPU. Burst balance is9,382.66, with preceding range3,938.22 to9,818.66 and multiple deployments in that window. This does not establish sustained weekday spare capacity. Database sampling has substantial I/O wait at that instant, so the earlier mostly-idle database observation is not a current universal claim. Hosts are unchanged.

## Guarded production preparation attempts

The diagnostic uses existing target selection/byte bounding, original parser, streamed complete-peer preparation, saved references and identifier evidence. It never calls the publisher, changes source ownership or enters a write transaction. Read-only repeatable-read PostgreSQL, three-second statement timeouts, existing background pressure/collector guards, nice10, CPU/wall/memory bounds and checks for active scheduled data jobs apply. Collector orphan recovery is disabled for this diagnostic. It yields when scheduled data work begins; it does not claim an exclusive scheduler reservation.

First attempt at00:30:54 is terminal/held in target preparation as ordinary data work begins:2.0432CPU/6.332wall seconds, peakRSS302.09MB. Receipt `institutional-readonly-capacity-1791765063.json`. Actual scheduler logs then verify completion of the contending00:31 news, fundamentals and reference jobs before a single retry; the retry was not based on silence or timeout.

Second attempt at00:31:40 is terminal/held during complete-peer scanning when the next data job begins. It selects20targets and spends1.519CPU seconds preparing their scopes; total13.9645CPU/20.1095wall seconds, peakRSS301.88MB. Receipt `institutional-readonly-capacity-1791765120.json`. No peer completion or publication/write cost was measured. Do not extrapolate this partial run to a complete batch, or label either attempt a passed capacity test. Both stop cleanly, zero provider requests, canonical writes or emails. No more opportunistic retries were launched.

The existing first100 local batch and complete574-source/full-state/no-send consumer replays remain dated evidence, not current production capacity. Code confirms the batch verifies the complete peer inventory even when limit20 reduces target work, so dividing the100-target time byfive would be unsound.

## Next action and unchanged gates

Add a bounded read-only preparation mode that executes in the existing serialized data lane, so the real pipeline can complete without overlapping imports. This is the next implementation requirement, not already implemented or authorization for a new recurring schedule. Measure full preparation, retain source/coverage holds and consumer checks, then validate active publication capacity before selecting institutions. No source activation, schedule change, hardware purchase, customer email or FMP cancellation. All local diagnostic handles are terminal; no local PostgreSQL.

Primary artifacts are under artifacts/direct-feeds/free-replacements-2026-10-09. Helpers read_institutional_inventory_oct12.py and probe_institutional_preparation_oct12.py are diagnostic-only, not deployed application code. Previous goal turn deployed a verified financial repair; this turn provides authoritative inventory and partial capacity evidence that changes the next implementation action. Goal remains active.
