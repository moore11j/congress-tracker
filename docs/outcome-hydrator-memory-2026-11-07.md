# Outcomes price-repair memory

November 7, 2026. The production price hydrator completed its last observed run in 372.64 seconds. Available host memory briefly fell to about 169 MiB with 13 pending jobs, then recovered to about 894 MiB. This observation does not establish a single root cause or an out-of-memory event.

Two queries unnecessarily loaded every historical score's source-evidence strings. Public event grouping now reads only its eight required scalar fields. The price hydrator selects candidates with deferred evidence and explicitly loads complete evidence only for its bounded selected batch. Deferred accesses raise, avoiding accidental per-record evidence queries. The original selected order is retained after the bounded SQL read.

No horizon, score, source, provider, schedule, quota, leader priority, round-robin rule, immutable entry or recorded observation is changed. The repair still considers all candidates; this is not a history cap. No additional compute is requested.

Validation before release:

- 120 local checks pass across integrity, selection, continuation, daily repair and ingestion. Five new cases cover batch sizes 1, 2, 4, 5 and 20, expected selection order, bounded evidence reads and unchanged immutable history.
- The existing 105-anchor integration case retains its exact observations and provider windows while verifying that event projection does not fetch evidence columns.
- An offline comparison executes the complete baseline and candidate job against 1,000 synthetic snapshots with large evidence strings. All 100 selected records, evidence values, price windows and result fields match, excluding elapsed time. Peak traced Python allocations are 68,312,908 bytes before and 9,987,060 after. This is a synthetic allocation measurement, not production RSS or capacity proof.
- An existing daily coverage test failed identically on the unchanged baseline because its June fixture fell outside today's relative 30-day window. Its clock is now fixed to the fixture date; production logic is unchanged.

The initial broader Windows run encountered temporary-directory permission errors; rerunning with a dedicated workspace temporary directory passed. Linux CI, installed hashes and ordinary scheduled operation remain release gates. Production source selection and FMP shutdown gates are unchanged.
