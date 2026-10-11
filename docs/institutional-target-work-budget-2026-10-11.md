# Institutional target work and prepared schedule, October 11, 2026

The old target guard rejected an entire selected batch when its complete originals exceeded100MB. Such valid large filings could repeatedly block both new work and retries. The new selector defers whole filings after an ordered prefix, retaining the existing100MB target budget and30MB per-original bound. Combined mode reserves half the byte budget for each nonempty queue, then reuses spare bytes. The retry cursor advances only through the selected retry prefix. Complete peer inventory verification is unchanged; deferred targets remain peer evidence and remain queued for a later batch. Missing/oversized originals still fail closed before publication.

The prepared hourly schedule runs at minute37 in the existing serialized data lane, with limit20, combined new/retry mode, reviewed staged identifiers and staged references. It never sets DIRECT_13F_PUBLICATION_ENABLED; fly.toml remains false. This prepares a trigger for later cutover. Disabled scheduled operation is not successful institutional publication or capacity acceptance. No customer email or source selection is included. Existing calendar/delivery times and host sizes remain unchanged.

## Validation

107 focused institutional/batch/scheduler checks pass on local Python3.14.2; five unchanged Linux-specific process tests skip. Two relevant schedule/calendar compilation checks independently pass on production Python3.12.15 using disposable files, without changing its actual scheduler. All50 batch tests pass, including full-filing deferral, repeated queue progress, combined byte fairness, spare capacity, no skipping a larger next retry, and invalid-source/too-small-budget rejection. No full-suite claim.

Actual PostgreSQL read-only transaction, three-second statement timeout:200 parsed production targets total21,748,178bytes. Default selection is identical. An in-process diagnostic10MB budget chooses81 entire documents (51new/30retry),9,846,868bytes; both group prefixes are preserved. The retry labels in this selector-only probe are hypothetical, not production publication receipts. No production write, source request or email. Receipt target-budget-live-1791757273.json.

## Broader acceptance and capacity

The separate streamed full cohort firstpass now has574sources and19qualifying events, with matching statuses, holdings, changes and holds against the prior integrated baseline. All event semantics match after excluding cross-run publication/creation timestamps. One DKS canonical hash differs because it binds the holder event timestamp and payload availability timestamp; this is not a claim of cross-run hash equality. Full within-run repeat and no-send consumers subsequently passed; see the final receipt below. Receipt streamed-full-first-pass-parity-oct11.json.

Fresh CPU evidence cpu-quota-1791756991.json has97 one-minute zero-throttle samples after20:39UTC through22:16UTC, with intervening releases. Shared baseline remains0.125CPU; recent nice/user/system/softirq total is about0.128CPU, leaving no demonstrated sustained spare capacity. Burst balance is9,498; database remains mostly idle. This is improved observation, not steady-state or publisher capacity proof, and does not justify more hosting spend. No active publication until retained coverage, ordinary operation and consumers pass.

## Release state

[PR586/9618ee6c](https://github.com/moore11j/congress-tracker/pull/586) deployed successfully through38180394245. All28 runtime hashes match at22:28-22:29UTC, both approved hosts remain unchanged, and the queue is empty. The compiled institutional job runs at minute37 with limit20, combined waiting retries and staged evidence in the data lane; publication remainsfalse. Disabled invocation uses0.0117CPU seconds without ORM imports. All eight public requests return200: feeds0.394/0.611s, freshAAPL1.570/NVDA1.602s and50news items. This is sampled recovery, not sustained capacity proof. Receipt two-service-verify-1791757747.json.

The ordinary minute37 serialized data-lane run has now finished with exit0 and publication false; this is a disabled-run receipt, not active publication or capacity proof. Receipt institutional-schedule-runtime-1791758246.json.

Full streamed replay16423 is terminal/pass; its final receipt is below. No provider activation, resource change, subscription, customer email or cancellation. Finish active-work capacity and remaining source/consumer acceptance before institutional activation.


Scheduler logs independently confirm enqueue at22:37:00UTC,8.08seconds lane wait, successful completion in0.2wall seconds and0.038CPU seconds. This verifies serialization/disabled execution only. At22:37:26, other ordinary jobs account for one pending and one running entry; the institutional job itself is finished with runs1/exit0.


## 2026-10-11, 22:38 UTC: Full streamed repeat and consumer acceptance

Full streamed cohort handle16423 is terminal/pass. The retained574 sources,694 comparison documents and132 references produce116 complete current comparisons,19 qualifying events and51 continuity checks. All-table fingerprint d0466e3e69401c926f5193afb3847d358403044566f405f97ea144bccee8bc1d is unchanged on full repeat; repeated publication inserts no filings, positions or events. Monitoring12/signal8/watchlist19 no-send contexts repeat identically, with duplicate alert creation rejected and zero EmailDelivery rows. Elapsed2727.07seconds is local SQLite/Python3.14.2 rehearsal time, not production throughput. Source/key coverage holds remain as recorded:49 held and272 waiting. Total127 feed events includes nonqualifying events and must not be confused with19 qualifying SEC events. Receipt institutional-streamed-full-cohort-oct11.json; no production writes or emails.
