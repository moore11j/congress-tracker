# Financial metric refresh capacity

## Measured issue

On October27 at07:44UTC, production's warming receipt contains2,490 symbols, including36 priority scopes. The configured eight-symbol limit and existing five-minute trigger allow at most2,304 attempts per24hours. The old1,751-symbol capacity test no longer represents current demand.

A read-only bounded probe found1,995 metric caches under24hours old,164 between24and26hours, six between26and48hours and one at least48hours old. Cache counts exclude symbols whose preparation fails before writing a panel. All last20 ordinary runs completed eight attempts in0.620to2.476seconds, with no elapsed/quota stop. These observations isolate structural capacity pressure; they do not prove every overdue cache has the same cause.

Receipt: primary `artifacts/metric-capacity-1793087089.json`. Probe used a read-only transaction,3-second statement timeout and10/12CPU limits; no source requests, production writes or record payload export.

## Repair and limits

Raise the configured and hard maximum metric batch from eight to ten. The existing five-minute trigger,20-second elapsed budget, shared45-per-minute limiter, daily per-symbol retry rule, priority ordering, source locks and serialization remain. Nominal capacity becomes2,880 attempts perday. More requests can occur in each run; no claim of unchanged workload or guaranteed continuous freshness is made. The collector still waits until24hours to refresh, so queue delay can create short expiry gaps.

The change adds no service or paid subscription and does not select free core fundamentals. Wrong identities, missing values and denied symbols remain held. The current overall financial audit still loses644 legacy classifications across the expanded universe; refresh capacity is only one readiness gate.

## Validation and release

New three-day simulation covers all2,490 symbols daily despite24 missed runs eachday, while retaining priority scopes and prohibiting any symbol attempt within24hours of its preceding attempt. All117 selected LinuxPython3.12 checks pass in58.61seconds, covering quota, elapsed-budget, cache-clock, repeated consumer and SEC warming behavior. Initial test invocation named a nonexistent budget-test file and collected no tests; corrected selection passed. No deployment yet.

After release, verify installed code/configuration and observe ordinary ten-symbol runs. Recheck broad freshness after a full rotation, together with ticker latency and CPU capacity. Institutional production capture remains separately awaiting explicit approval; FMP stays enabled and Massive remains last.
