# Institutional target work and prepared schedule, October 11, 2026

The old target guard rejected an entire selected batch when its complete originals exceeded100MB. Such valid large filings could repeatedly block both new work and retries. The new selector defers whole filings after an ordered prefix, retaining the existing100MB target budget and30MB per-original bound. Combined mode reserves half the byte budget for each nonempty queue, then reuses spare bytes. The retry cursor advances only through the selected retry prefix. Complete peer inventory verification is unchanged; deferred targets remain peer evidence and remain queued for a later batch. Missing/oversized originals still fail closed before publication.

The prepared hourly schedule runs at minute37 in the existing serialized data lane, with limit20, combined new/retry mode, reviewed staged identifiers and staged references. It never sets DIRECT_13F_PUBLICATION_ENABLED; fly.toml remains false. This prepares a trigger for later cutover. Disabled scheduled operation is not successful institutional publication or capacity acceptance. No customer email or source selection is included. Existing calendar/delivery times and host sizes remain unchanged.

## Validation

107 focused institutional/batch/scheduler checks pass on local Python3.14.2; five unchanged Linux-specific process tests skip. Two relevant schedule/calendar compilation checks independently pass on production Python3.12.15 using disposable files, without changing its actual scheduler. All50 batch tests pass, including full-filing deferral, repeated queue progress, combined byte fairness, spare capacity, no skipping a larger next retry, and invalid-source/too-small-budget rejection. No full-suite claim.

Actual PostgreSQL read-only transaction, three-second statement timeout:200 parsed production targets total21,748,178bytes. Default selection is identical. An in-process diagnostic10MB budget chooses81 entire documents (51new/30retry),9,846,868bytes; both group prefixes are preserved. The retry labels in this selector-only probe are hypothetical, not production publication receipts. No production write, source request or email. Receipt target-budget-live-1791757273.json.

## Broader acceptance and capacity

The separate streamed full cohort firstpass now has574sources and19qualifying events, with matching statuses, holdings, changes and holds against the prior integrated baseline. All event semantics match after excluding cross-run publication/creation timestamps. One DKS canonical hash differs because it binds the holder event timestamp and payload availability timestamp; this is not a claim of cross-run hash equality. Full within-run repeat and no-send consumers remain running on handle16423. Receipt streamed-full-first-pass-parity-oct11.json.

Fresh CPU evidence cpu-quota-1791756991.json has97 one-minute zero-throttle samples after20:39UTC through22:16UTC, with intervening releases. Shared baseline remains0.125CPU; recent nice/user/system/softirq total is about0.128CPU, leaving no demonstrated sustained spare capacity. Burst balance is9,498; database remains mostly idle. This is improved observation, not steady-state or publisher capacity proof, and does not justify more hosting spend. No active publication until retained coverage, ordinary operation and consumers pass.

## Release state

Prepared locally; no deployment, source activation, resource change, subscription or cancellation. Previous PR585/6ee941f0 remains production. Next review/deploy the disabled schedule and verify its ordinary disabled receipt, finish full16423, then establish bounded active-work capacity and remaining source/consumer gates before institutional activation.
