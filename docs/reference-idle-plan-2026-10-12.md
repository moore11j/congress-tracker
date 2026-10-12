# Institutional reference idle-plan preparation, October 12, 2026

## Current capacity evidence

The24-hour Fly metrics receipt role-cpu-1791826489.json contains289 five-minute samples for the current API machine. Mean busy CPU0.15122, recent two-hour mean0.21221 versus shared2 allowance0.125; latest0.13273. Burst balance4594.02 at the latest sample. Deployment and diagnostics overlap this window, so it is not an isolated steady-state estimate. Recent throttling includes nonzero samples, although the last sample is zero. Database recent mean0.11436CPU. No hardware change.

Recent scheduled receipts show the reference worker taking1.33-1.55CPU seconds per full run, including reused/unavailable evidence. Read-only reference snapshot17:34:54 contains377 scopes in its current bounded window and1030 retained attempts. The current plan is partial/truncated; publication remainsfalse. A separate import-only alert diagnostic costs2.395CPU seconds under cProfile, with no networking/job invocation/email. It identifies startup work but does not measure the entire alert cycle or justify delaying alerts.

## Repair and preservation

The reference preflight now reads its existing cached universe and attempt timestamps using the bounded, read-only driver connection. A shared pure helper supplies the exact quarter and24-hour retry rules to both preflight and full planner. It can prove no due scopes only if the plan version/quarters match, its observation is within the existing15-minute lifetime, and every scope has a qualifying recent attempt. Verified source records can only exclude additional candidates, so no source payload fetch is required for that proof. Unattempted/expired scopes, stale/future/invalid plans and read errors fall through to the complete worker.

No planner cursor, attempt timestamp, source observation, recurring trigger or request budget changes. Idle proofs preserve held/truncated coverage and the previous partial exit code; they do not claim that unavailable identities are complete. Existing provider cooldown behavior remains. No production source ownership or canonical data changes are included.

## Verification

Python3.14.2:104 reference preflight/planner/identity/mapping tests pass. Cases cover cached-plan expiry at899/900/901seconds, retry expiry, missing/naive timestamps, invalid keys, quarter rollover, version mismatch, missing weights, invalid coverage state, and subprocess refusal of ORM imports while preserving0/1exit codes. Existing selection and reference provenance tests pass.

Receipt reference-idle-parity-1791826862.json captures current production planning state at17:41:01UTC in repeatable-read/read-only PostgreSQL, with3-second statement timeouts and process CPU/wall limits. The deployed and candidate full planners produce identical entire results for377scopes now (zero due) and after one day (377due, identical two selections). The new early proof accepts only the idle case and retains partial coverage. No provider requests, production writes or emails. The future cached-plan comparison tests retry selection equivalence; the fast path correctly rejects its expired plan, leaving full plan rotation to the unchanged worker.

Release, deployed hash verification, ordinary idle CPU savings and subsequent due/rotating scheduled work remain to be observed. Fresh samples alone do not satisfy sustained-capacity acceptance. Remaining migration gates include institutional coverage and writer capacity, financial/debt definitions and scheduled propagation, transcripts, all alert/ranking/digest consumers, finalMassive, global zeroFMP traffic and cancellation.
