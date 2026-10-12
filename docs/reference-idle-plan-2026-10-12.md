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


## Deployed and scheduled due work verified

October12: PR615/966fe582 idle reference-plan repair deployed38286135776SUCCESS; 72 runtime hashes and deployed3.12 saved-state proof pass. 104 tests and actual377-scope PostgreSQL current/expired full-result parity pass. Ordinary17:46 rotation visits500 originals, plans1639 scopes/1200due and stages two references within the unchanged budget. The observed live preflight correctly falls through for that due plan; ordinary new idle-path CPU saving remains unobserved. Feeds0.503/0.555s and AAPL1.274/NVDA1.626s all200; sustained API capacity remains open. Fresh read-only financial audit1097usableSEC/1015combined and255verified issuers without SEC ratios; these are presence counts, not equivalence. CoreFMP/institutionsfalse; financial/debt coverage, institutions capacity/coverage, transcripts/all consumers, finalMassive/globalFMP/billing remain. No source/schedule/host/subscription change or validation email. All local handles terminal/no localPG; goalactive, concrete progress.

PR615 https://github.com/moore11j/congress-tracker/pull/615 source7ed3bf2c0524fcfcca1649ffc45abd0ea1882f7f merged966fe582d11ef2fadcbf8a87b08e652b60bb6af7. Exact-head secret scanpasses; dependency audits retain unchanged Pillow10.4.0 and frontend nanoid/Next/PostCSS/Sharp/source-map-js failures. Direct diff reviewed; no automated-review or full-suite claim. Artifact attachment attempted, existing100-item limit remains.

Receipt reference-idle-release-1791827146.json verifies72 source hashes and Python3.12.15. The saved actual377-scope proof/retry expiry is identical with no ORM imports. Live read-only preflight uses0.16927CPU seconds and returnsNone for the now-due plan; import/fixture overhead0.00522CPU seconds. This is not an observed idle scheduler saving and must not be compared as though it were a full before/after worker benchmark.

Ordinary reference-queue-1791827184.json at17:46:24 verifies the500-original window at17:46:07,1639scopes/1200pending, two newly staged unavailable identities with preserved source hashes and no public writes. Scheduler receipt scheduled-job-cpu-1791827228.json confirms completion at17:46:19:3.8269CPU/12.61wall seconds, exit1 because coverage is partial. The new fast path correctly does not suppress real plan rotation/collection. Subsequent idle-window behavior and sustained quota recovery remain observational gates.

Current institutional inventory institutional-live-inventory-1791827075.json:718parsed/51quarantined, all769unvisited by the publisher, all parsed originals present (82,758,139bytes). OwnershipFMPgeneration0. Former17failed entries are now quarantined; this current state does not make their evidence usable or establish publication capacity.

Fresh financial presence audit paired-free-coverage-1791827224.json at17:47:01:1751attempted/baseline scopes,1097usableSEC,1015combined,255verified issuers without SEC ratios. SEC directory absences380, missing3/expired2,14companyfacts-not-found. Finnhub cached identity/version gaps205, missing79/expired29/unavailable24. Only one compared SEC period is older (ACN),975newer/46same. Market fields are intentionally absent from this preparation and await finalMassive. Net-debt/EBITDA remains absent across1364previously populated baseline records; EBITDA/debt definition reconciliation is unresolved. These checks exclude extra issuer/foreign cash-flow supplements and do not prove complete provider coverage or numeric parity. Prioritise those contract/coverage gaps and consumer checks after capacity work, not subscription purchase prematurely.

Postrelease public receipt issuer-staging-public-1791827141.json records all200responses, including the retained four distinct AZO press links. No whole-universe, sustained-capacity or complete-alert guarantee follows from these samples. The overall FMP retirement goal remains active.


October12,18:05: Bounded ordinary log observation1791828256 verifies reference18:03 exit1/0.2423CPU/0.40wall and18:04 exit1/1.4288CPU/2.01wall. The cheaper partial run is consistent with the new exhausted-plan branch, but this mixed sample is not sustained saving or whole-window capacity proof. Read-only preflight1791828296 returns same-minute cooldown,72hashes unchanged; does not establish the preceding run branch. All new handles terminal/no localPG. ACN ordinary collector-to-financial propagation remains pending; goalactive.
