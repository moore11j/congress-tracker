# Portfolio import environment repair, October 27, 2026

## Measured issue

Production remains6906305b (PR729). PR730 is merged34831585 but not deployed because an ordinary protected portfolio cycle is unfinished. At06:16UTC it had completed837 of1,135 scopes with zero failed scopes and a live process. The continuation must not be interrupted or the release guard bypassed.

At06:17UTC API credits were at1 and recent throttling returned: latest metric0.98184, recent two-hour average CPU0.13322 versus the shared host's0.125 sustained allowance. Database throttling remains zero. Feed/Insights cached responses took0.507/0.398 seconds; these do not prove full request health or sustained recovery. Receipt `role-cpu-1793081878.json` supersedes the earlier zero-throttle sample.

An8.44-second process sample attributes1.54 CPU seconds to portfolio processes and0.01 to the API process, with short-lived work potentially missed. Latest scheduler logs show seven portfolio continuations using2.1066–3.7757 CPU seconds each, all `preloaded:false`, while neighboring warmers use `preloaded:true`. This does not attribute every historical exhausted credit.

Read-only environment comparison proves the external portfolio and cron import identities differ only in `CRON_TZ`, `PATH` and `SHELL`. No environment values or secrets were printed. External jobs lack the cron launcher overrides, causing the existing exact environment guard to reject import reuse every batch.

## Local repair

Branch `codex/portfolio-import-environment` preserves the exact child execution environment but removes these three launcher-only settings from the import identity. Template imports consistently use the scheduler's launcher settings regardless of which job arrives first. `PYTHONPATH`, `TZ`, database/provider settings, credentials and all other application settings remain identity-bound. No schedules, limits, scoring, portfolio calculations, provider choices, hosts or costs change.

Linux3.12.3 passes85 checks:42 cover both first-job orders, actual compiled cron settings, fresh child processes with exact environment restoration, rejection of meaningful setting differences, ownership, queue isolation, portfolio continuation and release guards;43 cover adjacent scheduler/outcome behavior. One check is skipped because Supercronic is unavailable locally. One initial invocation used the wrong outcome-test filename and ran no tests; the corrected run is included above. No full-suite pass is claimed.

Six alternating cold/reused-import runs against the retained public-member fixture produce identical five-lookback persisted results: five runs,1,220 points and289 positions. Full normalized state SHA256 is `c0382bcfcbac01b6df2fe2c29216f26b46c71340fbf83f5cd50ca2a186b1de51`, also equal to the October15 captured baseline. No network, production writes or emails occurred. CPU medians are10.793678seconds cold and9.912859reused (8.16% lower), with wide variation. The one-time template preparation costs8.293732CPU seconds. This includes fixture loading/persistence and is not a production job-savings or sustained-capacity claim. Receipt: `artifacts/portfolio-environment-benchmark-oct27.json`.

At06:27UTC the protected ordinary portfolio has advanced956/1,135, zero failures, one live process and no queued jobs. It remains unfinished; no backend repair has been deployed.

## Related migration evidence

PR730's required-period selector changes only GCT among280 retained candidate pairs;49-source full-result parity and203Linux tests remain valid. CoreFMP remains selected. GCT core ordinary refresh is due16:11UTC; COO core12:46UTC. See [revenue repair](sec-revenue-required-periods-2026-10-27.md) and [institutional capacity review](institutional-capture-capacity-2026-10-27.md). No current institutional payload export or source switch occurred.

Next: finish isolation/parity/CPU checks, prepare the exact reviewable repair, wait for protected completion, then guarded release and installed/ordinary verification. Sustained capacity and all broader migration gates remain open.
