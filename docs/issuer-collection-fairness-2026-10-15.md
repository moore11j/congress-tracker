# Issuer collection fairness, October 15, 2026

## Observed gap

Read-only operational evidence at21:56UTC finds all eleven configured issuer indexes present: ten parsed with ages3.73-18.73hours, and the new UL index pending with zero attempts. Nine reviewed static transcripts are parsed, but MRNA/RDDT have not been rechecked for40.19/39.73hours. The21:13 ordinary run processed two without errors and left27 pending documents. These are collection timestamps/statuses, not exported transcript text or customer data. Receipts: artifacts/issuer-budget-metadata-1792101401.json and issuer-run-metadata-1792100985.json.

The shared collector spends its budget on discovery first; reviewed static pages receive only the remainder. Continuous discovery can therefore delay existing coverage indefinitely. A complete-source cold-start replay reproduced44-48simulated hours before those nine pages received their first attempt. Current production is not a cold-start replay and those simulated delays are not asserted as observed production waits.

## Repair

Reserve one slot for a due reviewed static page within the existing two-request hourly allowance. No reservation when all reviewed pages remain inside their recheck period or when the registry contains no such page. Daily attempt persistence, explicit longer recheck settings, removed-entry filtering, source cooldown termination and no-network-during-transaction safeguards remain.

Give newly configured and at-least36-hour-old eligible indexes priority over queued documents. This protects discovery of new reports before the existing48-hour reader hold, with12hours of margin in the reviewed simulation. Eligibility still enforces the existing daily or longer retry boundary. SEC transcript preference within the remaining queue and immutable SEC original reuse remain unchanged. No additional source or request permission is granted.

## Validation

Final affected suite: **509 passed on Linux Python3.12.3**. Focused58 and isolated SEC80 were intermediate subsets, not additional distinct checks. An initial broad command named a nonexistent test file and ran no tests; corrected selection initially produced478passes/26failures. Two assertions assumed the old request allocation. The other24exposed an order-dependent SQLAlchemy default clock in the SEC test fixture, confirmed by80isolated SEC tests passing. The fixture now assigns its synthetic observation timestamp through a scoped before-insert listener; production freshness/availability checks were not weakened. Final509includes those tests together and five index-priority/recheck cases.

Seven-day discovery-only replay uses84complete saved originals, all eight enabled policies, eleven indexes,31transcript rows and34event/cover rows. Backlog clears at simulated hour37; original index refresh gaps reach38hours. It excludes the separate static collector, so is not whole-product proof.

Full collector baseline versus final repair uses all ten configured companies, eight discovery policies, eleven indexes and nine static pages over169hourly invocations per mode. Source corpus has88saved public originals; missing historical MSFT Q2 capture is an explicitly injected synthetic403, not a live provider observation. All other reads use complete captured bytes. Pure parsing is memoized only for byte-identical originals and identical metadata; 39unique inputs execute the real parser. Scheduling, database state, source verification and clocks execute normally. This is not a CPU benchmark or future live-source availability proof.

| Evidence | Deployed baseline | Final repair |
| --- | --- | --- |
| Static first attempts | Hours44-48 | Hours0-8 |
| Static repeat gaps | 24hours | 24hours |
| Maximum index refresh gap | 38hours | 36hours |
| Document/revision identities | 85/84 | 85/84 |
| Requests across169invocations | 332 | 333 |
| Per-invocation maximum | 2 | 2 |

The earlier reservation-only candidate allowed47-hour index gaps and was refined before release. Every URL retains at least24hours between attempts. The final full immutable-source fingerprint is identical to baseline: 69790fa9451cfde3bbcb2d430c5c95a569b0d99d3fa0b15df3baef0e61e6229c. Earlier first observations legitimately differ as pages receive a turn sooner; these timestamps are not replaced or backdated. No new canonical publication, provider request or customer email occurs. Receipts: artifacts/issuer-shared-budget-win32-oct15.json, issuer-fairness-replay-linux-oct15.json (intermediate), and issuer-fairness-replay-linux-oct15-v2.json (final).

Installed-code probe rehearses locally on Python3.14.2: five source/config hashes, actual synthetic PDF parsing, shared-budget progress, aging-index priority, prepared-reader identity, one isolated research document and unchanged repeat with zero research events/email. Exact release/runtime and ordinary production observation remain pending.

## Release boundaries

No schedule, request limit, host, provider selector, financial definition, paid subscription, video, landing page, customer delivery or FMP cancellation change. This repairs collection scheduling; it does not establish broad transcript coverage or final cutover readiness. Core/transcripts remainFMP and institutional publication remains gated. Fresh source coverage, all scheduled consumers/rankings, sustained host capacity, paid Finnhub and final Massive validation, global zero-FMP traffic and billing retirement remain open. Pending private-data export approvals were not retried. Prior percentage-only response was no progress; this source-based scheduling evidence, repair and verification are concrete progress toward the active goal.


## Deployed and ordinary source hold observed, October15 22:13UTC

[PR689](https://github.com/moore11j/congress-tracker/pull/689), tested987b8a4898c497f930ba24a100c85b34700ed8f4, merged/deployed209c22bb28d53ba62abce43b032dce4909c4d28d. All exact-head dependency audits, secret scan and Vercel preview pass. Backend merge parity is exact. Initial branch ancestry included the previous squashed release; rebased only the receipt/repair commits onto actualfbb0220f, with zero tested-backend diff and unchanged unrelated tsbuild hash. PR attachment remains unavailable because the chat has reached100attachments.

Guarded workflow38815357280/job116698535411 completed success; readiness and Premium privacy checks passed22:12:41UTC. A transient diagnostics502 was followed on the same job with status metadata; no duplicate dispatch. Existing .strategy-history-repair submodule cleanup warning is nonfatal.

Production Python3.12.15 verifies all five source/config hashes and the isolated synthetic PDF/collector/reader/research-repeat proof. Exact reservation/repeat/index-priority checks pass, one isolated canonical document and zero research events/email; zero production record reads, writes or provider requests. Receipt artifacts/issuer-fairness-runtime-1792102392.json.509 affected Linux checks and the final complete-source replay remain the release validation, not a full-suite pass.

Actual ordinary run615 starts22:13:01.838484 and finishes22:13:02.436401. It uses reserved_static_requests=1 and attempts the newly prioritized UL index once. The issuer returnsHTTP403 from its latest-results URL. The existing transport-stop rule correctly prevents spending the remaining slot: zero processed documents, one error,26pending/28unresolved, zero public writes, partial status. Pending count fell because the attempted index is now failed, not because new transcript coverage was obtained. This verifies installed scheduling and source-stop behavior, **not successful ordinary collection recovery**. Receipt artifacts/issuer-run-metadata-1792102408.json; terminal scheduler observer artifacts/issuer-schedule-observation-1792102474.json. No forced fetch, timestamp reset, HTTP impersonation or forbidden-source retry.

Unilever index access is now a fresh confirmed source gap; successful ongoing mixed discovery/static operation and broad transcript coverage remain gates. Preserve the daily retry boundary and seek a permitted alternative or source recovery. The earlier financial historical-PDF retention fix remains separate and does not solve this live discovery-index refusal.

Public rendered-response checks at22:13:03 return200 without server-render errors: Feed0.448s, Insights0.213s, AAPL0.331s, NVDA0.262s (artifacts/public-render-1792102383615.json). These are unauthenticated spot checks, not sustained capacity or full consumer completeness. Core/transcripts remainFMP, institutional publication remains gated, and all financial/consumer/paid-provider/finalMassive/zero-FMP/billing conditions remain. No new subscription, source selection, schedule, host, video, landing or customer-delivery action.


## Ordinary mixed collection confirmed, October16 00:55UTC

Read-only run metadata verifies ordinary runs622 (October15 23:13) and631 (October16 00:29) both terminal: two processed/two parsed, one discovery attempt/one processed, reserved_static_requests=1, zero errors and zero public writes. The collector's accounting includes the discovery result in processed; these are one discovery-stage success plus one static transcript refresh per run, not two newly obtained transcripts. Run631 leaves24pending/26unresolved, so broad coverage remains incomplete. The later actual execution reflects queue timing, not a schedule change.

Static-document metadata confirms MRNA refreshed at23:13:05.710346 andRDDT at00:29:49.091275, both inside those completed run windows. The prior39-40hour static starvation has recovered for those two sources. All nine configured static rows are parsed; all eleven indexes exist, ten parsed andUL still failed from its October15 22:13HTTP403. Index ages are0-24hours except no newUL success, and static ages range0.42-26.7hours. These metadata observations prove ongoing mixed collection, not full prepared-reader or research-publication availability. Receipts: issuer-run-metadata-1792112082.json and issuer-budget-metadata-1792112102.json.

A separate read-only verification of all ten configured readers reached its50second local observation timeout without a result. Exact-code process scan confirms remotePID2396 still running with7.91CPU seconds at00:57UTC (issuer-reader-liveness-1792112221.json). No duplicate probe was started and no source payload/customer data was exported. Reader acceptance stays unverified until a completed receipt; process liveness is not success. Pending private-export approvals remain untouched.

At00:58UTC the same exact reader process2396 is confirmed running with14.41CPU seconds (issuer-reader-liveness-1792112298.json). The existing process retains its20/25CPU-second safety limit; no new probe was launched. No completed reader receipt exists. Documentation diff check passes and unrelated frontend build-metadata SHA256 remainsA185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA.
