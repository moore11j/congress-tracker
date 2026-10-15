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
