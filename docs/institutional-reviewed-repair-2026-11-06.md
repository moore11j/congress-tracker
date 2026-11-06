# Reviewed historical institutional repair, November 6, 2026

Status: PR776 deployed as `f0b87fcc` through guarded run42300176982, with 492 Linux checks/one skip, seven installed hashes and the receipt schema verified. No production source correction, publication activation, provider switch, subscription, or customer delivery.

The complete-original source planner permits only independently corroborated dollar-unit corrections, exact scaled floating-point roundtrips, and fully reconciled repeated XML rows. It requires the whole original and whole canonical filing population, preserving IDs and security mappings. Unsupported weight, ownership, class, option, principal, amendment, or independent-unit conflicts remain held.

The operation bundle combines source corrections with the actual historical summary/activity rebaseline introduced in PR775. It guards every affected complete population and permits only existing-row changes. Source ownership markers prevent legacy imports from overwriting repaired positions. Staged source hashes, metadata, parsed status, publication receipts, selected provider and generation are checked again in the database. Canonical advisory locking and immediate table-lock conflict rejection prevent concurrent writers. The operation records a complete receipt, invalidates feed caches, and supports exact repeat and guarded rollback. Rollback refuses to overwrite changes made after repair. No automatic caller, endpoint or schedule is added.

The PostgreSQL transaction is bounded to 40 originals, 1,000 symbol/quarter scopes, 150,000 guarded rows, 15,000 updates, 30 MB of bundle JSON, 20 MB of staged originals, 20-second statements and a 45-second transaction check. Publication must remain disabled and FMP must still own the institutional feed. Local validation does not authorize an unreviewed bundle or waive a changed population.

## Evidence

- 95 focused local checks pass, covering complete-source reconciliation, historical event dates, exact corrections, stale populations, malformed/tampered evidence, partial-failure rollback, source ownership and staged-source/generation holds.
- The complete actual-runtime preparation reparses all 36 originals and repeats all 822 peer scopes. Its reviewed bundle is `777bbfbafa165e454135f72a2ef0976dd5290ccc4348c77d4f60c2842f199663`.
- The bundle contains 7,597 coordinated updates: 36 filing ownership records, 1,196 positions, 921 derived changes, 777 summaries, 3,940 activities and 727 existing feed records. All 1,428 existing feed identities, source URLs, stored dates and effective alert dates remain unchanged. Existing synthetic alert copies remain unchanged; no new events or sends.
- The initial actual PostgreSQL rehearsal loads all 110,763 guarded rows from the immutable complete-peer fixture. Apply updates 7,597 rows, repeat updates zero, rollback restores the complete original state, and rollback repeat updates zero. Canonical advisory-lock and ordinary table-writer conflicts are rejected. Initial apply/rollback take 37.4/36.0 seconds.
- Guard reads now project columns directly instead of constructing 110,763 ORM objects. The exact staged-source PostgreSQL entry passes complete source inspection, wrong-generation rejection, apply, repeat, rollback and rollback repeat. All 36 complete originals reconcile after apply; cache invalidation is verified. Apply/rollback take 28.3/29.0 seconds; repeats take 15.3/17.2 seconds. These local timings are not production capacity evidence. The final additional deadline checks are covered by the focused tests.
- The original complete source replay (6,472 plus exact repeat) and separate 1,970-event consumer replay retain their documented scopes. This repair does not qualify unmapped/current-quarter filings or establish global capacity/financial/transcript/alert acceptance.

Artifacts: `historical-bundle-preparation-nov6.json`, `reviewed-historical-bundle-nov6.json.gz`, `historical-bundle-postgres-nov6.json`, `staged-historical-bundle-postgres-nov6.json` in `artifacts/institutional-current-nov1/`. Original fixture SHA256: `2a367b72b5513db5350489d61c43993e1c9c8541fe3db1625d74ccec09438efb`.

Next: release checks and installed verification; inspect current production populations without applying stale plans; apply only an accepted exact bundle, then verify consumers and scheduled institutional operation. Broader financial/content/capacity checks, Massive last, zero-FMP and billing remain open.

## Production population guard and query repair

All 36 staged originals, 1,686 filing rows and 21,904 derived changes match the reviewed production hashes at November6 21:54 UTC. The original 75,827-position read hit its unchanged 23-second outer limit, so no correction was attempted. Read-only EXPLAIN identifies 887 index scans, including 64 repeated period scans, from tuple-IN expansion over 822 symbol/period pairs.

The follow-up factors common periods into explicit equality plus symbol-IN predicates, preserving the exact symbol/period pairs and the unrestricted complete source-filing population. The plan uses three index scans; the bounded actual production read returns all 75,827 rows with the identical reviewed hash in 3.52 seconds. No guard is removed, no time limit is raised and no index/schema change is required. The repair bundle hash is unchanged. Cross-period and malformed-source-position negative controls are included in 97 passing local checks. Current production source correction remains held until this exact query release and remaining population/consumer checks are accepted.

Evidence: `historical-position-guard-plan-1794002198.json`, `historical-position-grouped-proof-1794002273.json`, and `reviewed-repair-production-*` receipts. The earlier full staged PostgreSQL transaction remains the source/application/rollback evidence; the follow-up changes only the equivalent guard predicate.
