# Source-verified Form 4 owner distinctions, October 15, 2026

## Actual held population

After the publisher backlog repair, production has304held receipts. A read-only audit at23:01 finds282 held for unmapped existing events; every matching event is a normalized SEC transaction with a different reporting-owner CIK. There are798 source-row/event comparisons. The remaining22 recorded holds are18 unnormalized provider candidates and4 cross-accession owner/date candidates. They are not authorized for publication by this repair.

This establishes a concrete overly broad condition: the old publication guard treats any event for the same ticker and transaction date as a potential duplicate, even when complete SEC evidence establishes different reporting owners. Owner-specific identity already includes reporting-owner CIK in the canonical transaction hash and reconciliation contract.

Artifacts: form4-event-holds-1792105266.json and1792105353.json. An initial read-only audit used a reserved SQL alias and failed; the corrected query succeeds. No source payload or customer data was exported by these database audits.

## Independent originals and guarded repair

Five public SEC originals were fetched serially through the existing identified source client. They reproduce three source pairs involving PROV, different single reporting owners, and identical trade dates. Every original passes complete-source parsing. Their October7filing dates are distinct from historical transaction dates, including2024; publication preserves actual arrival time rather than backdating monitoring/trading access.

- https://www.sec.gov/Archives/edgar/data/1685789/0000939057-26-000263.txt SHA256 `939bc6305cc6dddcac2ae81c28c61a8dcc15f8bd211dcd707e1ab3da9d9e5044`.
- https://www.sec.gov/Archives/edgar/data/2055628/0000939057-26-000262.txt SHA256 `76e358fe6eb25093a51525275f4440a9775e258cfeb80b348320180974c55881`.
- https://www.sec.gov/Archives/edgar/data/1180038/0000939057-26-000265.txt SHA256 `e5f6b33917aa635a218ef9312fbe6192dde658f6c2ca6f053f8ca8c76877b512`.
- https://www.sec.gov/Archives/edgar/data/1685789/0000939057-26-000266.txt SHA256 `044213d24d218a4b13d9d005e89ae209a0ef7f8a22bf36600b4e39caa66dd151`.
- https://www.sec.gov/Archives/edgar/data/1333283/0000939057-26-000264.txt SHA256 `e5ecba5995799aaaf1580a878c478b3af82dd46bfebaeac49c306a093cb2196c`.

The new predicate distinguishes an existing event only after validating its retained transport checksum, official accession/URL, current parsed staging, published receipt, metadata hash, complete original parse, full normalized-row fields, canonical filing/XML, event/source references and every original field copied into the event payload. Missing, changed, ambiguous or legacy evidence remains held. Same-owner cross-accession and unnormalized-provider checks are unchanged.

Parsed original evidence is cached only within one atomic filing projection, bounded by32sources and8MiB of captured-source byte weight. This is not an RSS claim. Oversize evidence is still verified without caching; eviction never truncates rows or turns valid sources into failures. No cache survives the transaction. Publication limits, timing, source ownership, transactions, event identities and alert delivery rules remain unchanged.

## Complete read-only production predicate proof

Eight bounded read-only transactions evaluate all282 current held filings, revalidating each current source original and798 existing-event comparisons with the proposed predicate. All282/798 pass. A final read-only cohort reconciliation confirms all eight ID/source-hash groups still match the same unchanged282-source population, avoiding an offset-scan coverage assumption. No publication, writes, provider requests, source-payload exports or customer exports occur. The proof is for the false-hold predicate, not a claim that every other publication gate is satisfied or that the historical ledger is economically duplicate-free.

Final receipt: artifacts/form4-owner-cohort-1792105870.json; per-batch form4-owner-proof-{offset}-*.json. Predicate LF-normalized SHA256 bca1b93e34d9a94aabe838f455e0aafe098201135e686691cf277babd1e70705. Total measured predicate-process CPU4.805seconds across eight batches; this excludes full publication, downstream enrichment and sustained capacity.

## Tests and release state

The new positive different-owner case fails against the old code while17 safeguards pass. Repaired expanded run104passes on LinuxPython3.12.3; final25 focused evidence checks pass, giving111 distinct selected checks across the runs. Coverage includes same-owner holds,16 evidence faults, original reuse, bounded cache/eviction, read-only classification, corruption after prior proof, original reparse despite a changed hash, canonical repeat, alerts and arrival dates. This is not a full-suite pass.

Complete five-original isolated publication on WindowsPython3.14.2 produces five distinct canonical records, preserves all source originals and IDs/timestamps/payloads on repeat, and sends no requests or emails. The installed-runtime verifier rehearses six hashes, the same complete originals, canonical repeat, tamper refusal and rollback under autoflush=False and in-memory SQLite. Helpers: replay-form4-owner-originals-oct15.py and verify-form4-owner-runtime-oct15.py; source manifest/replay are in artifacts/form4-owner-originals/.

Local repair at this checkpoint. Exact reviewed release, installed verification and ordinary publication remain required. PR691 has already demonstrated200ordinary attempts/243events; PR692's collector repair is deployed but next ordinary discovery remains unverified. FMP stays enabled and core-financial/institutional/transcript/all-consumer/sustained-capacity/final-Massive/paid-provider/zero-FMP/billing gates remain. Previous turn's two deployments and current actual source proof/repair are concrete progress; goalactive. No source selector, subscription, hardware, schedule, manual publisher run, customer test email, video or landing change.
