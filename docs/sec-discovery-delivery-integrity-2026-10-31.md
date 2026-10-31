# SEC discovery delivery integrity, October 31, 2026

## Observed blocker

Fresh read-only production evidence at19:54 UTC shows171 discovery checkpoints with2205 observed covers:2008 qualifications pending,155 no-supported-statement,39 held and3 qualified. Four latest scheduled batches completed10/10, but BP and IAG surfaced only the generic DiscoveryError. All171 stored checkpoint receipts validate in the installed reader; this rules out corrupt persisted receipt structure in that sample. Sources: `artifacts/discovery-observation-1793476457.json` and `artifacts/free-recovery-1793476630.json`. Core remains FMP and discovery reading remains disabled.

The first unqualified cover for each of BP,IAG,NBIS was retrieved once from its exact official SEC URL, with no redirects, retries after denial or requests beyond three. All three differ from the stored raw hash. Only NBIS has a retained older complete original for an exact byte comparison here. That original's raw hash matches the production cover pin. The sole changed line is the SEC terminal delivery-script path; the existing strict document fingerprint is identical. This establishes the mechanism for NBIS; it does not independently prove that every byte difference for BP or IAG is harmless.

NBIS old raw SHA256: `8e35430b9a7b2befe783c5de1e51528be08b6b09ca0841df60bc60bc27bbeffe`,8478bytes. Current raw SHA256: `058e1e1657fd0078ced08923da1e37134ab2684b28271efa295e3a64857d6be9`,8477bytes. Exact current official cover: https://www.sec.gov/Archives/edgar/data/1513845/000110465926112824/tm2626792d1_6k.htm . Captures and metadata are under `artifacts/discovery-covers-oct31/`; a compressed complete current cover is added as a regression fixture.

## Shared repair

Reuse `sec_original_integrity` policy `sec_terminal_delivery_script_path_v1`. It substitutes only the reviewed terminal script URL path when hashing; financial text, tags, units, links, whitespace, other scripts and document identity remain byte-sensitive. New cover and qualification receipts retain both actual raw hashes and the restricted document fingerprint when the exact script shape exists. Unknown shapes keep raw-only verification.

Legacy unqualified cover hints are re-observed in a separate existing worker phase before any qualification. This phase consumes two cover reads at most and admits no financial values. Raw-identical legacy covers are also upgraded when the reviewed script shape exists. Known document-fingerprint changes cannot enter this legacy repair path. Denial preserves the prior checkpoint; atomic compare-and-swap and all size/source/identity limits remain.

Qualification version advances fromv4 tov5, so previous qualifications are revalidated rather than silently grandfathered. Full selected-original restoration checks the same restricted fingerprints. Semantic receipt comparison tolerates only raw hash/byte-length differences already proven to be terminal-path-only; actual fetched provenance remains in returned source evidence. No financial calculation, source selection, schedule, resource allocation or publication boundary changes.

## PDF dependency

PR743's October31 backend audit exposed seven advisories in unchanged `pypdf==6.21.2`; CI lists6.21.4 as covering all seven. This ingestion repair pins6.21.4 and verifies the actual installed test version. No advisories are ignored. Complete PDF parsing and Meta source-to-consumer replay are required before release. Dependency audit reference: GitHub run41236539855,job124641404057.

## Validation and release state

Final source47b9c6b5 passes275 affected Linux checks on Python3.12.3 with pypdf6.21.4. Final focused Windows checks pass11/11; the earlier39-case Windows run predates the last legacy-upgrade refinement, so the final Linux and focused Windows receipts are authoritative. Counts overlap and are not additive unique coverage. Regression coverage includes a complete15-filing NBIS discovery cycle, raw-path rotation at qualification and restoration, stable-versus-rotated projected-field parity, malformed proof rejection, known content-change holds, denied legacy refresh and idempotent repeats.

The exact pypdf6.21.4 Meta replay retains the complete original PDF/text hashes,56,989characters, one document and unchanged full-database repeats in both new and historical-source cases, with zero evidence events, emails, network calls or production writes. Linux runtime path/version was independently confirmed. Final receipts: `discovery-delivery-linux-oct31.xml`, `discovery-delivery-final-windows-oct31.xml`, `meta-index-oct27/consumer-replay-win32-oct31.json`; source/test manifest: `artifacts/discovery-delivery-manifest-oct31.json`.

PR744 security run41237218031 passes both dependency audits and secret scan on47b9c6b5; Vercel preview also passes. Manual source review and diff checks are complete; no automated Codex-review approval is claimed. This is pre-release evidence, not a production deployment or cutover claim. App attachment was attempted once and hit the existing100-identity limit: https://github.com/moore11j/congress-tracker/pull/744 .

Release must retain core/transcript/price FMP selectors, discovery collection enabled/readers disabled, the two-service configuration and existing worker bounds. Verify installed source hashes/pypdf version, normal scheduled recovery and public response health after any guarded deployment. A qualified filing is not proof of complete issuer selection or broad financial replacement. No forced refresh or source deadline bypass is part of this change.

## Other recovered acceptance evidence

At19:57 UTC installed readers show Meta Q3 prepared from the normal October28 08:13 collection, with the exact tested PDF hash `980c5e766b36f06c4d3d22b543cc35714da05c0abe400b60d36199f0c90759da` and text hash `3227be9e9e9be4cabead95dfaeebce246fa39cfedf9457c80f9eabc2f68bb8a4`. The Meta index refreshed normally October31 14:13; IBM Q3 remains prepared. This closes the previous ordinary Meta-recovery observation gate, not transcript coverage or public cutover. Same read-only receipt as above; zero provider calls or production writes in that observation.

Separately, draft PR743 preserves tested default-off Massive price-core reconciliation on0eb5020c. It is not deployed or activated; all public-caller, schema, selected consumer and subscription gates remain. Its app attachment attempt hit the existing100-identity limit; use https://github.com/moore11j/congress-tracker/pull/743 .
