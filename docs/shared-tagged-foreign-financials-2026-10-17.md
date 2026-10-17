# Shared tagged foreign financial statements, October 17, 2026

## Source inventory and common repair

The retained124-scope foreign/transition inventory contains1,900 recent6-K submissions across123issuers. A6-K form or recent filing date alone is not proof of a financial reporting period. `artifacts/shared-financial-cohort-oct17/interim-source-inventory.json` retains the complete public metadata inventory.

The shared selector matches standard entity-wide USD US GAAP duration facts and balance facts to the same SEC submission accession, filing date and report period. It then fetches and independently parses that exact original before enabling its qualified foreign fact history. No symbol allowlist, filename convention or per-company amount pin is used. Existing reviewed foreign/IFRS and domestic paths retain priority. Current periods must remain within180days; unknown currency/taxonomy, ambiguous filings, unqualified newer current facts, source mismatches and event-only filings cannot authorize replacement values.

The same rule finds qualifying history for50of124scopes and current source candidates for six: EDU,ESEA,FLNG,ICLR,NMM,SBLK. Complete originals for all six consist of12companyfacts/submissions and11current/annual statement documents, retained as23compressed fixtures with SHA256/URL receipts. One shared English date-transform correction accepts nonnumeric separators without requiring a comma, resolving an actual NMM original. The rule follows the [XBRL transformation guidance](https://www.xbrl.org/guidance/ixbrl-tagging-features/) and [TRR5 specification](https://www.xbrl.org/Specification/inlineXBRL-transformationRegistry/REC-2022-02-16/inlineXBRL-transformationRegistry-REC-2022-02-16.html); unsupported/invalid dates remain typed holds.

## Coverage and semantic limits

`artifacts/tagged-interim-oct17/cohort-comparison.json` compares the entire239held-source cohort against deployed540c6497 using identical complete companyfacts/submissions, adding exact retained originals for newly qualified foreign paths. Six scopes recover42metrics; the other233results are identical. The six public statement projections contain36annual rows and26quarterly rows. Zero quarterly rows for EDU,FLNG andSBLK is retained honestly; half-year figures are not relabeled quarterly. Standard foreign per-share EPS is withheld until a listed-share/ADS basis is verified. Native-currency IFRS, untagged earnings releases and more recent untagged filings remain separate coverage work; this does not establish latest-all-source completeness.

Current metric recovery: EDU9, ESEA6, FLNG6, ICLR9, NMM3, SBLK9. All source values retain original context/provenance. Complete current revenue inputs are compared to parsed original current/annual amounts in tests. The historical SEC reader remains partial and forecasts remain unavailable. Current core fundamentals stay FMP. Existing TTL/schedules govern gradual preparation; no universe cache reset, new schedule, resource change or paid service is introduced.

## Validation state

Initial expanded run:200pass/two failures due to older transition-test fixtures lacking the now-required primaryDocument directory array. Complete fixtures corrected without relaxing production validation. Final expanded Linux Python3.12 run passes265tests, including twelve complete-source score/watchlist lifecycle cases, in62.07seconds. Only two existing FastAPI lifespan deprecation warnings remain.

A32file verifier rehearses successfully on WindowsPython3.14.2 using complete sources, actual acquisition, isolated preparation/cache repeats and historical panels;13.688CPU seconds, no network/production reads/writes/emails. Final installed runtime verification must follow an exact reviewed release. No merge or deployment of this change yet. Branch codex/sec-interim-discovery.

Previous PR710's new ALOY/BCAX ordinary refresh remains pending in receipt1792203457; priorAEVA/ALLE ordinary recovery is already verified. Full financial coverage, institutional publication, transcripts/analysts/valuation, all monitoring/watchlist/daily/weekly/ranking consumers, sustained capacity, finalMassive, zero-FMP traffic and billing remain open.
