# Shared reporting-currency acquisition and normalization

## Scope and status

October 17, 2026: local implementation on `codex/sec-statement-currency-evidence`, based on deployed `a5976245`. This is a shared statement reader, not a ticker eligibility list. It is not connected to production selectors or deployed. FMP remains enabled and selected for core fundamentals; the overall migration is incomplete.

`backend/app/services/sec_statement_currency.py` reads explicit reporting/presentation currency declarations, binds them to a complete SEC original, selects the taxonomy actually used by tagged financial facts, and acquires a directory-qualified primary statement or uniquely linked exhibit. It reuses each fetched original during acquisition. Functional/listing currency, bare dollar signs, past/future declarations, conflicting declarations, malformed addresses and mismatched identities do not establish a basis. An unused taxonomy namespace declaration does not override the taxonomy of actual financial facts.

The new normalization function produces Walnut's existing financial-record shape with original amounts, native currency, start/end dates, filing availability, accession and source hashes. IFRS parent income/equity and consolidated income/equity remain separate concepts. Missing NCI is not assumed zero. The reader does not convert currency, infer ADS/EPS mappings, manufacture TTM figures, or admit these observations into scores or alerts. A text declaration alone is not proof of complete/current statements or issuer-wide accounting semantics; consumer admission still requires the remaining accounting and period checks.

## Complete retained-source replay

The entire 49-symbol retained discovery cohort was evaluated with the existing three-candidate bound, network denied and exact original hashes checked. Three sources have bound declarations and repeatable normalized facts:

| Source | Taxonomy | Currency | Normalized observations |
| --- | --- | --- | --- |
| ADSE | IFRS | EUR | 29 |
| ALC | IFRS | USD | 36 |
| NAMI | US GAAP | CNY | 24 |

These 89 observations are period/concept data points, not 89 newly available product metrics. NAMI's USD convenience columns are excluded from normalized CNY values. Complete originals also retain parent/consolidated distinctions. Artifact: `artifacts/document-set-cohort-oct17/native-basis-declarations.json`; replay helper: `artifacts/audit-native-declarations-oct17.py` in the primary workspace.

Remaining candidate-attempt holds: 29 originals not retained, eight missing explicit declarations, four out-of-scope statement links, 95 missing or ambiguous labelled links, three covers beyond the existing discovery size bound. These counts are attempts, not companies, and do not prove the corresponding issuers lack financial statements. PBR/PBR-A are not admitted solely because tagged USD facts exist. No source request, production record change, email, provider switch or subscription occurred in this replay.

## Validation

Initial declaration suite: 24 Windows checks passed. Adding acquisition exposed an unused US GAAP namespace on the actual ADSE IFRS document; the shared selector now considers the taxonomy used by numeric facts and holds genuinely mixed accounting facts. The expanded acquisition/document-set suites passed 110 checks on Linux Python 3.12. Final normalization additions pass 43 checks on Windows Python 3.14.2; the final Linux Python 3.12 rerun also passes all 43 checks after canonical metric names were aligned. The initial 110 plus the final additions cover 113 distinct Linux cases. Tests cover complete ADSE/ALC/NAMI originals, exact repeats, single-fetch behavior, source tampering, current-period qualification and every normalized point's source value/currency/provenance. Production application behavior is unchanged by this local work.

## Production historical acceptance

Fresh read-only receipt `currency-guard-ordinary-1792210901.json` confirms all six supported panels now have current currency-policy caches and NAMI has its final explicit unavailable cache. Ordinary receipt `document-set-schedule-1792211117.json` attributes the remaining BOSC/NAMI/TSEM results to the 04:09 run; this must not be assumed to prove the later PR715 planner without exact release timing evidence. The general enrichment rows remain queued and are a separate backlog.

Public receipt `currency-guard-public-1792210920.json` verifies exact source-derived annual/quarterly rows for AER, ASC, BOSC, SLGL, TSEM and VLN, all HTTP 200 in 0.312 to 0.546 seconds. NAMI returns unavailable with no ambiguous old rows in 0.245 seconds. These are spot measurements, not sustained capacity or whole-product coverage proof. Earlier pending-cache notes are superseded for this seven-symbol historical-panel scope; core financial preparation is not thereby accepted.

## Next integration gates

Connect the shared native records to historical/current financial normalization only after validating complete/latest period selection, comparative history and parent/consolidated accounting semantics. Preserve native currency labels and keep USD market combinations/per-share values held without evidence. Then validate existing cache, score, monitoring/watchlist/daily/weekly alert and ranking contracts before staged activation. Broader source coverage, institutional publication, sustained load, remaining consumer checks, final Massive activation, global zero-FMP traffic and billing closure remain open. This source layer alone is not a completed replacement feed.


Final retained-cohort replay after field-name alignment preserves the same three sources and 89 normalized points, with exact repeats and zero network/production writes. CPU 26.203 seconds is offline replay cost, not a production capacity estimate. The 04:31 aggregate scheduler read (`active-work-1792211507.json`) has zero active/pending scheduler jobs; it does not mean the separate enrichment queue is empty. The unrelated frontend buildinfo SHA256 remains A185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA.
