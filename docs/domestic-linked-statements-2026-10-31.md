# Domestic linked financial statements, October31, 2026

## Confirmed acquisition gap

Read-only production metadata at23:43UTC finds CLX still retaining an unavailable October10 preparation, while ARCC and DMRA retain October14 unavailable preparations despite later attempts. These are stale failed refreshes, not successful newly prepared statements. Six-case receipt: `financial-stalled-metadata-1793490199.json`.

Fresh complete public SEC sources reproduce three distinct failures: CLX reaches `latest_reported_revenue_unavailable`; ARCC's current filing exceeds the20MB capture guard; DMRA reaches `conflicting_reported_facts`. The latter two remain held and are not repaired by this package. No source-policy or size-limit relaxation is included. All successful original responses and hashes are retained in `artifacts/stalled-acquisition-oct31/sources.json`; the oversized ARCC body was not retained as a complete source or used in a projection.

Clorox's August7,2026 [10-K primary](https://www.sec.gov/Archives/edgar/data/21076/000002107626000034/clx-20260630.htm) contains the filing identity and shared XBRL contexts/units. Its Item8 and exhibit index explicitly point to the same-accession [financial statement exhibit](https://www.sec.gov/Archives/edgar/data/21076/000002107626000034/clx-20260630_d2.htm). The primary has only two numeric DEI facts; the financial facts reside in the linked exhibit. The old domestic acquisition stops at the primary and throws instead of reading the statements.

## Shared repair

The domestic10-K/10-Q acquisition can follow exactly one explicitly labelled financial-statement link after the primary passes identity/namespace validation but lacks current supported numeric facts. Ordinary complete filings keep their existing parsing and request behavior. Conflicting or malformed facts cannot trigger the fallback. The filing directory must uniquely bind issuer, symbol, accession, form, date and primary URL. Both originals remain within the same accession and current combined20MB size limit; the acquisition retains its six-document cap.

The document-set parser retains all context, unit, namespace, currency, transformation and conflicting-fact checks. Primary accounting numeric facts are disallowed in this path to prevent attributing cover values to the exhibit. The existing foreign6-K path is preserved. Cached domestic evidence binds the directory hash, both original hashes, exact URLs and statement label; missing or mismatched proof is rejected. No issuer allowlist, new schedule, host, subscription or public provider selector is added.

The complete Clorox original set produces55 reported facts and recovers eight supported metrics for June30,2026: gross margin42.3214285714%, net margin8.7351190476%, revenue growth-5.4054054054%, current ratio0.6577713668, ROE285.6447688564%, free cash flow405million USD, FCF margin6.0267857143% and FCF growth-46.7805519054%. Missing operating-margin expansion and net-debt/EBITDA remain unavailable. The public panel remains partial; this does not prove a complete financial classification or full migration acceptance.

## Evidence and release gates

Four complete public originals are pinned in `backend/tests/fixtures/sec_domestic_document_sets`. The exact baseline58c1309b fails on the primary; the candidate uses two originals and repeats identically. Existing complete ADSE/AER/ALC parses retain all output fields and23/37/30 facts. Local candidate acquisition plus core/panel projection uses4.672 CPU seconds; this is not production capacity proof. Receipt: `domestic-linked-proof-oct31.json`.

Initial Windows focused run:66pass, one test failure caused by expecting ValueError instead of the existing DirectSourceError for a wrong symbol. The guard correctly rejected the identity; the test expectation was corrected. Final Linux regression and additional source-to-score/watchlist lifecycle checks are running. No passing full-suite or deployment claim at this checkpoint.

Next exact-source review, guarded release, installed proof and ordinary recovery. Core financials/transcripts/prices remainFMP, discovery collection1/reader0. All retained consumers, broad coverage, sustained performance, current institutional validation and final Massive/zero-FMP/billing closure remain gates. The institutional snapshot/OneDrive approval question is still pending; no capture or transfer has been executed.
