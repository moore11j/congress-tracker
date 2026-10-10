# SEC banking revenue coverage

October10,2026. Implemented and locally validated; release/public verification pending.

## Source and behavior

Provident Financial Holdings (PROV,CIK1010470,SIC6035) has current net income and cash-flow facts but no supported total-revenue concept. The original [June2026 annual report](https://www.sec.gov/Archives/edgar/data/1010470/000101047026000010/prov-20260630x10k.htm) reports interest revenue, interest expense, net interest income before credit-loss provisions, and non-interest income separately. Its efficiency calculation uses net interest income plus non-interest income. The net banking revenue basis is therefore derived explicitly, not mislabeled as a directly reported SEC total or gross interest income.

The new fallback applies only to SEC-classified commercial/savings-bank SICs6021/6022/6029/6035/6036 with no existing supported revenue series. It combines exact `InterestIncomeExpenseNet` and `NoninterestIncome` inputs only when currency, period, accession and filed date match. Missing, ambiguous, later unmatched restatements and unsupported currencies remain missing; an explicitly reported zero is valid. Existing reported or conflicting revenue series are never replaced. Other companies retain existing behavior.

Derived inputs retain both component tags, values, accession and periods under a Walnut calculation identifier, with original companyfacts/submissions hashes in their parent evidence. Historical rows and TTM projection share the same calculation. The financial panel adds a visible revenue-basis note. No schedule, request budget, concurrency, canonical event, email or core-provider selection changes. PROV has scoped historical/ratio cache version changes; other eligible banks adopt the fallback on ordinary expiry.

## Evidence and checks

Original annual report:7,745,593bytes, SHA256 `ac3249f277dfa6d4409b8491a457cdaecee473436af084ce5c1ed30914f9adb3`. The browser fetch exceeded its content-size limit; the bounded direct-source client retrieved the original. Captured under `artifacts/direct-feeds/free-replacements-2026-10-09/prov-banking-oct10` with URL/hash manifest.

The year endingJune30,2026 has net interest income36.328million and non-interest income3.726million, yielding40.054million net banking revenue. Net income6.655million and operating cash8.970million remain independently reported; cash less PP&E purchases yields8.538million. These arithmetic metrics do not establish that a bank's cash-flow measure is comparable to an industrial company's or validate a scoring methodology.

- Python3.14.2:128 focused financial/source/cache/worker tests pass, including14 new banking tests.
- 22-company captured-source preparation/repeat:PROV recovers six metrics, other21 companies' values unchanged;49 captured reads and zero further reads on repeat, zero canonical writes/emails.
- Actual PostgreSQL16.15 isolated schema/one pooled connection:6annual/12quarterly PROV records, identical repeat, six refused concurrent attempts, no checked-out pooled connection or idle transaction during source retrieval. Source429 preserves cache and releases lock; demand-job identity/attempts unchanged. Schema removed/server stopped.
- TypeScript check passes using existing dependencies with an identical lockfile. Initial missing local compiler and an accidental skeleton insertion were corrected before the passing check. Generated build cache was restored; unchanged source-file encoding preserved.
- Eight existing financial UI checks pass from the frontend directory; initial wrong-directory invocation failed without exercising assertions. No rendered-browser or full-suite pass claimed.

## Remaining gates

Normal PR/release checks, exact runtime files, ordinary source preparation and public backend/site responses. Core remains FMP; bank-specific coverage is not universal bank coverage, IFRS support, full institutional/transcript readiness, final Massive validation or global FMP shutdown.
