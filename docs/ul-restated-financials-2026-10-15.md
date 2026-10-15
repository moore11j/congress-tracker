# Unilever restated financial replacement, October 15, 2026

## Scope and source reconciliation

Unilever's old first-half comparison includes the Ice Cream business. Combining it with the current continuing-business statements would give an invalid revenue and operating-margin comparison. The replacement uses the full current SEC 6-K, annual SEC 20-F and Unilever's explicitly restated half-year PDF. It checks every common 2025 first-half income line against the current filing before constructing trailing periods.

Reviewed originals:

- Current: https://www.sec.gov/Archives/edgar/data/217410/000021741026000057/ul-20260630.htm, filed July 28, 2026, period June 30, 2026. SHA256 4f6be2d563061a80c34c691eee64a0874c4ca1a6ff034f17cde9e58194774188, 2,092,905 bytes.
- Annual: https://www.sec.gov/Archives/edgar/data/217410/000021741026000007/ul-20251231.htm, filed March 12, 2026, period December 31, 2025. SHA256 6b7a85d845cd8874ef8ddc674016ab8f693f7443d41fd19e0e4ddb2a01d5659c, 13,935,127 bytes.
- Restatement: https://www.unilever.com/files/unilever-half-year-historical-financials-excluding-ice-cream-pdf.pdf, published March 12, 2026. SHA256 edddb9fb8ac446e216ccf7dbcc193ca9484600999764f9651afc7917d1e4de93, 179,697 bytes/seven pages. Page two was rendered and visually checked against extracted 2025/2024 columns.

Complete SEC directory and all six later 6-K covers were captured and inspected. They are shareholding/voting-capital notices, not later financial statements. Their exact accession/form/filing/report/document identities and original hashes are recorded in the adapter and artifacts/ul-financials-oct15/later-covers.json. New or amended financial disclosures, changed source bytes, ambiguous issuer/period identities and stale periods hold preparation. This is reviewed current-period support, not proof of unattended future-report rollover.

## Values, definitions and consumers

Seven EUR-based values: revenue growth -1.6667314192%; operating margin 18.0956143817%; operating-margin change +2.0887765092 percentage points; net margin 18.3188463058%; current ratio 0.7403343602; ROE 54.5647121127%; cash-flow margin 14.2078229949%.

Revenue and operating income use continuing operations excluding Ice Cream. Net margin and ROE use total parent income including discontinued operations and disposal gains; they are not recurring profitability. Cash-flow margin uses continuing operating cash flow after tax less gross PPE purchases; IFRS interest is classified in financing, intangible purchases are excluded, and this is not issuer-adjusted FCF. No USD free-cash-flow amount, USD valuation ratio, ADR EPS, EBITDA/net debt or missing growth is inferred. These semantic limits remain part of the broader cutover assessment.

Source evidence now carries bounded display metadata. The existing summary and ticker component show `SEC EDGAR + Unilever` with the disposal/cash-flow qualifications; genuine Finnhub supplementation retains its own label/note. Existing FMP/other SEC labels and scores remain unchanged. The provider engine stays sec_edgar, with the issuer PDF distinguished in provenance. Only the exact reviewed issuer URL is temporarily permitted by the source client; the original allowlist is restored even on errors.

Preparation uses the existing scheduler and a UL-specific cache version. It does not alter public provider selection, canonical financial rows, schedules or alert thresholds. Tests preserve the existing summary's fractional ratio contract, while cache fields retain percentage values.

## Validation

Full captured-original replay passes Windows Python 3.14.2 and Linux Python 3.12.3, with network blocked and FMP disabled. Seven metrics, all source hashes, exact cache/projection repeats and three usable scored inputs are verified. Preparation creates zero canonical financial rows. Missing-to-restored data initializes the watchlist baseline with zero false alerts; a later synthetic threshold crossing creates one alert, repeat creates zero duplicates, and rollback creates zero alerts. No emails are sent. Twenty saved issuer controls remain identical to deployed d4e95e7f. Receipts: artifacts/ul-financials-oct15/full-replay-win32.json and full-replay-linux.json.

Focused tests cover source tampering, periods/currency, line reconciliation, restated comparisons, ambiguous numbers, bounds, new/amended reports, allowlist restoration, USD-market-cap isolation, cache repeat and source disclosure. Initial synthetic HTML encoding and new summary ratio assertions were corrected to the established contracts; no production formula was changed to satisfy them. Final focused and affected selection: 342 passed on Linux Python 3.12.3, including supplemental Finnhub identity and metric compatibility.

The isolated installed-code probe has been rehearsed locally. It verifies five installed hashes, seven synthetic values, preparation/repeat, exact issuer URL, source qualification and changed-source/new-report holds. Its PDF extraction fixture is synthetic; actual complete PDF parsing is covered by the separate full-original replay. No production record reads/writes or provider requests are needed for this probe.

## Release and remaining gates

Local implementation; exact PR, deployment, installed verification and ordinary preparation are pending. The previous transcript repair PR684 is merged as d4e95e7f; its verified release receipt is in the primary checkout's SEC cover report. A fresh scheduler read (artifacts/issuer-queue-1792097662.json) still shows the earlier failed issuer run and no active/pending jobs; workers are alive. No postrelease scheduled collection claim is made and no timestamps/jobs were forced.

Core financials and transcript publication remain on FMP, institutional publication remains disabled. Broad financial/transcript coverage, institutional/current-cohort verification, every scheduled consumer and sustained performance, paid Finnhub and final Massive validation, zero global FMP requests and billing retirement remain open. No new subscription, hardware, schedule, customer email, video or landing change.
