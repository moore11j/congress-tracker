# Valuation request reduction and replacement boundary

October 31, 2026. Baseline production/backend source: `82b899fea11d976096d5429f857f9aa1a338f158`.

## Finding and change

The public valuation route synchronously invokes the valuation service. The frontend has its existing client cache, but the backend valuation path and FMP stable-request helper have no result cache. A service execution previously made up to14 sequential provider requests. Two growth responses, cash flow and balance sheet, were never used. Annual cash flow and balance sheet responses were fetched even when the model selected existing TTM rows.

The change removes those two unused endpoints and requests annual cash/balance only when the existing latest-TTM-row selection would fall through. Annual income remains required for the estimate horizon. Empty results, empty dictionaries, API failures and subscription restrictions retain annual fallback; a nonempty date-only TTM row retains the existing selection behavior rather than silently changing accounting semantics. Cash and balance fallbacks are independent. Global provider guard exceptions still stop acquisition immediately.

Full valuation requests now make10 calls when both TTM rows are present,11 with one annual fallback, or12 with both fallbacks. This reduces work; it does not reduce the fixed FMP subscription bill or prove a measured public latency improvement. The remaining proprietary custom DCF calls, forecasts, consensus and compatible source inputs are still replacement dependencies.

The unchanged model calculation is separated into `_valuation_model_from_rows`, allowing normalized replacement inputs to be compared without provider requests. Its FMP-shaped input contract is not itself an SEC adapter, and it does not establish period, currency, share or accounting-basis compatibility. Public output fields, formulas, assumptions, market anchor and provider selection remain unchanged.

## Validation

The Windows3.14 and Linux3.12 runs each have31passes and the same three failures in existing valuation expectations. Frozen deployed-source execution independently reproduces all three failures (14passes/3failures); they are not introduced by this cleanup. The failures concern the negative-DCF tax assumption, pre-profit margin expectation and positive-CFO asset-heavy classification. Their product/model reconciliation remains separate; tests were not weakened to hide them.

Nine new checks cover required annual fallback, failed/restricted TTM responses, independent cash/balance selection, provider shutdown at fallback and offline deterministic model calculation without input mutation. Existing shutdown tests still verify zeroHTTP when disabled. The guard call-count expectation changes from14to12 only because two unused requests are removed.

`valuation-complete-parity-oct31.json` records77complete before/after results with exact equality:13existing synthetic valuation scenarios and64fallback/sector combinations. It includes all published response fields or the complete assumptions result where that is the existing test entry point. Source baseline hash: `7dbb00722337a1c76c80dc2b4122d50703d768f8f4f430a45b9610e1a0764fd8`. These are controlled compatibility checks, not live financial coverage proof. No network, database or customer messages occur.

Receipts: `valuation-efficiency-windows-oct31.xml`, `valuation-baseline-windows-oct31.xml`, `valuation-efficiency-linux-oct31.xml`, `valuation-complete-parity-oct31.json`. An initial attempt to run tests from a partial frozen checkout collected none; the actual baseline receipt uses the exact deployed module in the complete current test environment.

## Current migration evidence

The22:25UTC read-only financial audit (`financial-capacity-1793485509.json`) finds110expired caches,101already unavailable with discovery pending,3other unavailable and6partial. This does not support calling110usable financial caches stale. The2623attempted scopes exceed a nominal20hour rotation at ten symbols/five minutes, but the cache validity is24hours; freshness needs measured coverage, not an automatic batch/CPU increase. No schedule or capacity change was made.

The22:26UTC FMP category audit (`fmp-category-traffic-1793485564.json`) still shows core fundamentals, price/history, transcripts, profile/sector and institutional permission/accounting activity, plus the valuation dependency. These counters are not confirmed successful HTTP responses. Zero-FMP readiness is not established.

## Release state

Prepared and locally checked; review, merge, guarded deployment and installed verification are pending. Core financials, transcripts and prices remain FMP. Discovery collection remains enabled with its reader disabled. Ordinary IFRS qualification and cash-flow-v3 rotation remain pending at the last recorded observation. Institutional snapshot/OneDrive transfer still awaits explicit approval; no capture or transfer was attempted. No paid subscription, email, source switch, schedule, machine size, video or landing-page change is included.
