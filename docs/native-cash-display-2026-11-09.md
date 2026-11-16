# Native-currency cash flow compatibility

The ticker overview previously displayed only the canonical USD free-cash-flow field. Qualified non-USD amounts already retained in SEC calculation evidence stayed missing. The prepared change exposes those amounts as display-only context with explicit currency, TTM basis and reporting period. It does not populate USD financial fields, convert currencies, alter source selection or change scoring, valuation, screening and alert inputs.

The summary reader requires a selected SEC/free-direct row, an absent canonical cash amount, matching reviewed currency and reporting period, a finite numeric amount and an explicit TTM calculation. Conflicting, unknown, malformed or incomplete evidence remains unavailable. The frontend shows the currency code and retained calculation; legacy USD presentation remains supported. Public projection preserves this safe context while continuing to remove internal provider fields.

Validation before release: 97 focused source/cache cases pass, followed by 39 display/public/alert cases including exact Confirmation Score equality. Fourteen frontend rendering/fetch cases and TypeScript checking pass. Retained actual acquisition outputs for 49 scopes produce identical existing summary fields and an exact repeat; that cohort contains no new native display amounts. Separate complete retained Enbridge and Novo Nordisk source tests verify CAD 1.668 billion and DKK 77.859 billion respectively, including unchanged canonical USD fields. These are dated fixtures, not current live balances.

The bounded read-only production preparation audit at 2026-11-09 02:58 UTC found three native cash evidence records across 2,620 caches. Two have complete current display metadata: ENB (CAD, September 30) and BIRK (EUR, June 30); one remains held. This is metadata coverage only. It does not establish complete current source-reader qualification, public source activation or a reduction in the 737 classification gaps.

Separate gap triage identifies 134 of those classification gaps as financial-industry policy holds, 93 as incomplete/nonpositive EBITDA and eight as unverified finance-inclusive debt. No accounting-policy relaxation is included.

Production remains backend PR804 and frontend PR806 until a release receipt is added. Core fundamentals remain FMP-selected. No schema, provider, schedule, subscription, billing or customer delivery changes are part of this package.
