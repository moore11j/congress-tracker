# Massive financials contract preparation, October 27, 2026

## Decision and scope

The remaining free-source coverage work continues. Massive financials is an optional final-phase candidate, not a purchased plan or selected replacement. This package adds only an offline audit script and its tests. It imports no application modules, performs no provider requests or database writes, and has no scheduled or production caller. No deployment or selector change is required.

The official [pricing page](https://massive.com/pricing), checked October 27, lists Financials & Ratios at $29/month separately from Stocks Starter at $29/month. Together that would be $58/month before applicable taxes, if later chosen. Starter alone does not include financial statements. This is not a recommendation to buy now or a claim that the add-on closes the remaining coverage gaps.

## Source contract findings

- [Ratios](https://massive.com/docs/rest/stocks/fundamentals/ratios) use a current trading-date snapshot. ROE is a fraction, requiring multiplication by 100 for Walnut's percent field. Snapshot EPS uses point-in-time converted shares and must not become Walnut's reported EPS. A price date does not identify the financial period. Net debt/EBITDA is not a supplied ratio.
- [Income statements](https://massive.com/docs/rest/stocks/fundamentals/income-statements) distinguish consolidated and common-shareholder income, weighted basic/diluted shares and TTM records. Preserve these distinctions. The documented monetary fields do not establish the issuer's reporting currency or Walnut EBITDA definition.
- [Balance sheets](https://massive.com/docs/rest/stocks/fundamentals/balance-sheets) distinguish total and parent equity, current debt and long-term debt/capital leases. Component alignment, currency and Walnut's finance-inclusive debt policy need independent verification before computing leverage.
- [Cash flow statements](https://massive.com/docs/rest/stocks/fundamentals/cash-flow-statements) preserve signed capital spending and separate total/continuing flows. Their filing date can refer to a later restatement of an older period. It is not evidence of original public availability or a safe historical backtest boundary.

## Executable local audit

Run from the backend directory:

```text
python scripts/audit_massive_financials.py response-bundle.json
```

Input is one local JSON object with `symbol`, independently verified `cik`, `observed_date` (YYYY-MM-DD), and `responses`. Response keys are `ratios`, `income-statements`, `balance-sheets`, and `cash-flow-statements`, holding the corresponding raw successful responses. Missing endpoints are explicit. Nothing reads API keys or fetches missing pages. Inputs are capped at 4 MiB and 100 rows per endpoint; pagination is refused rather than reported complete. Large histories must be captured in bounded independently identified scopes during later authorized validation.

The audit verifies issuer/share-class identity, numeric values, date/fiscal/timeframe constraints, duplicate consistency and current-snapshot cardinality. It preserves original field names for income, equity, cash flows and EPS; candidates for five Walnut ratio fields remain separate. It records a canonical response hash and observation date. Currency, accounting basis and original availability remain unverified. Stale and empty results are explicit. Every output sets `publicationEligible=false`.

This is a response-contract checker, not a live adapter, cache warmer, full comparison to FMP or production integration. The tests use synthetic cases shaped by the official schema, not live paid responses. A passing result establishes structural compatibility only.

## Verification and remaining acceptance

Windows Python3.14: 34 checks pass, including CLI roundtrip, duplicate JSON keys, wrong ticker/CIK, invalid dates, nonfinite/boolean/string values, duplicate conflicts, stale/empty results, pagination/denials, signed cash flows and non-equivalent EPS. The same34 checks also pass on Linux Python3.12.3; these are34 distinct tests, not68. Git staged whitespace checks pass. No unrelated application tests are needed for an isolated offline script with no runtime imports.

Before any purchase decision, retain the free-source coverage audit and identify which fields actually need this option. After explicit subscription authorization, capture a bounded cross-section of currently missing symbols and ordinary domestic/share-class cases. Independently reconcile currency, basis, periods, debt/leases, consolidated/common income and restatements against originals and existing Walnut contracts. Only then design a gated production reader/cache integration and replay scores, alerts, digests and rankings. Do not treat this script's candidate field names as authorization to populate canonical data.

## Concurrent production observation

Read-only `artifacts/discovery-observation-1793095550.json` at10:05UTC confirms collection1, reader0, coreFMP and the10:01 ordinary SEC worker completing10/10 scopes. No discovery checkpoints exist yet; the selected ten scopes did not reach a continuation candidate. NBIS remains ordinarily due21:41UTC. This is scheduled worker recovery, not live discovery/source/public-reader acceptance. Production remainsb5633d10; no forced refresh, publication, subscription, schedule or resource change.
