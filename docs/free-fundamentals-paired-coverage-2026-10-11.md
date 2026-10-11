# Free fundamentals paired coverage, October 11, 2026

Read-only production comparison at22:33UTC covers all1,751 symbols attempted by the existing warmer, each with a successful FMP cache baseline. This is field presence, not numerical/methodology equivalence or approval for a provider switch. Public core fundamentals remain FMP. Massive remains the final subscription/price step. No newly required paid plan is established by this audit.

## Method and limits

Fresh, correctly versioned SEC and Finnhub preparation caches are checked against their actual source and symbol. Finnhub observation time must match its cache timestamp. The deployed pure combination function enforces reviewed issuer identity and reporting-period rules, with SEC values preferred. Verified SEC issuers without supported current revenue may still receive independently attributed Finnhub metrics, matching the deployed fetch fallback. The audit excludes market-derived fields and possible extra issuer cash-flow supplements for unsupported SEC ratio cases, so absence here is not proof that a complete future fetch could never fill a field. No customer data is exported; only aggregates and named public-company examples are retained. SQL is read-only with a three-second statement timeout.

The first diagnostic incorrectly added a version suffix to Finnhub cache keys, so paired-free-coverage-1791757793.json is invalid for Finnhub coverage. The next result was limited to SEC-supported ratio scopes. The final corrected receipt is paired-free-coverage-1791757982.json; earlier counts are superseded.

## Current coverage

1,095 fresh SEC ratio scopes are usable;255 additional issuers are identity-verified but lack a complete supported SEC trailing-revenue period. A total1,225 scopes combine usable Finnhub data. Remaining SEC preparation conditions:380 directory absences,14 companyfacts404s,three expired caches,three missing caches and one identity/version mismatch (ETR). The380 absences require instrument/lifecycle classification; they are not automatically subscription gaps. Within verified SEC scopes,45 Finnhub caches report unavailable and80 are missing after the three targeted preparations.

Among comparable periods,975 SEC periods are newer than cached FMP,45 equal and two older: ACN May31 versus August31,2026; AZO May9 versus August29,2026. Diagnose these before accepting freshness. Period equality alone does not prove equal formulas or data values.

| Field | Present in FMP | Present in free preparation | FMP presence retained | FMP presence missing | Newly present |
|---|---:|---:|---:|---:|---:|
| market_cap | 1650 | 0 | 0 | 1650 | 0 |
| price | 1715 | 0 | 0 | 1715 | 0 |
| volume | 1715 | 0 | 0 | 1715 | 0 |
| avg_volume | 884 | 0 | 0 | 884 | 0 |
| beta | 321 | 0 | 0 | 321 | 0 |
| dividend_yield | 0 | 0 | 0 | 0 | 0 |
| trailing_pe | 1363 | 912 | 844 | 519 | 68 |
| forward_pe | 0 | 995 | 0 | 0 | 995 |
| price_to_sales | 1366 | 1184 | 1114 | 252 | 70 |
| ev_to_ebitda | 1366 | 987 | 922 | 444 | 65 |
| gross_margin | 1366 | 1043 | 979 | 387 | 64 |
| operating_margin | 1366 | 1208 | 1135 | 231 | 73 |
| operating_margin_expansion | 1319 | 854 | 801 | 518 | 53 |
| net_margin | 1366 | 1214 | 1139 | 227 | 75 |
| roe | 1360 | 1152 | 1077 | 283 | 75 |
| roic | 0 | 1128 | 0 | 0 | 1128 |
| revenue_growth | 1366 | 1180 | 1105 | 261 | 75 |
| eps_growth | 1366 | 824 | 760 | 606 | 64 |
| ebitda_growth | 1366 | 0 | 0 | 1366 | 0 |
| free_cash_flow | 0 | 764 | 0 | 0 | 764 |
| fcf_yield | 1353 | 0 | 0 | 1353 | 0 |
| fcf_margin | 0 | 762 | 0 | 0 | 762 |
| fcf_growth | 6 | 593 | 2 | 4 | 591 |
| debt_to_equity | 0 | 1130 | 0 | 0 | 1130 |
| current_ratio | 1366 | 1150 | 1081 | 285 | 69 |
| net_debt_to_ebitda | 1366 | 0 | 0 | 1366 | 0 |
| eps_ttm | 0 | 0 | 0 | 0 | 0 |
| earnings_yield | 1363 | 0 | 0 | 1363 | 0 |

Market data and market-dependent yields are intentionally absent from this preparation audit. Existing SEC/Massive code supports independently verified price, volume, average volume, market cap and cap-based yields, but remains gated and unvalidated for final selection. Net debt/EBITDA and EBITDA growth have no current free-field output here despite1,366 FMP rows containing each. Beta also remains absent. They need real source/formula work or an explicit retained-feature decision; do not silently substitute zeros or delete product fields. Some FMP rows also have missing fields, so FMP is a comparator, not assumed ground truth.

## Reviewed issuer caches now verified live

Runtime hashes for finnhub_metrics, finnhub_metric_identity, free_fundamentals and sec_fundamentals match the managed release. BRK-B, TSM and CX attempts at00:06/00:26UTC predated the identity repair; their caches were absent. Three bounded normal prepare calls through the existing shared free limiter at22:32UTC now create source-isolated caches, each containing eight permitted issuer ratios. Berkshire adds five previously missing fields; TSMC and Cemex each add eight. Each immediate repeat returns identical cached payload/timestamp. The alternate security symbols BRK.A,2330.TW and CEMEXCPO.MX remain explicitly identified. No price, valuation or per-share fields are transferred across these associations. Public provider remains FMP. Receipts metric-identity-runtime-1791757898.json and reviewed-metric-preparation-1791757939.json.

## Remaining acceptance

Resolve ETR cache validity, ACN/AZO freshness and source/instrument absences; implement missing retained financial fields with source evidence; measure numerical/semantic differences; then run complete score, ranking, monitoring, daily/weekly and watchlist rebaseline checks. The19 period-based field holds remain truthful (ROE1, ROIC8, debt/equity8,current ratio2:19 total); no lagging series was used to fill a newer SEC period. No institution or core provider activation, customer email, capacity change, purchase or cancellation occurred.

### Follow-up diagnostics at22:34UTC

ETR retains an unavailable v3 preparation from October10,22:36, although its latest v4 attempt was October11,13:28. This is an unsuccessful refresh, not proof of an issuer mismatch. ACN's May31 period cache was observed October10,23:21; its October11,14:26 attempt did not change that observation. AZO's May9 period was freshly observed October11,10:23, so cache age alone does not explain its older period. Receipt core-freshness-gaps-1791758071.json. Investigate actual filing availability and supported facts before alleging wrong vendor data or discarding the dates.

Code inspection also identifies a freshness-scheduling risk: SEC warming records the latest attempt time even for a cache hit, while priority refresh uses20hours since that attempt and preparation accepts caches under24hours. A near-expiry hit can therefore postpone the priority refresh beyond cache expiry. Finnhub already preserves actual cache observation timestamps. This SEC behavior is a repair candidate requiring a no-starvation/no-request-storm test; no change has been made in this audit.

Captured Finnhub responses from the earlier reviewed-issuer probe contain beta, annual/quarterly EBITDA series, and net-debt/equity ratios. These are candidates for further free-source work, not already equivalent replacements: five-year EBITDA CAGR is not annual growth, quarterly EBITDA is not TTM EBITDA, and ratios cannot be combined without checking units and aligned denominators. Beta and per-share/valuation inputs from alternate securities remain excluded. No paid subscription is justified merely by the current adapter omitting these fields.
