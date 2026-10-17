# Remaining revenue coverage diagnosis, October17,2026

Seven complete public companyfacts/submission captures reproduce current coverage holds for AB,ADTX,AEVA,ALLE,ALOY,AMH andANAB. Originals andSHA256 provenance are in artifacts/revenue-holds-oct17/manifest.json; diagnosis.json and six per-symbol revenue-series files retain the local replay. No production records were exported or changed, no provider purchased and no emails sent.

These holds have different causes. They must not all be treated as companies without revenue or repaired by filling missing values with zero.

| Case | Source-backed finding | Required next step |
|---|---|---|
| AEVA,ALLE | Reader selects Revenues at the latest period, but that series has no complete annual basis. RevenueFromContractWithCustomerExcludingAssessedTax has current and annual coverage. Both concepts have identical four quarter/YTD comparisons in each current original10-Q. | Implement a reviewed concept-equivalence rule with complete annual/current source proof, period/identity/conflict guards and consumer parity. Do not globally prefer a component merely because it has more history. |
| ALOY | Current excluding-tax concept lacks annual history, while older including-tax concept has a2025annual. | Establish meaning, tax basis and any business/reporting change from originals before bridging concepts. No equality assumption. |
| ANAB | Recent submissions include10-KT for2026-06-30 filed2026-09-21; current form rules omit that transition report. | Review complete transition statement and fiscal-period alignment before accepting10-KT. March coverage is stale under current policy. |
| ADTX | Latest captured periodic report is2026-03-31 filedMay20, outside180day freshness. | Preserve stale hold; investigate newer permitted issuer filings separately. Do not relabel old financials fresh. |
| AB,AMH | Captured standard revenue series absent/currently stale despite current income/cash facts. | Review full statements and holding-company/rental revenue semantics. Missing generic tags are not proof that revenue is zero. |

Four complete AEVA/ALLE originals pass the existing inline parser: current10-Q and2025annual10-K for each. Statement manifest retains identity, URLs, report/filed dates and hashes; statement-revenue-proof.json retains dimensionless parsed facts and provenance. Current quarter/YTD totals match both concepts exactly. Complete same-concept annual-plus-current-YTD-minus-prior-YTD calculations yield AEVA21,598,000USD andALLE4,288,500,000USD, before any code change. This is source diagnosis, not a released adapter, authoritative financial advice, activated public value or all-consumer acceptance.

AEVA quarterly source: https://www.sec.gov/Archives/edgar/data/1789029/000119312526338179/aeva-20260630.htm
AEVA annual source: https://www.sec.gov/Archives/edgar/data/1789029/000119312526116518/aeva-20251231.htm
ALLE quarterly source: https://www.sec.gov/Archives/edgar/data/1579241/000157924126000028/alle-20260630.htm
ALLE annual source: https://www.sec.gov/Archives/edgar/data/1579241/000157924126000007/alle-20251231.htm

No application change has yet been made for these seven cases. Source acquisition and baseline replay are terminal. Next repair revenue-concept selection with original-statement regression/consumer checks, then exact release and ordinary operation. Full financial coverage, institutional publication, transcript/analyst/valuation gaps, final consumer/capacity checks, Massive, global FMP shutdown and billing remain incomplete.
