# SAP EUR IFRS financial preparation, October 15, 2026

## Source gap and reviewed replacement

SAP companyfacts contains annual IFRS EUR values but lacks current interim observations. The replacement prepares seven ratios from the exact SEC-filed current and prior half-year statements and annual inline XBRL: revenue growth; gross, operating and parent net margins; operating-margin change; current ratio; parent ROE. It does not select a public provider, add a schedule, purchase a plan or change server capacity.

The 6-K reportDate is a publication date, not the financial period. Both statement date headers and exact directory identity are checked. Original hashes bind the parser to reviewed statements, while any subsequent or amended 6-K/20-F holds preparation pending review. Thus quarterly rollover remains an operational gate, not a completed capability. Collection makes exactly three bounded original requests after validating the directory. Missing or changed originals never fall back to older annual financials.

## Economic definitions and explicit remaining gaps

Use actual-currency EUR IFRS consolidated statements, not adjusted or constant-currency KPI tables. Income and equity are attributable to the parent. USD market cap cannot divide euro statement amounts; no USD cash amount, FX conversion, ADR EPS or valuation proxy is inferred.

Cash purchases combine intangibles with property, plant and equipment. The issuer's free cash flow further includes asset-sale proceeds and lease payments. These cannot silently replace Walnut's PPE-only cash-flow definition. The cash-flow statement also discloses that interest paid/received classifications changed in January2025. Canonical FCF and its ratios remain unavailable pending a compatible breakdown and period reconciliation. EBITDA and net-debt ratios also remain missing. Seven verified ratios are progress, not complete SAP financial coverage.

Original euro-million values are independently rounded. Statement identities are validated against the sum of their half-million rounding intervals, not a percentage tolerance. Current/prior consumed comparative values match exactly. The prior interim's December2024 equity and current-liability observations differ from the newer annual values; both revisions are retained explicitly in evidence and neither is used in the published ratios. Any mismatch in a consumed period is a hard hold.

## Source originals

- Current cover: https://www.sec.gov/Archives/edgar/data/1000184/000110465926087251/tm2621275d1_6k.htm, SHA256 e4063183fe3a970fb5ef37b09acffff9fea121d63e380ec2511f59c9a1edc8b7.
- Current half-year exhibit: https://www.sec.gov/Archives/edgar/data/1000184/000110465926087251/tm2621275d1_ex99-2.htm, SHA256 f2afc72d4c46b140350ebf7e6476d00f1c393fba172451995972cb26eaf14329.
- Prior cover: https://www.sec.gov/Archives/edgar/data/1000184/000110465925070485/tm2521516d1_6k.htm, SHA256 aa7d096f54849eea5fca23089ebfca64a00f6f5f5f5dddd20b22bad5c9d24a43.
- Prior half-year exhibit: https://www.sec.gov/Archives/edgar/data/1000184/000110465925070485/tm2521516d1_ex99-2.htm, SHA256 df76b849c9b76ad1eda631efb47a9d2d9d8aa54bb615789cfc098d37206dca9d.
- Annual: https://www.sec.gov/Archives/edgar/data/1000184/000110465926020058/sap-20251231x20f.htm, SHA256 4628dc2d12f68bfbbe7c07a2099a6916db30d9547b8f3d1424b451272790fc15.

Originals and source manifests are retained under artifacts/sap-interim-oct15 and artifacts/foreign-batch-oct14. Covers explicitly link the reviewed half-year exhibits; duplicate index links resolve to the same URL. Reduced original-table/inline fixtures reproduce all46 parsed financial observations, with source identity hashes recorded separately. All18 annual observations match official companyfacts amounts, periods and accessions.

Independent arithmetic, EUR millions: current trailing revenue36,800+19,432-18,040=38,192; prior34,176+18,040-16,329=35,887. Current operating profit9,617+5,383-4,789=10,211; prior4,665+4,789-434=9,020. Parent income7,161+4,112-3,477=7,796. ROE uses parent equity40,998 and44,742; current assets/current liabilities22,649/19,734. No cash value is converted to USD.

## Validation and release state

The first40 new tests pass on Windows Python3.14.2. Final304 selected tests, including persisted-cache/USD-currency guards and the affected financial regression suite, pass on Linux3.12.3 in95.83seconds. Full original/source/cache/confirmation/watchlist replay passes on Windows3.14.2 and Linux3.12.3: seven metrics, three usable scored inputs, exact repeat, zero canonical rows during preparation, zero alerts on source transition, one later synthetic threshold alert, zero repeat/rollback duplicates and no emails. Complete output for20 saved control tickers matches baseline2fe98978. Installed verification is pending. Receipts: artifacts/sap-linux-oct15.log, artifacts/sap-consumers-linux-oct15.log and artifacts/sap-interim-oct15/full-replay-linux.json. The isolated network-blocked runtime probe passed its local rehearsal.

Initial validation repaired a real prior-year blank date-label column. Two test-generator problems (Unicode bytes literals and omitted fixture encoding metadata) were corrected. All relevant failures are retained in the work history rather than described as passing runs.

Local implementation only. Release, ordinary preparation/rollover, remaining financial definitions and all broader institution/transcript/analyst/valuation/consumer/sustained-capacity/finalMassive/globalFMP/billing gates remain. Pending production-derived export permissions were not retried; FMP remains enabled. No subscription, source switch, hardware change, customer email, video or landing-page edit.
