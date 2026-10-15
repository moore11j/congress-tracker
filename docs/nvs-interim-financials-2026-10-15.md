# Novartis SEC interim financial coverage, October 15, 2026

## Problem and scope

The SEC companyfacts capture has annual and comparative Novartis IFRS observations but lacks its current June 2026 results. The existing US GAAP reader therefore cannot prepare current NVS fundamentals. The issuer reports USD and publishes the current inline XBRL in a linked 6-K exhibit. The cover holds DocumentType and registrant name; its exact linked exhibit supplies their shared context, CIK and period.

The replacement uses a reviewed NVS-only IFRS basis, validates that document set, and makes at most two serial requests for the missing current filing. Complete companyfacts needs no filing request. A newer same-period amendment still triggers validation even when the old revenue period is complete. Missing or ambiguous cover/exhibit identity fails closed. Reporting forms, currency, namespace, fiscal quarter and filing-day availability remain explicit; no generic foreign-issuer mapping or ADR conversion is introduced.

## Source definitions

Revenue is net sales to third parties, excluding separately reported other revenue. Margins use this net-sales denominator. Income and equity are attributable to owners of the parent. Cash flow is consolidated operating cash flow minus cash PPE purchases, including discontinued cash flows where reported and excluding intangible purchases. This is not issuer-adjusted free cash flow. The older historical statement reader uses continuing-operations CFO and consolidated net income, so its fields must not be described as identical to these parent-income/consolidated-cash metrics.

Ten metrics are prepared: gross, operating and net margins; revenue growth; operating-margin change; current ratio; ROE; free cash flow, margin and growth. No EBITDA, net-debt, enterprise-value or per-share proxy is invented. Missing components remain unavailable. The NVS cache version changes selectively; other issuer versions and source selectors stay unchanged.

## Retained originals and independent verification

- Current cover: https://www.sec.gov/Archives/edgar/data/1114448/000111444826000012/nvs-20260630.htm
- Current exhibit: https://www.sec.gov/Archives/edgar/data/1114448/000111444826000012/nvs-20260630-99_2.htm, SHA256 fd11714b58b3c49b8e09759a553f790d3160b04da179787b3b441a9a8646eaee.
- Prior cover: https://www.sec.gov/Archives/edgar/data/1114448/000137036825000012/nvs-20250630.htm
- Prior exhibit: https://www.sec.gov/Archives/edgar/data/1114448/000137036825000012/nvs-20250630-99_2.htm, SHA256 c2199b297ed0a119180777722469d53c8670a1ab825aef019698f42bce280d96.
- Annual: https://www.sec.gov/Archives/edgar/data/1114448/000111444826000004/nvs-20251231.htm, SHA256 ee4e72876e1e8fe464286874b883191ffac5a9ba630aacf7023e59a66878eda0.

The prior accession's submitting prefix differs from the issuer CIK; the SEC directory and original inline identity prove the issuer. All 40 current/prior/instant calculation inputs match the complete retained originals, including amount, concept, currency, period, accession and filing date. The two interim originals yield 30 reviewed unscoped facts each; the annual yields 24. Reduced regression fixtures preserve original relevant fact/context/unit nodes, with complete parsed-fact equality to each original. Original bytes remain in artifacts/nvs-interim-oct15 and artifacts/foreign-batch-oct14.

Independent arithmetic in USD millions: current trailing revenue 54,532 + 27,521 - 27,287 = 54,766; prior 53,263. Parent income 13,984 + 6,420 - 7,647 = 12,757. CFO 19,144 + 9,558 - 10,309 = 18,393; PPE 1,548 + 667 - 585 = 1,630; cash flow after PPE = 16,763. ROE uses average parent equity 41,985 and 41,542. Current assets/liabilities are 27,351 / 31,748.

## Tests and consumers

Final new Windows Python 3.14.2 selection: 33 passed. The preceding NVS/CAD selection passed 45 before two extra consumer/amendment tests. Two initial test-fixture defects (an HTTP versus HTTPS mutation and an invalid empty supplement) were corrected; no failing check was ignored. Linux Python 3.12.3 affected regression selection passes 235 checks; final NVS selection passes 33, totaling 237 distinct checks. The full-original/cache/confirmation/watchlist/control replay also passes on Linux. Installed-probe local rehearsal passes with external connections prohibited; installed validation remains pending. Its first invocation referenced a nonexistent test filename and collected no tests; the corrected invocation is tracked separately.

Offline full-original replay passes: ten prepared metrics; complete cache/projection repeat; no canonical records during preparation; three scored inputs and an available existing confirmation consumer. A missing-to-available source transition produces zero alerts, a later synthetic threshold crossing produces one, and repeats/rollback produce none. No emails or network requests. Twenty saved control tickers match complete baseline e2c5b3b5 output. Receipt artifacts/nvs-interim-oct15/full-replay.json; original input receipt original-parity.json. These are isolated tests, not live scheduled delivery or broad migration completeness.

## Release and remaining gates

Local implementation only. Exact release, installed runtime and ordinary preparation/quarter rollover remain unverified. Broad financial coverage and definitions, institutional publication, transcript coverage, paid analyst/valuation decisions, scheduled consumers and sustained reliability, final Massive and global zero-FMP/billing remain open. FMP remains enabled. Pending production-derived export approvals were not retried. No subscription, capacity change, customer email, video or landing-page change.
