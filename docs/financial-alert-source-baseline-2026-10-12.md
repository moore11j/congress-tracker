# Financial alert source baseline, October 12, 2026

## Local repair and pause

The custom watchlist evaluator rebaselines Confirmation Score methodology changes, but previously did not retain financial provider/calculation identity. Switching a financial ratio across a threshold could create a false business-change alert. Seven initial regression cases reproduced this for provider, methodology, supplemental field source/basis, rollback, comparison operand and legacy state. The repair is local only on `codex/fundamental-alert-baseline`; owner requested a sleep pause before review/release.

`custom_alert_rules.py` now reads financial values and their provenance from one selected cache row per rule. State records the provider, methodology and relevant supplemental field source/path/basis/unit/scale/issuer association/version. Revenue basis is included for revenue-growth rules. A changed identity or legacy state establishes a baseline without a trigger. Ordinary dates, source hashes and unrelated supplements do not reset the baseline, so later real financial changes continue to alert. Missing replacements retain valid prior state until observations recover. Price-only daily alerts keep their existing behavior. No schema migration, provider request or source selection was added.

Local Python3.14.2:185 tests pass across custom rules, financial source transitions, Confirmation Monitoring, monitoring alerts, price alert job, watchlist delivery matrix, Top Ideas digest and email digests. Four pre-existing framework deprecation warnings remain. This is focused consumer coverage, not an all-suite or production cutover claim. The first expanded run had a new test fixture using an unsupported metric-to-metric operator; corrected to supported thresholds. That process was verified terminal before the final run. Final session92525 completed exit0; log `artifacts/direct-feeds/free-replacements-2026-10-09/alert-basis-final-pytest-oct12.log`. No local database server was started.

## Saved-source financial definition audit

The offline helper `audit_finnhub_leverage_oct12.py` compares the same four fiscal quarter ends across saved free Finnhub and FMP responses, with prior SEC source evidence. These are historical saved observations, not fresh requests. Diagnostic scaling of Finnhub amounts by one million is an issuer-specific reconciliation hypothesis, not proof of a universal currency/unit contract.

| Issuer | Finnhub diagnostic TTM, USD billions | FMP TTM, USD billions | Denominator difference | Effect on ratio if same net debt were reused |
|---|---:|---:|---:|---:|
| AAPL |167.959|168.486|-0.313%|+0.314%|
| MSFT |193.593|212.433|-8.869%|+9.732%|
| NVDA |201.145|233.894|-14.002%|+16.281%|

All twelve quarter dates match. The free responses contain net-debt/equity ratios but no direct net-debt series. AAPL's diagnostic EBITDA matches SEC operating income plus D&A; NVDA differs from that basis by121million as well as differing substantially from the net-income-based denominator. An initial assertion of exact NVDA operating-basis parity failed and was corrected; it is not represented as equivalent. No rounded ratio inversion or mixed-provider ratio is accepted or published. The calculation differences demonstrate why the alert baseline protection is required; original issuer component reconciliation and broad coverage remain open.

Audit receipt `finnhub-leverage-definition-audit-oct12.json` records every source SHA256. Saved source hashes: free Finnhub `f8dd21eb5e4426a02ce0d306e14753ee3f58dcc039e6188be87808c651425940`; broader Finnhub `c22e048af36a3a06122225a363e9bf6cb08fada5f8482da26feb8971a81c8a57`; FMP `3a319604dcd63dd0ee543e9d91c6e0b1bc6323f96b128cb8b61d6f31ac3a0f92`; SEC comparison `efe1b71bb588dcd264bfa273db964500059a014765bf88496a9e11ba633db11c`. Zero network requests or production writes in this replay.

## Resume

Review the exact local diff, strengthen real-source provenance cases if required, then create/review/release the tested repair and verify deployed behavior before financial activation. Production remains9ce01d59. Core fundamentals remain FMP and institutions publication is off. The ordinary SEC06:47quarantine observation has not been performed and must be checked on resume, without treating elapsed time as proof. Retain the full migration scope, including institutional capacity/coverage, transcripts, every scheduled consumer, final Massive subscription and zero-FMP traffic/billing cancellation. No new purchase, email, video, landing edit, schedule or hosting change.

## October 12 review and release preparation

The resumed direct review adds per-metric formula identity, plus revenue-basis identity for operating-margin rules. A formula change can otherwise cross a threshold with the same provider/version. The new regression verifies baseline suppression and a later genuine alert. An integration fixture runs the actual Finnhub normalizer and SEC-first combiner, verifies supplementation and return to SEC reset once, and preserves later numeric transitions and two distinct alert records.

Expanded Python3.14.2 run:202 passed, one new SQLite fixture failed because its reloaded timestamp lacked timezone information. Corrected the fixture to explicitly preserve UTC; final affected financial/normalization run:30 passed. Together these cover203 selected tests, with four existing framework deprecation warnings in the expanded run. No application failure remains in these checks; no full-suite claim. Direct diff and whitespace review completed. No source activation, customer email or schema change.