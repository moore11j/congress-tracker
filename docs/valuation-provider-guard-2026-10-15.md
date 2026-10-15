# Valuation provider guard and residual cutover dependencies

Date: October 15, 2026. Baseline: main `12fd3668`. Status: implemented and tested locally; not deployed. This repair is independent of pending CAT PR656 and does not select a replacement valuation model.

## Reproduced failure and repair

An offline rehearsal selects free_direct fundamentals, SEC financial statements, Massive prices and Finnhub news, disables FMP, and supplies a dummy key to exercise the actual provider guard. Network access is blocked at both requests and socket boundaries, usage persistence is disabled, and the database setting is isolated SQLite. No production data, credentials, customer messages or live provider calls are used.

Before repair, `get_ticker_valuation('AAPL')` raises uncaught `ProviderDisabled` on the initial price-target request. The public route invokes this handler without catching that exception. The fix catches the shared `ProviderUnavailable` class at consensus and calculation boundaries, returns the existing unavailable contract and preserves consensus already received. A guard raised while gathering inputs propagates to that calculation boundary, immediately ending collection. Unexpected programming errors still propagate.

The after-repair rehearsal returns `status=unavailable`, null fair value and empty cash-flow inputs. There is one valuation adapter attempt and zero HTTP/network attempts. These counts are not production traffic measurements. FMP remains required for actual valuation availability.

## Validation and limits

Linux Python 3.12.3: eight new guard cases pass, including disabled/budget errors at consensus, input and final calculation stages; retained consensus; real disable guard without HTTP; and unexpected errors not hidden. Combined existing/new tests: 22 pass, three fail. The same three failures reproduce against checksum-verified unchanged main loaded in memory: 14 pass, three fail. They concern rounding (79.8023 versus79.8022), a pre-profit growth expectation (0 versus0.15) and NAV valuation (8.25 versus16.2633). This patch changes no calculation formulas or defaults. There is no full-suite pass claim.

Offline before/after receipts run on Windows Python3.14.2; Linux tests exercise the actual guarded path. Changed valuation source SHA256 (LF normalized): `7dbb00722337a1c76c80dc2b4122d50703d768f8f4f430a45b9610e1a0764fd8`. Receipts: `artifacts/valuation-cutover-before-oct15.json`, `valuation-cutover-after-oct15.json`, `valuation-provider-guard-oct15.log`, `valuation-baseline-tests-oct15.log`. Reusable rehearsal: `artifacts/rehearse-valuation-cutover-oct15.py`.

## Remaining functional replacements

1. Valuation still directly requests price targets, twelve input groups and the final FMP custom DCF calculation. The SEC historical statements selection does not redirect this path. Inputs include TTM and annual statements, annual estimates, growth rows, company profile and a baseline DCF. A retained valuation feature requires an explicit local calculation/assumption policy and compatible complete inputs, or another verified calculation source. The existing operating-cash-flow projection alone is not a complete free-cash-flow valuation. Do not silently redefine the model or represent unavailable as a completed replacement.
2. Sector performance still uses FMP `sector-performance-snapshot`. The rehearsal records six guarded adapter attempts and an empty result, with zero HTTP/network attempts. Massive quote selection does not replace this calculation. Any sector ETF proxy or own universe calculation requires truthful naming, constituent/weighting and period definitions; neither is proven equivalent here.
3. Screener reads are selected-provider aware, but both response builders still label the source `fmp_company_screener` even for cached replacement rows. Dataset origin needs to be tracked explicitly, without claiming all mixed metadata/overlays came from SEC.

These are additional explicit cutover gates alongside financial coverage/definitions, institutional current-cohort writer and consumer proofs, transcript coverage, all alert/ranking consumers, sustained performance, final Massive activation and global zero-FMP/billing verification. No production source, schedule, host, account, billing, email or landing-page change occurred.

## Session and approval state

The screenshot's remote429 is a Codex request failure. Current account tool reports weekly ordinary allowance100% used, available credits and reset October20,03:25Pacific. The current session executes; this is not proof of a permanent remote-service repair. Official documentation describes shared usage and credit continuation: https://learn.chatgpt.com/docs/pricing . No account/model/reset changes were made.

PR656 remains separately held for explicit permission after automatic approval review rejected CAT financial/scheduler payload copies to OneDrive. The separate institutional snapshot export also awaits its own approval. Neither rejected command was retried, and this work uses local code/fixtures only. Last verified production remains12fd3668; FMP stays enabled and institutional publication stays disabled.

## Deployed release verification

PR657 is merged and deployed as1b85b84d, workflow38669903883success. Four runtime source hashes and seven synthetic guard checks pass onPython3.12.15 without database reads/writes/provider requests. Readiness/Feed/Insights return200; Feed0.577s/Insights0.584s are short HTTP samples only. See [current release and screener evidence](screener-source-provenance-2026-10-15.md). FMP selections and pending payload approvals remain unchanged.
