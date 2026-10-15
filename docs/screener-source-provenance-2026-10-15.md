# Screener source attribution and provider cache isolation

October 15, 2026. Baseline main: `1b85b84d`. Implemented and tested locally; not deployed.

Both screener response builders previously labeled every result `fmp_company_screener`, including selected SEC/free caches and the broader identity/quote universe. The response cache also omitted the selected fundamentals provider from its key, permitting a previously built response to be reused after a provider selection changed within the cache lifetime.

The dataset now captures the selected fundamentals provider once, uses it consistently in both cache queries and provider-fallback eligibility, and returns explicit metadata. `source=walnut_cached_screener` describes the local assembled dataset, and `fundamentals_provider` identifies the selected fundamentals input. It does not claim that identity, quotes or intelligence overlays all originated at SEC. Only a dataset built through the actual allowed FMP fallback uses `fmp_company_screener`. The response cache key includes the fundamentals provider, retaining existing query and entitlement isolation. No provider is switched, no new fetch is introduced, and per-row fields, rankings, filters and entitlement caps are unchanged.

## Validation

Twelve new Linux Python3.12.3 cases pass: three providers across ungated/Free/Premium builders, same-symbol cached provider isolation, metadata/quote-only rows retained with unknown fundamentals, deduplicated identities, repeated reads, zero event creation, fallback classification and response-cache partitioning. All run on isolated SQLite with HTTP blocked and enrichment queue calls replaced locally.

Combined existing/new screener tests:64pass/6fail. The same six failures reproduce on main extracted into memory:52pass/6fail. They concern old confirmation-strength/filter expectations, a Premium CSV-export fixture when export requires Pro, and fixtures expecting25 Free rows while the current limit is5. This repair does not alter any of those rules. No full-suite/all-CI pass is claimed.

An independent baseline/candidate rehearsal compares every response field, excluding only the intended top-level `source` and new `fundamentals_provider` metadata. Eighteen complete comparisons pass across the nine provider/access combinations and identical repeats. Each response retains two unique identities and all86 ungated/88 entitlement row fields, including metadata-only rows with null fundamentals. There are no production reads/writes, provider calls or customer emails.

Evidence: `artifacts/screener-provider-source-oct15.log`, `screener-baseline-tests-oct15.log`, `screener-source-parity-oct15.json`; reusable replay `replay-screener-source-parity-oct15.py`. Source hashes (LF-normalized):

- `app/services/screener.py`: `117cd1625bcc144739c750e0de26d6514d9cf281dd888e7d36132e91249e20a6`
- `app/routers/screener.py`: `8d714a50023ca62164bc7f2ee08cb351c96a37b9b45704d28a8156e1936660f6`

## Release and wider cutover gates

Exact release and deployed source/contract checks remain required for this screener patch. It fixes provenance and transition-cache correctness, not missing financial coverage or broad ranking/alert equivalence.

The separate valuation guard PR657 is now deployed as1b85b84d through successful workflow38669903883. Four source hashes and seven synthetic controlled-provider checks pass in deployed Python3.12.15, with zero database reads, production writes or provider calls. Receipt: `artifacts/valuation-guard-runtime-1792030600.json`. Anonymous readiness/Feed/Insights HTTP checks return200; Feed0.577s/Insights0.584s with no server-error-page marker. This is not browser-render or sustained-capacity proof. Receipt: `artifacts/valuation-guard-public-1792030726.json`. PR657 exact-head secret scan/Vercel preview pass; dependency audits fail on unchanged Pillow10.4.0 and five frontend vulnerability groups (four high/one critical).

Owner preference is pending on replacing the final FMP valuation calculation with a clearly labeled Walnut DCF model whose results may differ. No new model is implemented or activated by this repair. FMP's public formula page does not establish full custom-endpoint equivalence: https://site.financialmodelingprep.com/developer/docs/dcf-formula . A replacement needs explicit forecast, reinvestment, discount, terminal-value and equity-bridge policy with actual source coverage. Primary valuation reference: https://pages.stern.nyu.edu/~adamodar/pdfiles/valn2ed/ch15.pdf .

CAT PR656 and the separate institutional snapshot export still await explicit production-payload permissions after automatic review rejected copies into OneDrive. Neither rejected command was retried or routed around. The deployed guard verifier returned only code hashes and synthetic test counts; it read no production cache, financial, institutional or scheduler data.

FMP remains enabled for core/prices/transcripts and institutional publication remains disabled. Full financial/statement/valuation/sector/transcript coverage, current institutional writer/consumer proof, all alerts/rankings, sustained performance, final Massive subscription/validation and global zero-FMP/billing verification remain open. No host, schedule, subscription, landing-page, video or customer-email change.
