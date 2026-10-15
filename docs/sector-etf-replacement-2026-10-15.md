# Explicit sector ETF replacement

October 15, 2026. Implemented locally against main `3fb09beb`; release and production selection are pending.

Insights already had a sector ETF fallback, but it used the two latest arbitrary rows, omitted missing sectors and dates, and the detail formatter removed ETF identities. That is insufficient for a reliable replacement of FMP's sector snapshot. This package provides an explicit opt-in view of eleven ETF price returns. It is not numerically equivalent to FMP's sector-average series and is labeled accordingly.

`INSIGHTS_SECTOR_PROVIDER=etf_proxy` selects the new view. Default remains `fmp`. A single indexed query reads only the expected completed US session and its preceding trading session for the eleven existing ETF symbols. Each pair requires a matching provider/symbol and the canonical `split_adjusted_price_return` basis, with positive finite prices. The result excludes dividends. Missing days, future-only data, raw prices, mixed providers or unsupported provenance remain unavailable, never zero. All eleven sectors are retained with available/missing counts, dates, underlying price source and method version. This uses the existing market calendar, including its existing calendar limitations.

The selected view has a separate persisted snapshot key, `macro-snapshot:sectors:sector_etf_eod_v1`. Activation cannot reuse a fresh or stale FMP-sector snapshot; rollback preserves the original snapshot. Both builder-safe and legacy macro modes use the selected view. The direct FMP sector function returns without an adapter request when the ETF view is selected. Other FMP macro capabilities are not claimed replaced by this change.

The detail page preserves ETF names and identifies the series as ETF price returns for a completed trading session, excluding dividends. Legacy FMP labels/layout remain unchanged. The old blanket source-text test prohibited even an `is_proxy` field; it is updated to allow truthful investment-basis labels, with executable formatting tests replacing that prohibition. Provider-name privacy checks remain.

## Bounded preparation

`INSIGHTS_SECTOR_WARMING_ENABLED=true` separately enables preparation within the existing macro refresh. It defaults off and never hydrates prices from an API request context. It uses the existing selected-provider canonical-price hydrator, at most two symbols per refresh, and a durable fair cursor that advances after failures. Fresh complete prices require no further requests. No new cron trigger, host or subscription is introduced. The two-symbol limit is not a two-HTTP-request limit: the existing hydrator may fetch both bars and split actions. Actual workload capacity still requires observation before enabling this flag.

The simulated full cycle fills all eleven ETFs in six bounded runs (2/2/2/2/2/1), stores exactly22 daily rows, repeats without growth and then performs no further hydration. A separate all-failure cycle attempts every ETF before retrying the first, preserving fairness. This is not a live Massive entitlement or coverage test; the price subscription and final provider verification remain last.

## Validation

Thirty Linux Python3.12.3 focused sector/Insights checks pass, including source switching/rollback, both macro modes, missing data, legitimate zero returns, one-query reading, cache persistence and bounded preparation. The final focused run forbids socket attempts in all new integration cases and passes30checks in13.21seconds. Six frontend layout/formatting checks and TypeScript pass.

Broader news/macro run:67pass/4fail. All four failures reproduce with the three unchanged main modules loaded in memory: two old headline-cache expectations, a ticker-news status expectation and missing expected macro-unit metadata. No full-suite pass is claimed. Local receipts: `artifacts/sector-etf-broad-oct15.log`, `sector-etf-baseline-oct15.log`, `sector-etf-tsc-final-oct15.log` and `sector-etf-final-oct15.log`. No production payload is read/exported, no customer email is sent and no provider is called in the replacement rehearsal.

## Cutover status

Prepared only; production source and warming flags are unchanged. Verify exact runtime, complete current ETF-price coverage, underlying price provenance and sustained workload before activation. This replaces only the sector snapshot dependency. Core financial coverage, valuation choice, transcripts, institutions, all alert/ranking consumers, sustained reliability, Massive activation and global zero-FMP/billing checks remain open. Pending CAT/institutional payload permissions are separate and were not bypassed. Previous PR659 remains the last verified deployment at `3fb09beb`.
