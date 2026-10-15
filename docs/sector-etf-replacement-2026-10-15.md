# Explicit sector ETF replacement

October 15, 2026. Implemented locally against main `3fb09beb`; release and production selection are pending.

Insights already had a sector ETF fallback, but it used the two latest arbitrary rows, omitted missing sectors and dates, and the detail formatter removed ETF identities. That is insufficient for a reliable replacement of FMP's sector snapshot. This package provides an explicit opt-in view of eleven ETF price returns. It is not numerically equivalent to FMP's sector-average series and is labeled accordingly.

`INSIGHTS_SECTOR_PROVIDER=etf_proxy` selects the new view. Default remains `fmp`. A single indexed query reads only the expected completed US session and its preceding trading session for the eleven existing ETF symbols. Each pair requires a matching provider/symbol and the canonical `split_adjusted_price_return` basis, with positive finite prices. The result excludes dividends. Missing days, future-only data, raw prices, mixed providers or unsupported provenance remain unavailable, never zero. All eleven sectors are retained with available/missing counts, dates, underlying price source and method version. This uses the existing market calendar, including its existing calendar limitations.

The selected view has a separate persisted snapshot key, `macro-snapshot:sectors:sector_etf_eod_v1`. Activation cannot reuse a fresh or stale FMP-sector snapshot; rollback preserves the original snapshot. Both builder-safe and legacy macro modes use the selected view. The direct FMP sector function returns without an adapter request when the ETF view is selected. Other FMP macro capabilities are not claimed replaced by this change.

The detail page preserves ETF names and identifies the series as ETF price returns for a completed trading session, excluding dividends. Legacy FMP labels/layout remain unchanged. The old blanket source-text test prohibited even an `is_proxy` field; it is updated to allow truthful investment-basis labels, with executable formatting tests replacing that prohibition. Provider-name privacy checks remain.

## Bounded preparation

`INSIGHTS_SECTOR_WARMING_ENABLED=true` separately enables preparation within the existing macro refresh. It defaults off and never hydrates prices from an API request context. It waits for explicit Massive price selection and uses the Massive transport for at most two symbols per refresh, with a durable fair cursor that advances after failures. Fresh complete prices require no further requests. No new cron trigger, host or subscription is introduced. The final adapter makes at most two HTTP requests per refresh, each bounded by the existing15-second timeout, without a split-action request or FMP fallback. Actual workload capacity still requires observation before enabling this flag.

The simulated full cycle fills all eleven ETFs in six bounded runs (2/2/2/2/2/1), stores exactly22 daily rows, repeats without growth and then performs no further hydration. A separate all-failure cycle attempts every ETF before retrying the first, preserving fairness. This is not a live Massive entitlement or coverage test; the price subscription and final provider verification remain last.

## Validation

Final Linux Python3.12.3 sector/Insights checks cover the complete direct-source path, including source switching/rollback, both macro modes, missing data, legitimate zero returns, one-query reading, cache persistence and bounded preparation. Socket attempts are forbidden in all new integration cases. Exact final receipt:43passed in22.00seconds, artifacts/sector-etf-release-oct15.log. The mocked real transport also proves Massive-only acquisition while FMP remains enabled. Six frontend layout/formatting checks and TypeScript pass.

Broader news/macro run:67pass/4fail. All four failures reproduce with the three unchanged main modules loaded in memory: two old headline-cache expectations, a ticker-news status expectation and missing expected macro-unit metadata. No full-suite pass is claimed. Local receipts: `artifacts/sector-etf-broad-oct15.log`, `sector-etf-baseline-oct15.log`, `sector-etf-tsc-final-oct15.log` and `sector-etf-final-oct15.log`. No production payload is read/exported, no customer email is sent and no provider is called in the replacement rehearsal.

## Cutover status

Prepared only; production source and warming flags are unchanged. Verify exact runtime, complete current ETF-price coverage, underlying price provenance and sustained workload before activation. This replaces only the sector snapshot dependency. Core financial coverage, valuation choice, transcripts, institutions, all alert/ranking consumers, sustained reliability, Massive activation and global zero-FMP/billing checks remain open. Pending CAT/institutional payload permissions are separate and were not bypassed. Previous PR659 remains the last verified deployment at `3fb09beb`.


## Acquisition audit and final isolation

The first implementation used the existing canonical-price hydrator. Source inspection then proved that helper still prefers FMP and fetches FMP split actions, regardless of `STOCK_PRICE_PROVIDER`. It is removed from this feature before release. The broader legacy daily-price callers therefore remain an explicit final-price-phase gate, alongside subscription, quotes, history/actions and alert/return verification.

Sector warming now requires explicit Massive selection and uses only the existing bounded Massive transport, with one split-adjusted daily aggregate request per attempted ETF. Each whole pair is validated before any write: exact ticker, `adjusted=true`, both expected sessions at Eastern midnight, no extra page, no duplicates, positive finite OHLC, consistent OHLC bounds and finite nonnegative volume/dollar volume. Partial or contradictory pairs write nothing. Canonical rows retain `price_source=massive` and the split-adjusted price-return basis; incompatible old raw/action fields are cleared. Repeated valid hydration retains exactly two rows. The reader holds even same-provider FMP rows, so an old FMP cache cannot be treated as ready replacement coverage.

The original adapter preference defect is not claimed fixed globally. No live Massive request, new subscription or selector change is performed. The explicit source wiring here prepares the sector dependency for the final price phase.

Definition sources checked October15: [Massive Custom Bars](https://www.massive.com/docs/rest/stocks/aggregates/custom-bars) documents split-adjusted aggregates and bar timestamps; [Massive adjustment FAQ](https://massive.com/knowledge-base/article/is-massives-stock-data-adjusted-for-splits-or-dividends) distinguishes dividends. No extra split factor or dividend adjustment is applied to already adjusted bars.
