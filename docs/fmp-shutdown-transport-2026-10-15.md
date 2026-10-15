# FMP shutdown transport repair, October 15, 2026

## Verified defect

The current managed application based on deployed209c22bb still calls FMP from `main._fetch_eod_light_closes_for_symbol` without the global permission check. Its threaded caller is used to obtain prior closes for market quotes. Setting FMP_PROVIDER_DISABLED therefore does not prevent this path from reaching HTTP, and the request also bypasses budget accounting.

The October8 cutover report described this helper as repaired, and the divergent primary checkout contains the repair. That is not evidence that the managed/deployed application has it. This report supersedes that claim for this particular helper. Other price adapters in the primary checkout remain local and are not promoted by this repair; Massive activation remains last.

## Repair and validation

Call the existing global/budget guard immediately before transport, record error/throttle responses through the existing usage service, and return the existing empty result on controlled provider refusal. Cached rows remain readable. Successful enabled payload parsing and dates are unchanged. No flag, provider, quote semantics, schedule or database schema changes.

The initial Linux Python3.12.3 shutdown test run gives19passes/eightfailures: three direct/threaded/mixed-cache network escapes, four missing accounting checks and one budget refusal escape. All outbound HTTP is intercepted; no real FMP request is made. The first repaired run gives75passes/one failing new assertion because the established usage service records successful attempts before transport, not as a second response event. The assertion was corrected to verify one call plus status-specific failure events, without changing the application repair.

Final affected run: **76 passed** on Linux Python3.12.3. Files: test_fmp_shutdown_transport.py, test_market_quotes.py, test_provider_usage_hot_routes.py, test_valuation_provider_guard.py, test_cache_first_fmp_remaining.py. The new27checks exercise real House/Senate/insider/institutional/screener/news/financial/transcript/metadata/index/market-pressure callers under the disabled flag, explicit-live override refusal, direct and threaded previous-close calls, retained cache reads, enabled success/error contracts and budget refusal. This is targeted coverage, not a full application or all-schedules pass.

The installed-code probe rehearses on Windows Python3.14.2 with exact main/provider-usage/request-context hashes. It extracts the exact two helpers via AST to avoid application startup and uses the real guard in a separate process, with usage persistence disabled and HTTP intercepted. It verifies direct/threaded shutdown, cache preservation, a synthetic enabled response and budget refusal. Production runtime/deployment verification is pending. Helper: artifacts/verify-fmp-shutdown-runtime-oct15.py.

## Release and remaining gates

Local application repair only at this checkpoint. No production flag was changed; FMP is still enabled. Exact reviewed release, guarded deployment and installed-runtime proof are next. Whole-product zero-FMP traffic still requires actual cutover and observation; this fix does not establish source coverage, retained price availability or complete alerts.

Core financial coverage/definitions, institutional current-cohort validation and activation, Senate freshness, broad transcripts, paid analyst validation, watchlist/monitoring/daily/weekly/ranking consumers, sustained performance, final Massive activation and billing retirement remain open. Prior source-evaluation turn yielded new candidate evidence; this turn reproduces and repairs a concrete shutdown defect. Goal remains active.
