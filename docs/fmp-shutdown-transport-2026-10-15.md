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

## Deployed and installed-runtime verified, October15 22:32UTC

[PR690](https://github.com/moore11j/congress-tracker/pull/690), tested9b3a761e8439b734c9cc3f08be77a3f90ace7e9a, merged as24bce6384f8a697dd62ff9c6f7a7ca7d6739d0f0 at22:29:43UTC. Both dependency audits, secret scan and Vercel preview pass on the tested head. The full merged tree equals the tested tree. The chat attachment limit is100, so the normal PR URL is retained. No extra reviewer approval is claimed.

Guarded workflow38817525515/job116705844418 completes successfully. Database readiness and Premium-route privacy checks pass at22:31:33UTC. A transient diagnostics502 was followed using status metadata and the same job; no duplicate dispatch. Existing strategy-history-repair submodule cleanup warning remains nonfatal.

Installed Python3.12.15 proof passes: three LF-normalized source hashes match, exact installed direct/threaded helpers stop before HTTP when disabled, existing cache stays readable, synthetic enabled parsing/accounting and budget refusal pass. The proof runs in a separate process with sockets/HTTP and usage persistence blocked. No production records read or written, no provider requests or customer emails; the live application's FMP flag is unchanged. Receipt: artifacts/fmp-shutdown-runtime-1792103514.json. Hashes: main03fdcb40b5f8e960bf04b99c2f78b247f24502a5b89453f5aa1b01cc4df877e8; provider_usage c31187befdf439a3deebdd38b53ed40494967a918cd43850a53c5bb6ab0189ab; request_priority73f445b60436f8c80c2b3f38d189df4194773747120231858bd8b0c865c26afe. Local probe normalization was corrected for Windows CRLF versus deployed LF before use; these are normalized-source hashes, not raw-file hashes.

Public rendered-response checks at22:31:50 return200/no server-render exceptions: Feed0.608s, Insights0.797s, AAPL0.492s, NVDA0.248s. Receipt artifacts/public-render-1792103510871.json. Pre-release receipt1792103269280 also passes. These are unauthenticated spot checks, not sustained capacity or signed-in/all-consumer verification.76 affected Linux tests remain the regression evidence, not a full-suite pass.

The repaired path is ready for eventual FMP shutdown; FMP remains enabled and full source/consumer/Massive/billing gates remain. No schedule, selector, host, subscription, customer delivery, video or landing change. Prior candidate evaluation and current deployed repair both constitute progress. All test/deployment/probe handles are terminal. Local receipt branch is rooted in actual24bce638; unrelated frontend tsbuild hashA185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA is preserved.
