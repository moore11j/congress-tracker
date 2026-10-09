# SEC financial preparation release

October 9, 2026. Continues the owner's request to verify compatibility, prepare exact releases and observe scheduled operation before final FMP shutdown. Massive remains last.

## Scope and safeguards

Adds opt-in preparation of SEC company identity and historical financial-panel caches for the existing watchlist/recently-viewed/default symbol universe. Each run attempts at most twenty companies, rotates failed scopes, obeys the shared collector lock and database-pressure guard, and stops after source refusal/cooldown. A durable twenty-minute lease uses the deployed same-machine dead-owner recovery. The proposed schedule is every five minutes at minutes 4 through 59. SEC_RESEARCH_WARMING_ENABLED defaults off. FINANCIAL_STATEMENTS_PROVIDER and COMPANY_METADATA_PROVIDER remain FMP.

The financial reader can select the separate SEC cache later. Public cache misses queue background work; requests do not fetch SEC sources inline. Existing FMP financial caches, canonical metadata, score inputs, holdings, events and delivery records are unchanged by preparation. Historical reported USD periods remain explicitly partial; diluted EPS requires a direct reported fact, ambiguous periods are omitted, and cash-flow quarters require aligned reported periods or aligned YTD differences. Forward estimates/targets, valuation and unsupported issuer concepts are unavailable. This does not establish broad bank, ADR, foreign or share-class coverage.

The pure SEC fundamentals projector is included as the shared calculation dependency. This job does not call its market-data fetcher, invoke Massive, or select FUNDAMENTALS_PROVIDER. The later ratio/ranking integration is separate.

## Validation

Seventeen exact-release SEC projection/cache/warmer checks pass on Python 3.14.2. The primary warmer/free-adapter run passes 23 tests. Four existing financial frontend contract/default tests pass. The existing forward-P/E normalization assertion also fails using the unmodified main ticker-financials module; it is a reproduced baseline failure, not a full-suite pass. An initial missing date import in the newly extracted fixture was fixed.

Actual disposable PostgreSQL with pool size two and no overflow prepares one complete fixture cache through the real directory/panel cache code. Three source-fixture requests occur, three competing collector attempts are refused, repeat adds zero requests, the lease clears, and public selection stays FMP. The initial contention harness incorrectly reused one process's saturated pool for the second process; the corrected harness uses a separate pool sharing the same PostgreSQL lock. No external HTTP or email occurs. Its disposable schema is removed and the loopback server stopped. Evidence: sec-financial-two-connection.json; runner backend/scripts/test_sec_financial_pool.py.

Final combined exact-release check: **41 passed, one deselected** (the reproduced unchanged forward-P/E assertion).

Deployment, explicit preparation enablement, actual source coverage and scheduled repeat are pending. Public financial activation requires fresh prepared-cache consumer checks and clear coverage labels. No purchase or FMP cancellation.


## Verified SEC directory absences

The first scheduled SEC financial batch processes twenty symbols and stores seventeen financial caches: fourteen partial, three unavailable (BULL, CX, ENB). AL, BNPQY and DRAM are absent from the verified current SEC exchange directory. The worker had no cache for those identities, so a selected public financial reader would repeatedly show warming. A small correction stores an explicit unavailable panel for verified directory absence. It makes no issuer request, invents no CIK, and leaves historical FMP caches untouched. Network/SEC refusal still raises and is not cached as a missing security. Default public selection remains FMP.

Two added tests cover public unavailable-state reads without queue churn and transient directory failure without a misleading persistent negative cache. The primary combined warmer/free-adapter group passes25 tests. This correction requires exact-release checks and deployment before public financial activation.

## October 9 complete attempt rotation and financial consumer compatibility

PR495/7f141eca is deployed with 24 matching runtime hashes on four workers. Verified directory absences produce explicit unavailable panels; transient source failures remain retryable. By 22:08 UTC the actual schedule has attempted all 63 symbols and prepared 61 caches, with 50 partial and 11 unavailable. Two earlier directory gaps await their next rotation. No financial, metadata, fundamentals or price selector has changed.

The consumer audit found that peer comparison directly read the old FMP financial cache even when SEC financial statements were selected. The prepared correction uses the caller's transaction to read only a fresh, source-matched, symbol-matched SEC panel. Missing, stale, future-dated or mismatched selected data cannot reuse old FMP forecasts. Returning to FMP retains the original cache. No additional connection, source request or queue write is introduced.

Thirty-nine exact-release financial/comparison/warmer checks pass on Python 3.14.2; one previously reproduced unchanged forward-P/E assertion is excluded. A replay of all 61 actual public financial caches through the public endpoint function and comparison helper preserves 50 partial/11 unavailable states, has stable repeat output and retains the legacy rollback fixture. It performs zero HTTP calls, queue writes, emails or production writes. The first harness invocation patched a function at its import destination instead of its defining module and failed before consumer execution; the corrected harness passes. Evidence: sec-financial-consumers.json, sec-unavailable-deployed-hashes.json; runner backend/scripts/rehearse_prepared_financials.py.

The comparison correction is prepared for a scoped release. Core fundamentals, ranking inputs and public financial activation remain separate gates.
