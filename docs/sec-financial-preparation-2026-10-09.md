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

The final consumer audit also updates research-brief financial context, ticker hydration and financial diagnostics to the selected cache. The 61-cache replay covers all five consumer functions, with stable source/status values and no legacy cache mutation. Fifty financial/comparison/hydration/warmer tests and seven research context tests pass. A broad research run encountered default temporary-directory errors; an isolated-directory rerun exposes the unrelated CapturingSession.bind schema fixture failure, reproduced against unmodified main 7f141eca. No full research-suite pass is claimed. Research source provenance is included in the prepared context. Historical audit/backfill tools retain their explicit historical FMP cache inputs.

## October 9 company-facts absence follow-up

The compact 22:19 UTC read-only receipt recovers after a full cron receipt timed out. Six actual scheduled runs have prepared 62 caches: 50 partial and 12 unavailable, clear lease. SPY remains the sole uncached symbol because its official company-facts URL returns HTTP 404 in two scheduled attempts.

The follow-up caches an explicit unavailable panel for an exact 404 from that issuer's company-facts URL. Permission, quota, transport and server failures remain retryable and are never cached as missing financials. The existing 24-hour TTL applies. Twenty-one focused absence/cache/warmer/consumer checks pass on Python 3.14.2. The compact read-only verifier can omit the approximately 5 MB public financial payload. No public financial selection, canonical write, Massive request or email.

## October 9 orphaned collector session recovered and prevention tested

PR497/a10f9b9a has 36 matching file hashes across four workers. Scheduled 22:24/22:29 financial runs correctly yield to the shared collector lock, rather than starting overlapping requests. Read-only diagnosis finds database session 18012, owner walnut:cron:807d42ce974018:751, started 22:20:03 before the cron machine's current 22:24:53 boot. Its last SQL only acquired advisory lock 84193647; no live collector process exists on that machine. The pending run 68 is House/Senate collection, not SEC.

The explicit recovery preview verifies boot ordering, exact session/user/owner/timestamp/state/query and granted lock. At 22:33:53 the conditional action terminates only that orphaned lock session. Zero canonical writes/emails. The actual 22:34 financial batch completes 20 scopes with clear lease, proving scheduled work resumed; SPY is still awaiting its next rotation. The unfinished historical run receipt is preserved, not relabeled as successful. Evidence: collector-recovery-preview.json, collector-recovery-applied.json, sec-financial-after-lock-recovery.json.

A scoped prevention fix recovers only a lock-only session from a previous boot of the same cron machine when lock acquisition fails. Live-boot, other-machine and other-query sessions remain untouched; unsupported/missing machine boot identity fails closed. Thirty-nine collector/schedule/warmer checks pass. Six real disposable PostgreSQL checks confirm live/foreign/non-lock preservation, exact old-boot recovery, reacquisition and repeat no-op. No source requests, production writes or emails in tests; the local database server is explicitly stopped. Evidence: collector-boot-postgres.json. The prevention release is prepared; public providers remain unchanged.


Final prevention validation also disables recovery for read-only prior-selection previews. The refreshed 39-test group passes; seven real PostgreSQL checks now include preview non-recovery. Windows refused reopening loopback port 61982 once; a bounded retry on fixed loopback 61983 succeeds without changing system settings. The completed test server is stopped. Final receipt: collector-boot-preview-postgres.json.
