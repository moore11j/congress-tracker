# Financial adapter version rotation, October 15, 2026

## Observed delay

PR685/e9ff171b installs the verified Unilever adapter, but ordinary21:01 and21:06 batches each complete ten routine scopes while eleven are due. UL remains on its10:56 unavailable cache and old attempted version. The new due routine scopes rotate from G through H; this is not repeated execution of the same failed batch. Ordinary oldest-attempt ordering continually puts older routine refreshes ahead of the newer-attempted but obsolete adapter, delaying release validation and new coverage.

Evidence: artifacts/financial-rotation-diagnostic-1792098436.json contains job dates/statuses and attempted versions/deadlines only, not financial values or customer data. UL's installed version is sec_ratio_preparation_v5_sec_ul_restated_eur_v1; its attempted version is sec_ratio_preparation_v5. The21:06 job is terminal/partial, exit1 by the module's documented status policy, CPU3.3591seconds; receipt artifacts/ul-financial-schedule-observation-1792098518.json. No jobs were replayed or refresh timestamps altered.

## Repair

Within the existing bounded batch, one waiting changed-version scope can replace the last ordinary slot if no changed-version scope is already selected. Watched/research/recent-view priorities and changed issuer evidence are protected. Other slots preserve oldest-attempt order. Existing attempt/version/deadline receipts remove the special eligibility after one attempt, including normal source-failure backoff. Neither the schedule nor batch size, shared-source lock, leases, transport-stop rules, Finnhub capacity or public provider selection changes.

This avoids waiting behind the routine rotation without adding requests to a batch or moving active monitoring work. A batch fully occupied by protected work still defers nonpriority updates. Updated versions are selected in their existing oldest-attempt order; there is no permanent UL preference.

## Validation

66 selected tests pass on Linux Python3.12.3: new version rotation, full existing financial warming, broad freshness, priority refresh and preparation. New cases verify protected work, limits, no mutation/duplicates, existing selected-version handling, one-time version refresh, subsequent displaced routine work and source429 backoff. A full isolated12-scope scheduler replay produces batches10/2/0, restores UL once, preserves12 canonical FMP fixture rows and creates no events/emails. The same synthetic installed-code probe is rehearsed on Windows Python3.14.2 with network blocked and no production reads/writes. Five installed hashes are prepared for postrelease verification.

## Other source work and remaining gates

Captured Vale's fresh official SEC directory and four full public statements, including amended6-K/A0001292814-26-004322, original6-K0001292814-26-004012, annual20-F0001292814-26-001844 and prior interim6-K0001292814-25-002831. The amendment explicitly adds interactive data without changing the original report; it contains3,032 inline facts and complete issuer/form/period identity. Financial comparison, tag semantics, later-filing review and integration remain pending. Originals/manifests are under artifacts/vale-financials-oct15/. No Vale metrics have been activated or presented as validated.

This scheduler repair is local until exact release/runtime receipts below. Unilever ordinary preparation, hourly transcript recovery and broader source/financial/institutional/consumer/performance gates remain. Core and transcripts remainFMP, institutional publicationfalse. Paid Finnhub and final Massive subscription/validation, global zero-FMP evidence and billing retirement remain unfinished. No new paid plan, hardware, recurring schedule, customer email, video or landing change. Pending private-payload export approvals remain untouched.
