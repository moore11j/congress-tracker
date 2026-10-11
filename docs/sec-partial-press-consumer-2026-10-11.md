# Retain verified releases from partial SEC press batches

October 11 UTC / October 10 local, 2026.

The selected SEC press reader can return `partial` when it retains a verified release but another filing exceeds the bounded download size. The operational research consumer accepted only `ok`, `empty` and `no_data`, rejecting the entire batch before its per-document source and publication checks.

A bounded read-only production check at05:09UTC reproduces this for ONDS: its02:07 prepared cache contains one release, one held filing and three unchecked filings, with `sec_submission_size_limit`. Research coverage incorrectly reports only `unavailable`/`RuntimeError`. The other two observed warnings, DRAM and QTUM, are actual SEC directory absences and must stay unavailable. Receipt: `press-consumer-1791695393.json` under the existing free-replacement artifact directory.

The repair accepts `partial` only for the selected direct SEC press adapter. Every item still passes the original provider, source, acceptance-time, activation boundary and deduplication checks. Partial payloads and explicit source failures remain partial after consumption, including when no release can be published. Stale responses, foreign providers, unavailable and warming responses remain rejected. There is no new source fetch, extraction budget, schedule, alert identity or coverage claim.

## Checks

The new regression fails on the prior release with zero documents/events instead of the expected one. After repair,69 focused press, canonical event and operational research tests pass on Python3.14.2. The regression exercises the real saved-source extractor with a simulated model response, verifies a single canonical/research event, identical retry without another extraction, visible partial coverage and zero email delivery. Additional cases retain stale/provider/availability refusal and hold pre-activation releases without documents or events. These are isolated tests, not live model or email calls.

Full integrated institutional replay93622 remains independently active. This repair does not change its calculations. Broader source coverage, institutional capacity/growth, core/transcript selection, ordinary consumers and final Massive/global FMP/billing gates remain open. No new subscription or infrastructure is introduced.
