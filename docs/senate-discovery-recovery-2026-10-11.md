# Senate discovery outage recovery, October 11, 2026

The currently selected official Senate feed has returned HTTP403 since October10,20:17UTC. Its seven-day default discovery window would eventually move past unsearched dates. Successful parser tests or a matching community snapshot do not close this availability gap.

## Repair

Default unattended Senate collection now saves its earliest uncovered date before requesting the source. First initialization includes the existing publication boundary if earlier than the ordinary seven-day window. A complete official paginated search advances the discovery checkpoint only after all discovered identities are committed to staging. Discovery errors, a wrong/incomplete receipt or staging failure retain the checkpoint. Parsing/publication completeness remains separately reported; a completed search alone does not claim those are complete.

Recovery searches preserve the existing93-day interval and20-page Senate limits and overlap their boundary day. Long outages are traversed in successive bounded runs. Ordinary seven-day overlap resumes once caught up. House/SEC windows, schedules, document limits, source selections, canonical identities and alert behavior remain unchanged. Explicit operator start/end arguments bypass automatic recovery and retain their exact scope. A denied or unreachable Senate discovery no longer makes a second same-run request for saved documents; saved originals remain unchanged and the run still reports the source failure.

The checkpoint uses the existing insights_snapshots table, no new schema migration. Normal collector serialization protects planning and completion; stale competing completion is rejected. Unfinished discovery catch-up reports partial with caught_up=false. Rollback is the prior backend release; the additive checkpoint can remain unused without changing public data.

## Validation

Python3.14.2:94 focused tests pass across Senate recovery, Senate session/parser, direct feeds and scheduling. They cover outages beyond the seven-day lookback, multi-window recovery over months, interruption before HTTP, malformed/wrong-source/wrong-window receipts, source-denial handling, failed staging, exact manual windows, and real saved Whitehouse original repeat with exactly one document/revision and zero public writes. Initial fixture omissions and a Path/bytes mistake failed only local tests and were corrected before the passing run. git diff --check passes. No production recovery or access restoration is yet claimed.

At20:45UTC the separate read-only capacity sample has zero current five-minute throttling, API reserve about53.49 CPU seconds and background nice CPU0.0817. This is still short-window evidence. Scheduled SEC metadata/earnings completed5/5 scopes; SEC financial preparation completed20/20 with partial coverage, and fundamentals10/10 with explicit missing-directory/current-financial coverage. A separate insights job logged malformed Walnut Take model responses, requiring diagnosis. No new machine/resource/provider selection or subscription changes.

Release and scheduled verification remain pending. Senate403, institutional capacity/growth, remaining financial/transcript coverage, all alert/ranking consumers and final Massive/global-FMP/billing gates remain open.
