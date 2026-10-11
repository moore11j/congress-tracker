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

## Deployed and runtime verified, October11 20:58UTC

PR581 merged4d0d69023e65ea9c27c70e417b2178902db041a6; backend deployment38174258100 succeeded. Secret scan and Vercel preview pass. Existing frontend/backend dependency audits fail in unchanged dependency files; automated code-review quota remains unavailable and no independent review pass is claimed. The app could not attach the PR because this chat already exceeds100 attachment identities; its ordinary GitHub link remains usable.

Receipt two-service-verify-1791752288.json verifies23 exact runtime hashes, the release image, unchanged two-host sizes, zero pending jobs/one ordinary active job and institutional publication stillfalse. Six public requests return200; feeds0.387/0.390s, NVDAprofile0.464s and cached contexts0.373/0.532s. Earlier before restart, fresh AAPL/NVDA contexts1.336/0.972s passed. Neither sample proves sustained full-load recovery.

An isolated in-memory SQLite exercise using the deployed Python3.12.15 code verifies that a denied October1-11 search retains its anchor, then a later complete October1-31 search advances it. Receipt senate-recovery-runtime-1791752286.json makes zero production queries, source requests or canonical writes. The next ordinary hourly Congress run is still required to verify the production checkpoint; official access has not been restored by this repair.

Two read-only news-response audit helpers timed out while observing remote SSH. The first was subsequently confirmed absent; the completed deployment ended pre-deployment processes. A new bounded read-only audit is being checked separately. No production source selection, schedule, public event, customer test email, purchase or FMP cancellation was performed by this release.

## Final read-only news finding,21:00UTC

The bounded audit completes (take-response-audit-1791752372.json):39 of the last40 Walnut Take responses exactly hit the configured output-token cap, including all shown eight-article720-token batches; the one-article256-token request used141. Their HTTP200 audit result does not establish valid complete JSON. Code echoes article cache identifiers derived from URLs up to500characters in each output item; this is a concrete candidate cause of replacement-news enrichment failures. Short request-scoped identifiers can preserve durable article URLs/cache identity and avoid raising the token allowance, but this fix is not yet implemented. No paid validation/model request was issued by this audit.


## Ordinary scheduled recovery receipt, October 11, 21:17 UTC

Actual collector run179 starts21:17:05.167527UTC and finishes21:17:07.632664UTC. The source again returns403 at the public home page. Its separate Senate recovery window is October1-11, while the unchanged ordinary House window remains October4-11. The persisted checkpoint retains anchor2026-10-01, covered_through=null, last_attempt_run_id179 and caught_up=false/advanced=false. Both saved original hashes/statuses/checked timestamps and source generations remain unchanged. This verifies production outage retention, not restored Senate access or completeness. Twenty-six denied attempts are now recorded since October10; last complete official discovery remains October10,16:22UTC.

The first read at21:17:06 observed the checkpoint before run179 completed; the second at21:17:40 captured its terminal receipt. No source retry was manually launched. The intervening queue observation's active b6268a9ce4ddfb678d92c12f was ordinary enrichment, not proof of an active Senate collector. Evidence: senate-runtime-1791753426.json and senate-runtime-1791753460.json.
