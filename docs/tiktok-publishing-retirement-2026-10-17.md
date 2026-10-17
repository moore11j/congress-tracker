# TikTok publishing retirement

The owner disconnected Walnut's TikTok account to make room for separate Instagram influencer accounts and instructed Walnut to stop TikTok posting. This does not authorize connecting the new accounts or restarting video generation/publishing. Runway remains the future video choice; existing automation remains paused.

## Implementation

- Buffer destinations contain only the existing Walnut Instagram ID. No automatic account discovery or reassignment.
- Video generation/publish/retry/reconciliation requests reject TikTok. The Buffer client rejects it before transport.
- The worker filters out retired destinations before its work limit, including old queued and scheduled TikTok records. Historical records retain their original status and provider IDs, without falsely claiming remotely scheduled posts were cancelled.
- TikTok media tokens no longer serve approved video assets. Existing historical links remain readable.
- Admin controls default to Instagram only; TikTok retry/reconciliation controls are removed. Historical rows explain that posting is disabled. Old schedule timestamps are labelled as recorded schedules.
- Source/approval checks, Instagram scheduling/idempotency and all pause configuration remain unchanged.

## Verification and release

Four executed frontend control tests pass, including the actual Instagram-only payload and no TikTok retry controls. Initial Linux backend run: 105 pass and three legacy rendering-path tests fail because the local environment lacks the pinned Pillow dependency; no publisher failure. Installing the existing project dependency and rerunning are pending at this checkpoint. New tests cover API rejection, direct client transport prevention, historical states, media denial and an old TikTok row not starving Instagram with a one-item work limit.

Application changes are local on codex/retire-tiktok-posting. No production release, provider request, post, customer email or automation activation occurred. Existing source-migration work is uncommitted and excluded from this change. External Buffer queues have not been inspected or changed; disconnection is owner-reported.

Final local validation: all 108 affected backend checks pass on Linux Python 3.12 after installing the project's already-pinned Pillow 12.3.0 in the test environment. All four frontend behavior checks and TypeScript no-emit validation pass. Read-only release preflight finds zero protected unfinished/running jobs and zero pending queue jobs; the prior failed portfolio request remains unchanged. Release remains pending.
