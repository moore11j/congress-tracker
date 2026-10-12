# Insider freshness request accounting, October 12, 2026

The selected SEC Form 4 feed already prevented the legacy freshness HTTP request, but its wrapper called `ensure_fmp_live_allowed` before checking feed ownership. That function also consumes budget and records a provider_call. The FMP transport then called it again. Skipped work therefore appeared as a request and enabled legacy work could be counted twice.

The repair checks ownership first and retains the global retirement, user-access and budget guard immediately before HTTP. Missing credentials, paused or SEC-selected feeds and failed ownership remain closed. This does not activate a provider or change ingestion schedules.

## Verification

Python 3.14.2: 21 insider ownership and provider usage tests pass, including five cases using the real budget counter. The initial fixture incorrectly used the direct-source selection API to select FMP; that API properly refused. The fixture now uses default legacy ownership without weakening its guard. A Windows newline mismatch initially left that fixture unchanged, then was corrected.

Read-only production comparison `insider-counter-runtime-1791823783.json` uses actual current ownership, disables telemetry persistence and forbids HTTP with a BaseException guard. Deployed code records one provider_call and zero HTTP attempts; candidate code records zero and zero. No source calls, canonical writes or emails. This reproduces the accounting defect independently of the historical ledger.

The broader provider_call ledger still measures permitted attempts, not independently observed network egress. This fix does not establish globally zero FMP traffic. Prices, core fundamentals and transcripts remain active dependencies; institutions, broader coverage, rankings/digests/watchlist consumers and final Massive validation remain gates.

Release pending. No source selection, subscription, host size, recurring schedule, video, landing-page or billing change.
