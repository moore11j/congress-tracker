# Insider freshness request accounting, October 12, 2026

The selected SEC Form 4 feed already prevented the legacy freshness HTTP request, but its wrapper called `ensure_fmp_live_allowed` before checking feed ownership. That function also consumes budget and records a provider_call. The FMP transport then called it again. Skipped work therefore appeared as a request and enabled legacy work could be counted twice.

The repair checks ownership first and retains the global retirement, user-access and budget guard immediately before HTTP. Missing credentials, paused or SEC-selected feeds and failed ownership remain closed. This does not activate a provider or change ingestion schedules.

## Verification

Python 3.14.2: 21 insider ownership and provider usage tests pass, including five cases using the real budget counter. The initial fixture incorrectly used the direct-source selection API to select FMP; that API properly refused. The fixture now uses default legacy ownership without weakening its guard. A Windows newline mismatch initially left that fixture unchanged, then was corrected.

Read-only production comparison `insider-counter-runtime-1791823783.json` uses actual current ownership, disables telemetry persistence and forbids HTTP with a BaseException guard. Deployed code records one provider_call and zero HTTP attempts; candidate code records zero and zero. No source calls, canonical writes or emails. This reproduces the accounting defect independently of the historical ledger.

The broader provider_call ledger still measures permitted attempts, not independently observed network egress. This fix does not establish globally zero FMP traffic. Prices, core fundamentals and transcripts remain active dependencies; institutions, broader coverage, rankings/digests/watchlist consumers and final Massive validation remain gates.

Release pending. No source selection, subscription, host size, recurring schedule, video, landing-page or billing change.

## Verified release

October12: Insider request accounting PR611/ace765f6 deployed38280316200SUCCESS. 21 focused tests pass; actual production read-only comparison reproduced one phantom counter/zeroHTTP before and zero/zero after. Deployed ingest source hash matches. Ownership precedes budget accounting, with the immediate transport/global-off guard preserved. Historical ledger counts remain permitted attempts, not independent egress proof. CoreFMP/institutionsfalse; prices/core/transcripts and remaining source/financial coverage, all ranking/digest/watchlist consumers, finalMassive/globalFMP/billing gates remain. No provider selection, host, subscription, schedule, customer email, landing/video change or cancellation. All local handles terminal/no localPG. Prior issuer release and current accounting release both make concrete progress; goalactive.

Runtime receipt `insider-counter-runtime-1791824058.json` verifies deployed zero request-budget entries and zero HTTP attempts against current selected ownership. The probe disables telemetry writes and uses read-only transactions. Earlier baseline receipt1791823783 reproduced the defect.

PR head2cb509041fee134e4710443987e09282085b1091: secret scan passes, dependency audits fail in unchanged Pillow10.4.0 and frontend packages. Both exact-head logs were reviewed with the CI diagnostics tool; transient502 recovered through that tool. Initial PR creation failed transiently; an empty head-branch listing preceded successful retry. Direct diff reviewed, no full-suite or automated-review claim. App PR attachment limit100 persists.

Public receipt issuer-staging-public-1791824050.json: all six reads return200; feed0.389/0.457seconds, AAPL1.197/NVDA1.306seconds, AZOpress0.347seconds and four unique releases. These samples do not prove sustained capacity or complete coverage.
