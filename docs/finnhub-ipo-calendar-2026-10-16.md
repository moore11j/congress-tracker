# Free IPO calendar replacement, October 16, 2026

## Source evidence

The existing free Finnhub account successfully reads `/calendar/ipo`. Four bounded requests returned HTTP 200: October 1–31 has 32 rows; October 1–15 has 31 and October 16–31 has one. The complete row multiset from the two nonoverlapping windows exactly equals the monthly response. November 1–30 returns an empty `ipoCalendar` array. This verifies the returned windows and current account access, not independent completeness against all market IPOs or future subscription availability.

The [official endpoint documentation](https://finnhub.io/docs/api/ipo-calendar) and the pinned official OpenAPI revision `8071bcd46ca5101adcef7a844c96b81a3eceb76e` specify expected, priced, filed and withdrawn statuses. The response has no offering identifier, currency or confirmed first trading date. Some records have missing symbols. New Iceland Arctic Acquisition has both filed and withdrawn observations in the October source; these remain distinct.

The probes use header authentication, disable redirects, bound time/response size and reserve the existing shared Finnhub request budget. Only public offering fields are exported. Budget reservations are production state writes; there are no canonical-event writes, subscription changes or customer messages. Receipts are `artifacts/finnhub-ipo-probe-{1792114855,1792115367,1792115373,1792115379}.json`. The complete public fixture and a four-response provenance manifest are tracked under `backend/tests/fixtures/finnhub_ipo_oct2026*`.

## Replacement behavior

`finnhub_ipo_calendar.py` validates the complete monthly response before writing a versioned raw-plus-normalized `InsightsSnapshot`. It rejects invalid dates/windows/statuses/symbols/numbers, nonfinite values, conflicting observations and bounded/truncated-looking responses. A failed refresh preserves the prior snapshot and its clock. Readers revalidate the raw response against the stored projection, require an exact source version and reject stale, future-dated or modified caches. Freshness is 24 hours. Provider dates are explicitly labeled as status dates; prices retain their supplied string or range, with no currency inference.

Identity is a source status observation keyed by company name and status. It is not a canonical security, verified offering ID or transaction identity. Exact repeats collapse. A corrected date/price retains the same status identity; ambiguous same-name/status offerings are held instead of merged. Conflicting observations across requested months are omitted with an explicit coverage error. Raw observations remain available in their snapshots. Existing canonical events are neither created nor rewritten.

The selected free calendar can read this cache through `IPO_CALENDAR_PROVIDER=finnhub`, default `disabled`. IPO scope remains global within the watchlist calendar, matching the existing contract. The existing enrichment job can prepare an explicit monthly IPO scope. Only when the IPO selector is enabled does the existing free-research warmer add current and next month; no separate schedule is created. Preparation requires the selected free calendar or explicit research warming, plus the shared request limiter. Page/digest readers do not fetch providers synchronously. Missing data stays a coverage error and there is no FMP fallback for this selected capability.

Titles preserve the source phase, for example `Company IPO: filed`. The calendar subtitle and digest detail explicitly state that the first trading date is unconfirmed. This repairs the existing IPO digest formatter, which otherwise replaces the subtitle with the company name. Existing display limits, calendar subscriptions, Premium entitlement and opt-out behavior remain intact. No frontend layout, pricing, landing-page or ranking change is included.

## Validation

205 distinct affected tests pass under Linux Python 3.12.3: 203 source/cache/calendar/digest, free calendar contracts, warming, transport and email-digest checks, followed by two additional direct transport/queue-dispatch checks. The suite checks all 32 complete public records, field preservation, source phase/date semantics, duplicate/correction identities, invalid payload rejection, freshness/tamper holds, failed-refresh preservation, real calendar and signal-digest replay, access/opt-out gates and existing warming behavior. Earlier test failures were fixture setup and expectations for the pre-existing three-items-per-kind digest limit; the final regression run passes without changing that limit.

The seven-source-hash installed verifier is prepared in `artifacts/verify-ipo-runtime-oct16.py`; local Windows Python 3.14.2 rehearsal passes (`ipo-runtime-1792115544.json`). It uses isolated SQLite, forbids network and replays all 32 originals through cache/calendar and digest rendering, with exact repeat and tamper rejection. Complete projection digest is `b11d5a0e9ca45c86227b6dba2bd3aae4fb8fe7cd75e6f7fcd62a63a4e0456d4f`. This is a local rehearsal, not installed-production proof or ordinary scheduled operation.

## Release and remaining gates

Implementation is local on `codex/finnhub-ipo-calendar`; review, merge, installed validation, bounded preparation and ordinary selected operation remain pending. No production IPO selector is changed. The latest guard receipt `release-blocker-1792115156.json` still has one protected continuing portfolio cycle at 603/1,135 scopes, zero failures and 11 pending jobs. No portfolio PID is live at that observation; the unfinished durable cycle still prohibits rollout. PR694–701 remain merged but undeployed in the latest evidence. No guard bypass or deployment retry was attempted.

This closes an implementation gap for IPO status dates only. Dividend/split calendars, complete economic coverage, financial coverage, institutional activation, transcripts, remaining analyst/valuation requirements, all scheduled consumers and sustained performance still require acceptance. Massive prices remain last; FMP remains enabled. Global zero-FMP traffic and billing closure are not verified. Prior status-only goal turn was no progress; this turn produces public-source evidence, implementation and consumer tests.

## Reviewed merge, October16 01:56 UTC

[PR702](https://github.com/moore11j/congress-tracker/pull/702), tested `9ca4eb8f6da1b051dc6eddbda563cb1c58301108`, merged `6852066e4452d45e27d58be8659de7dbba66cd2a` at 01:56:28 UTC. Backend/frontend dependency audits and secret scan all succeed in workflow `38837668249`; Vercel preview succeeds. Fetched merged application and test content exactly matches the tested revision. The chat attachment limit of 100 prevents another native PR attachment; the ordinary PR URL remains usable.

Post-merge guard `release-blocker-1792115813.json` reports one protected active claim at 746/1,135 scopes, zero failures and zero pending jobs. A live portfolio PID was not confirmed by this observation; the unfinished protected cycle is sufficient to defer deployment. PR694–702 remain undeployed. No bypass or rollout retry was attempted.

Four public response samples (`public-render-1792115781050.json`) return 200 with no RSC/server-error markers: Feed 0.576s, Insights 1.851s, AAPL 0.310s and NVDA 0.466s. An initial sandbox-restricted fetch failed before receiving responses; the approved network read succeeds. These are public-render spot checks, not signed-in/data-completeness or sustained-capacity acceptance.

A final portability check found the fixture manifest hashed the working file's Windows CRLF bytes, while Git stores LF bytes. The public records and original provider-response hashes are unchanged. A follow-up explicitly defines fixture hashing as UTF-8/LF and adds a Linux-verifiable assertion; canonical fixture SHA256 is `04d95f64c58233211cf4c8c3d506106d0a461dcdbf329f6ee9d605ea1565e551`. The correction changes evidence metadata only, with no application behavior change. Its review/merge state is recorded in the subsequent checkpoint.
