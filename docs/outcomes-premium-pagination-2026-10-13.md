# Outcomes Premium pagination

Owner request, October 13, 2026: Free users should receive only the first ten Outcomes rows. Next and Last must open the same Premium gate as the 25-row option.

## Implementation

[PR627](https://github.com/moore11j/congress-tracker/pull/627), source `dfc8c61f`, merged as `0bb5d0f4`.

- Next, Last and row-size controls share the existing upgrade modal. Free rendering clamps to page zero immediately, including when entitlement state changes. Controls wrap on narrow screens.
- Both public overview and snapshot routes project at most ten rows after reading existing prepared caches. Later public pages return 402. Summary aggregates and stored full caches are preserved.
- `/api/outcomes/premium/snapshots` requires an authenticated Premium, Pro or admin account and returns `private, no-store`. It is excluded from the shared HTTP response cache. The frontend fetches this endpoint only after verified entitlement resolution, rather than trusting local plan hints.
- Versioned public request URLs avoid reuse of old frontend/browser responses containing 500 rows. Ticker search and horizon filters retain their existing scope, with ten preview rows per result; this does not prohibit searching other tickers.

## Validation and release

- Python 3.14.2: 38 backend checks pass, including public projection of old full caches, pagination rejection, anonymous/spoofed-header denial, Free denial, Premium/Pro/admin access and cache isolation. Existing ledger preparation and event tests pass.
- Frontend: 20 focused checks pass, including executed Next/Last/25-row handlers for Free and Premium. TypeScript no-emit check passes. Vercel preview build succeeds; secret scan succeeds.
- Both dependency audits fail in unchanged dependencies. No dependency manifests or lockfiles changed; no full-suite pass claimed.
- Backend workflow `38521840338` succeeds. Fly image and frontend `/api/app-version` report `0bb5d0f436a746f1df33d16a358f3a620d55da44`. Normalized `backend/app/main.py` SHA256 matches production: `93654afa849f39b32fff0b80c3a0813582f867fe29736b990187a8ad8965ce91`.
- Live public overview and snapshots each return ten rows of 3,103 total, page zero, preview locked and no next page (0.944/0.952 seconds). Public page1 returns402; private snapshots return401 anonymously even with a spoofed admin-tier header. Readiness/database pass.
- Live browser Next and Last both open the same Premium upgrade modal and retain Showing1-10/page1. At390px mobile width the modal fits and Next works. Temporary viewport restored and test tab closed.
- Paid behavior is verified in isolated backend and executed frontend handler checks; no signed-in paid production account was used. Live request evidence is retained locally at `artifacts/outcomes-live-check.json`.

No provider, scheduled job, hosting size, pricing, landing-page design or video change. Wider reliability and FMP cutover gates remain open.
