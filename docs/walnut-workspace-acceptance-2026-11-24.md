# Walnut workspace acceptance

November 24, 2026. Draft PR846 remains unreleased. No production flag changes.

The exact `676790b0491cc5bca2609e2958570fcbb6037714` Vercel preview `FzRMySgvRymWjXywrGXQNynLovEi` is Ready at `https://congress-tracker-mzpjiemej-moore11js-projects.vercel.app`. Browser inspection verifies guest ranks3/4/5 with protected numeric scores absent, noindex metadata, and no horizontal overflow at1920desktop and390x844mobile. My Stocks redirects guests to login, preserving all values in the test query including repeated filters and the encoded nested ticker return path. No browser console errors were observed. The temporary viewport was reset and the owned tab closed. Initial tab creation timed out, but reconnecting to the returned Chrome session recovered the same tab without creating a duplicate.

The visible ranking timestamp was November20,2026 at9PM EST. This observation is not proof of fresh November23 rankings. Freshness remains an acceptance gap; no refresh or provider activation was triggered. The preview has no authenticated Walnut session, so populated authenticated browser checks remain unverified.

Nine new lifecycle cases execute the actual Discover and WatchlistsDashboard effects with isolated API fixtures. They cover Free/Premium/Pro/admin populated responses, an add-ticker URL opening the creation form without a write, failure distinct from empty results, explicit retry and ignored late responses after unmount. Unexpected API calls, direct fetches, timers, navigation and storage writes are recorded and fail the checks even if a component catches their exception. These cases pass locally.

These are component/API-boundary checks using a controlled hook harness. Child components are opaque, and HTTP transport, authentication and browser hydration are outside that harness. They do not close full authenticated browser or passive network acceptance. The existing16backend signed-session tests separately cover ownership and current-plan visibility; prior12frontend/render/auth/middleware checks remain required. CI includes the new lifecycle cases.

The institutional replay continues independently in its immutable checkout. No recopy, reload, duplicate worker, provider request, production write, merge, release or customer send is part of this acceptance work.
