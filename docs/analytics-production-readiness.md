# Analytics production readiness — 2026-09-09

> Subsequent authorization: the user approved committing and deploying this implementation. The assessment below records the pre-deployment evidence and remaining analytics setup; it is not a deployment prohibition after that approval.

**Recommendation: NO-GO for declaring the production funnel fully verified.** The code and build checks pass with zero new failing test names. Provider configuration/receipt and actual production cookie inspection remain open. No deployment, account creation, purchase, or dashboard funnel change was performed.

This pass extends commit `02a8ec73` only for analytics completeness, consent/session handling, and event ordering. Pricing, entitlements, Outcomes/Confirmation Score calculations, and product design are unchanged.

## 1. HeyCatch canonical-event compatibility

**SDK transport compatibility verified; ingestion and dashboard classification are not yet verified.** Installed `@heycatch/sdk` version 0.7.0 supports these custom names unchanged:

`homepage_viewed`, `screener_opened`, `ticker_viewed`, `leaderboard_viewed`, `pricing_viewed`, `signup_started`, `signup_completed`, `checkout_started`.

Tests execute the installed browser SDK's event function with a capture spy, checking the exact name and `heycatch_custom_user_event: true`. A separate test executes the installed Node SDK, intercepts its outgoing request, decompresses its actual gzip payload, and verifies the canonical event, project key, project group, and stable account distinct ID. These checks go beyond successfully calling Walnut's facade, but are not evidence of real provider ingestion.

The production HeyCatch dashboard was inspected through Chrome. It showed a **Daily** conversion funnel, automatic change history adding/removing named steps, and “purchase event not detected yet.” Its current steps still use historical labels such as “Browsed leaderboards” and “Viewed pricing.” The settings control opened SDK installation instructions; no supported manual funnel-step editor was found. No repository-owned dashboard definition or documented funnel-management API was found. The observed funnel is managed in HeyCatch; automatic custom-event tagging is verified, but automatic mapping of these particular names into its daily funnel is not established.

After deployment, confirm incoming custom events in HeyCatch, then use its supported configuration or support channel to apply/confirm the definitions below. Do not assume the daily model will choose them, or that renaming a display label changes its underlying event predicate. A dashboard refresh was attempted; later Chrome debugger disconnections prevented inspecting fresh provider results.

## 2. Exact funnel definitions

Use exact event-name equality, ordered steps with intervening events allowed, and unique-user conversion counts. Keep discovery paths separate. Suggested discovery window: one session; suggested signup-to-paid window: seven days. These are recommended definitions, not claims that the current HeyCatch UI exposes those controls.

| Funnel | Ordered events |
| --- | --- |
| Screener activation | `homepage_viewed` → `screener_opened` → `screener_result_clicked` → `ticker_viewed` |
| Leaderboard upgrade interest | `homepage_viewed` → `leaderboard_viewed` → `upgrade_prompt_viewed` → `upgrade_prompt_clicked` → `pricing_viewed` |
| New account to paid | `pricing_viewed` → `signup_started` → `signup_completed` → `checkout_started` → `subscription_completed` |
| Existing account to paid | `pricing_viewed` → `checkout_started` → `subscription_completed` |

For the stock leaderboard branch, filter both upgrade steps to `gated_feature = stock_leaderboard`. Other branches can use `congress_leaderboard`, `insider_leaderboard`, or `institutional_leaderboard`. Multiple visible gates are separate impressions, not duplicate page visits. Break down conversion by `acquisition_source`, `utm_campaign`, `current_plan`, `target_plan`, and `billing_interval` when those fields exist on the relevant event. Use `gated_feature` for impression-to-click matching. Do not require signup again for existing accounts or use a client success-page view as paid conversion.

## 3. Verified paid conversion → HeyCatch

**Supported and implemented through the documented installed server SDK.** `node_modules/@heycatch/sdk/dist/server.d.ts` specifies `analytics.init({projectKey})` and awaited `analytics.trackEvent(name, properties, {userId})`. The Python billing service calls a small authenticated Next.js Node bridge, which uses that API.

The bridge accepts only a row ID and a fresh, scoped HMAC signature. It retrieves and atomically claims a persisted, authoritative Stripe analytics row from the backend; it never accepts paid status, account identity, or plan from the initiating request. Browser product-event requests still reject `subscription_completed`. Invoice deduplication serializes competing Stripe event types for an account; per-provider compare-and-swap claims are committed before sending. Replayed bridge requests cannot obtain another delivery claim.

Provider requests run after the Stripe transaction commits. Network and database dispatch failures are caught. Billing success therefore does not depend on either provider. The first-party row remains intact.

Delivery is deliberately **at most one application-level attempt per provider**, not guaranteed exactly-once receipt. A crash after claiming can lose an external delivery; an ambiguous attempt is never automatically resent. The SDK has its own transport behavior and catches errors internally, so its resolved promise means attempted, not confirmed ingestion. Inspect provider logs and durable `forwarding` metadata before recovery; do not clear claims casually. `forward_pending_paid_events()` can dispatch unclaimed recent rows, examines at most the latest 100 from 72 hours, and is not scheduled automatically.

## 4. Verified paid conversion → GA4

**Supported and implemented with GA4 Measurement Protocol.** At consented checkout start, supported `gtag('get', ...)` calls obtain the existing client and session IDs with a 250 ms bound. Those identifiers go only to the first-party checkout record. Missing/blocked identifiers skip GA4 forwarding; no synthetic identifier is invented. The verified webhook supplies the server account ID, payment event timestamp, and restricted properties. Ad-user-data and ad-personalization consent are explicitly denied in this server event.

The actual payload builder was checked against Google's strict validation-only endpoint: HTTP 200, `validationMessages: []`. This request recorded no production event and used a placeholder secret; it proves payload validity, **not production credentials or ingestion**. See `analytics-readiness-ga-validation.json`. Google's [Measurement Protocol reference](https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference?client_type=gtag), [supported gtag getters](https://developers.google.com/tag-platform/gtagjs/reference#get), and [validation documentation](https://developers.google.com/analytics/devguides/collection/protocol/ga4/validating-events) describe these mechanisms. A normal collection 2xx response is recorded as received, not treated as proof of processed analytics.

## 5. Cross-subdomain attribution

**Production navigation observed; cookie contract tested; actual browser cookie attributes/values remain unverified.** Chrome traversed `walnutmarkets.com/?utm_source=reddit&utm_campaign=test` → `app.walnutmarkets.com/screener`, returned to marketing, and then navigated internally through leaderboards, pricing, and registration. The available browser inspection surface did not expose cookie storage/network payloads, and these cookie fixes are not deployed. Page rendering cannot prove attribution continuity.

The source audit found a real consent gap: host-local consent could disagree across the two domains. The fix shares the consent cookie and gives any coexisting legacy opt-out precedence. Consent is written before notifying listeners; opting out clears the shared session/acquisition cookies. Existing default analytics-consent behavior is preserved.

| Cookie | Domain / path | SameSite / Secure | Expiration |
| --- | --- | --- | --- |
| `ct_analytics_sid` | `walnutmarkets.com` / `/` | Lax / yes | 1,800 seconds, refreshed on activity |
| `walnut_acquisition` | `walnutmarkets.com` / `/` | Lax / yes | 1,800 seconds, refreshed on activity |
| `walnut_privacy_consent` | `walnutmarkets.com` / `/` on the two HTTPS production hosts | Lax / yes | 180 days |

Tests execute the real consent/context modules with a domain-aware, expiring cookie simulation. They verify Reddit campaign retention across marketing→app→internal pricing, unchanged session ID on anonymous→authenticated transition, inactivity rotation, and cross-host opt-out. These tests do not substitute for Chrome's production cookie store. Local debug intentionally does not write production cookies, so full local document navigation resets acquisition to direct. No new email/address fields, fingerprint, authentication token, or arbitrary URL queries are persisted in this analytics context.

## 6. End-to-end QA

**Production Chrome, guest after the user logged out:** homepage→screener; homepage→leaderboards→“Unlock with Premium”→pricing; annual Premium→registration all rendered successfully. The final production URL retained `return_to=/pricing?plan=premium&interval=annual`. No account was submitted and no checkout/purchase was created.

**Updated local code, one uninterrupted Chrome sequence after the final ordering fix:**

| UTC time on September 10 | Observed canonical event | Key context |
| --- | --- | --- |
| 06:14:49.936 | `homepage_viewed` | Reddit / `readiness_qa`, guest |
| 06:15:24.584 | `screener_opened` | source `/landing` |
| 06:15:24.933 | `upgrade_prompt_viewed` | `premium_results`, guest |
| 06:15:37.846 | `homepage_viewed` | Second homepage visit counts |
| 06:15:57.294 | `leaderboard_viewed` | source `/landing` |
| 06:15:57.348 | `upgrade_prompt_viewed` | `stock_leaderboard`, target Premium |
| 06:16:07.145 | `upgrade_prompt_clicked` | source `/leaderboards`, destination `/pricing` |
| 06:16:11.597 | `pricing_viewed` | source `/leaderboards` |
| 06:16:27.256 | `signup_started` | registration form, destination `/pricing` |

Other visible leaderboard gates each emitted once with their distinct feature IDs. Changing monthly→annual did not produce another pricing view. The signup URL preserved annual Premium. A discovered race allowed visible impressions to beat the page event while identity loaded; impressions now wait for route readiness, and the corrected order above was observed. Automated tests cover repeated renders, A→B→A, stale cancelled lookups, dotted `BRK.B` route context, and sanitized ticker/entity paths. A separate Chrome navigation to `/ticker/BRK.B` emitted one `ticker_viewed` at 06:19:45.652 UTC with `ticker: BRK.B`; the fixture has no company data and rendered its not-found state, so this verifies symbol routing and event context only.

**Still unverified:** real-provider event ordering/counts; native browser Back deduplication beyond the automated visit-state checks; authenticated signup completion→Stripe checkout in a safe deployed test environment; actual anonymous→authenticated provider identity stitching. No explicitly configured usable Stripe test environment or existing test account was identified. The fixture cannot create accounts or run checkout. Production cookie/provider verification must not be inferred from local logs or simulated storage tests.

## 7. Manual configuration and release checks

Before enabling forwarding, set `GA4_API_SECRET` on the production Python service for existing stream `G-QQTFFK7FBH`; set the same strong, random `ANALYTICS_FORWARDING_SECRET` (at least 32 characters) on the production Python service and Next.js app; retain the actual `NEXT_PUBLIC_HEYCATCH_PROJECT_KEY` beginning `hck_pk_` on the Next.js app. The signing secret must never have a `NEXT_PUBLIC_` prefix. Verify production environment guards and connectivity between the fixed app/backend bridge URLs. Production secret presence was not inspected or changed; the local environment lacks these provider settings.

After an approved deployment: inspect the three cookies in Chrome on both hosts; verify the same consented session and original campaign through an internal navigation and login; check real canonical events in both providers; configure/confirm the four funnel definitions via supported HeyCatch controls/support; mark `subscription_completed` as a GA4 key event and register desired custom reporting dimensions. Verify the next legitimate, consented paid conversion against its authoritative first-party invoice row and provider attempt state. Do not create a real payment for this check. Safe test-mode billing must use an isolated test analytics destination; test Stripe events remain excluded from the production funnel.

## 8. Regression evidence

| Check | Result |
| --- | --- |
| Original frontend baseline | 483 passed, 46 failed (`frontend-funnel-baseline-complete.log`) |
| Final frontend suite | 500 passed, 46 failed, 546 total (`analytics-readiness-frontend-final.log`) |
| Frontend failing-name comparison | Exact same 46 names; zero added or removed |
| Original backend comparison | 128 passed, 8 failed, 1 error (`backend-funnel-baseline.log`) |
| Current backend comparison suite | 140 passed, 8 failed, 1 error (`analytics-readiness-backend-full.log`) |
| Backend failing-name comparison | Exact same 8 failure names and 1 error; zero added or removed |
| Final focused backend analytics checks | 13 passed, including the subsequently added database-failure isolation test |
| TypeScript / production build | Both passed (`analytics-readiness-typecheck.log`, `analytics-readiness-build.log`) |

The original historical baseline was not rerun unnecessarily. Current changes were tested and compared against the existing conclusive baseline logs. No unrelated historical failure was repaired.

## 9. Release recommendation

**NO-GO for a fully verified analytics release at this point.** No new regression was found, and the implementation is ready for review. Remaining release evidence is production provider configuration, real custom-event recognition, actual shared-cookie inspection, and an isolated safe signup/checkout test. Some receipt/cookie checks necessarily require an approved rollout of these fixes; they are explicitly pending rather than reported as completed. Await approval before any deployment.


## Approved release follow-up — September 10 UTC

The user subsequently authorized commit and deployment. Implementation commit `3e2947ff` was pushed to `main` and deployed to Vercel and Fly. Both public hosts returned that exact revision from `/api/app-version`; backend `/ready` returned HTTP 200 with database status OK. The deployed guest homepage, leaderboard, and pricing were checked in Chrome.

A new random private `ANALYTICS_FORWARDING_SECRET` was configured on both production services without exposing its value or storing it in the repository. The existing production HeyCatch project key remains configured. `GA4_API_SECRET` is confirmed absent on Fly, so GA4 paid forwarding remains inactive until that stream-specific credential is supplied. HeyCatch dashboard mapping and actual paid-event receipt remain to be confirmed using a legitimate conversion.

The signed bridge smoke check exposed a PostgreSQL integer-range edge case for an oversized nonexistent event ID. The follow-up bounds the lookup to the database INTEGER range and treats out-of-range IDs as absent records before querying. Regression coverage verifies signed oversized requests return no event and never query the database. The focused backend analytics suite passes all 14 tests. This check creates no payment or analytics row.
