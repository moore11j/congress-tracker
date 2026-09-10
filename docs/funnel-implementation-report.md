# Walnut funnel implementation report

September 10, 2026. Local implementation only; no deployment, billing changes, account creation, or marketing workflow.

## Findings and changes

The pre-implementation audit is in [funnel-analytics-audit.md](funnel-analytics-audit.md). Missing named HeyCatch events, inconsistent legacy names, GA return-navigation omissions, rejection of dotted ticker routes, and backend deduplication across different tickers explain concrete measurement gaps. Anonymous custom events lacked session linkage, authenticated pageviews dropped it, source context disappeared during navigation, and HeyCatch lacked environment/consent guards. The historical HeyCatch funnel configuration and six-click replay were not independently inspected.

A typed trackEvent facade now sends canonical client events to first-party storage, HeyCatch, and GA4. Existing HeyCatch autocaptured pageviews remain owned by its SDK. Canonical route events have one Next.js owner, avoid rerender duplicates, support dotted tickers, and count A → B → A correctly. Exact delivery IDs replace normalized-route/time-window suppression. Authentication completion is emitted only after successful responses; Google new-user detection comes from the backend. Watchlist, alert, and follow events require successful actions. Visibility events require an intersecting element in a visible document.

Session and original acquisition labels persist across the two production subdomains through bounded first-party cookies. Both anonymous and authenticated records retain the same session hash. Identity uses an internal account ID; canonical properties exclude emails, names, passwords, tokens, arbitrary query strings, and full referrers. Backend authentication/plan context is authoritative. Transmission requires consent, an HTTPS production hostname, and a production build; Vercel preview builds are excluded. NEXT_PUBLIC_ANALYTICS_DEBUG=1 prints sanitized JSON locally without sending it to production.

Paid completion is recorded from a verified live Stripe initial paid invoice, deduplicated by invoice and linked to a previous recorded checkout. Client requests cannot fabricate subscription_completed. Analytics failures are isolated from billing transactions.

## Product changes

- Homepage: direct Open Screener, View Leaderboards, and Explore Strategies paths; outcome/ranking-focused hero copy; existing layout retained.
- Upgrades: contextual inline benefits and Premium/Pro CTAs, visible impressions and clicks, preserved three-row leaderboard previews and all protected metrics/filters. Empty/error data is not replaced with invented rankings.
- Pricing: Premium marked Recommended, Pro explains its professional datasets. Signup preserves plan and interval using unique query keys, and annual selection is restored on return. Free/Premium/Pro, prices, cadence, and entitlement rules are unchanged. No email capture, exit popups, or re-engagement automation was added.
- Ticker discovery: existing insider/Congress source cards link to the corresponding ticker-filtered activity only when supported by the data; institutional links additionally require the existing entitlement. Existing historical top-match data links to that ticker's Outcomes history, with a visible ticker filter and a return to all tickers. Existing follow, comparison, tabs, and research controls are retained; no stock–strategy relationship is invented.
- Outcomes Opened: a consistently broken sort was not reproduced. Original desktop and mobile local behavior changed row order correctly. In live Chrome, one initial pointer attempt did nothing, then Enter and a subsequent pointer click sorted ascending and descending. No intercepting element was found at the button center. The original 16-pixel-high target is now a full-width, minimum 44-pixel button with focus styling and aria-sort. Sorting logic, return calculations, certification, and reconstructed ledger data were not changed.

## Canonical event contract

| Group | Events |
| --- | --- |
| Acquisition | homepage_viewed |
| Discovery | screener_opened, screener_result_clicked, leaderboard_viewed, leaderboard_entity_clicked, strategy_list_viewed, strategy_viewed |
| Research | ticker_viewed, congress_trades_viewed, insider_activity_viewed, institutional_activity_viewed, outcomes_viewed, confirmation_score_viewed |
| Conversion | upgrade_prompt_viewed, upgrade_prompt_clicked, pricing_viewed, signup_started, signup_completed, signin_started, signin_completed, checkout_started, subscription_completed |
| Monitoring | watchlist_created, ticker_added_to_watchlist, strategy_followed, alert_created |
| Contextual ticker discovery | ticker_related_content_viewed, ticker_related_content_clicked |

Properties include route/source/destination paths, ticker or entity/strategy identifiers, leaderboard type, gated feature, target/current plan, authentication, billing interval, acquisition labels, and destination_type/destination_id where applicable. A viewed route is not counted as an action or a successful payment. Distinct gate components are separate impressions; count unique sessions for conversion rates.

## Validation

- Focused frontend: 68 passed, including new facade/schema, route/deduplication, source/session, gate/CTA, success-only follow, sort-order, and supported-discovery checks.
- Full frontend: 494 passed, 46 failed (540 total). Original code baseline: 483 passed, the same 46 failed (529 total). No additional failing test names remain.
- New backend funnel tests: 6 passed. Relevant accounts/admin/Stripe suite: 128 passed, 8 failed, 1 error; the original-code baseline reproduced the same eight failures and one error. These include existing account/admin, billing configuration, export and concurrent Stripe fixture failures.
- TypeScript (`npx tsc --noEmit`) and optimized production build (`npm run build`): passed. A development-generated screener route-check artifact initially contaminated the build check; removing that temporary QA include restored the normal build inputs.
- Browser: original and changed Outcomes sorting verified locally on desktop and a 390×844 viewport; live Chrome admin session verified ascending/descending sorting. Homepage CTA destinations, screener entry/gated state, pricing tiers, annual selection, and pricing → registration URL were checked. Chrome captured homepage_viewed, screener_opened, leaderboard_viewed, and contextual impression payloads. Later Chrome local-tab interactions timed out, so a complete uninterrupted leaderboard → pricing → signup payload chain is not claimed.
- The local fixture has no real screener results, ticker source/peer data, or working account/payment services. Full result → ticker, data-backed ticker module clicks, real signup/OAuth, checkout, and webhook delivery were therefore not completed in the browser. They are covered only to the extent described by automated tests and code inspection.

## Remaining measurement limits

subscription_completed is currently first-party/server-only; it is not forwarded into HeyCatch or GA4. Use the first-party records joined to Stripe's existing billing ledger for checkout-to-paid reporting. A missing/blocked checkout event intentionally prevents attributed paid-funnel insertion; billing itself remains authoritative. The checkout record supplies consent context at checkout time, not a later persistent consent history.

Consent opt-outs, blockers, offline navigation, SDK delivery failures, and unknown auth during API outages can undercount. Anonymous return sessions across cookie deletion/devices cannot be reliably joined; known account returns can. The bounded session attribution is not a multi-touch attribution platform. Exact delivery deduplication is best effort under concurrent requests, without a new database uniqueness migration. Legacy events remain for existing reports: do not add canonical and legacy totals together. Historical contamination is not removed or backfilled. Dashboard funnel definitions still need to use the canonical names after deployment.

## KPIs for the next 30 days

Use distinct consented sessions for same-session activation, distinct linked accounts for post-signup cohorts, chronological steps, and one provider/data source per metric. Establish a baseline before optimizing prices. Report sample sizes alongside rates; do not treat missing events as confirmed abandonment. Define a meaningful interaction as a result/entity/contextual-content click, strategy-detail visit from the list, or a successful monitoring action, not merely a landing/list impression.

| KPI | Numerator / denominator |
| --- | --- |
| Landing → meaningful product interaction | Landing sessions with a later meaningful interaction / homepage_viewed sessions |
| Landing → Screener | Landing sessions reaching screener_opened / landing sessions |
| Landing → Leaderboards | Landing sessions reaching leaderboard_viewed / landing sessions |
| Landing → Strategies | Landing sessions reaching strategy_list_viewed / landing sessions |
| Product interaction → ticker/profile/strategy detail | Interacting sessions reaching ticker_viewed, a profile destination, or strategy_viewed / interacting sessions |
| Product interaction → signup | Interacting sessions/accounts completing signup / interacting sessions |
| Gated feature → upgrade click | Sessions with upgrade_prompt_clicked after that feature's visible impression / sessions seeing that feature |
| Upgrade click → pricing | Clicking sessions with subsequent pricing_viewed / upgrade-clicking sessions |
| Pricing → signup | Pricing sessions linked to subsequent signup_completed / anonymous pricing sessions |
| Signup → checkout | New accounts with checkout_started within 7 days / signup_completed accounts |
| Checkout → paid | Checkout accounts linked to authoritative subscription_completed within 7 days / checkout accounts; reconcile with Stripe billing totals |
| New user → return session | New accounts active in another session on a later day within 7 and 30 days / new accounts old enough for the window |
| Conversion by acquisition source | Above activation/signup/paid rates split by original source/UTM campaign, retaining direct and unknown buckets |

Profile visits can be joined through existing first-party pageviews and the entity click destination; no artificial profile event was added. Use 7-day and 30-day maturity windows only after cohorts have had time to return.

## Files changed

The generated QA TypeScript/configuration files were restored to their original contents.

- `backend/app/routers/accounts.py`
- `backend/tests/test_funnel_analytics.py`
- `docs/funnel-analytics-audit.md`
- `frontend/app/auth/google/callback/page.tsx`
- `frontend/app/landing/page.tsx`
- `frontend/app/layout.tsx`
- `frontend/app/outcomes/page.tsx`
- `frontend/app/ticker/[symbol]/page.tsx`
- `frontend/components/CookieConsentManager.tsx`
- `frontend/components/PageAnalyticsTracker.tsx`
- `frontend/components/analytics/VisibleEvent.tsx`
- `frontend/components/auth/LoginRegisterPanel.tsx`
- `frontend/components/billing/ContextualUpgrade.tsx`
- `frontend/components/billing/PremiumFeatureGate.tsx`
- `frontend/components/billing/PricingActions.tsx`
- `frontend/components/billing/PricingPlanner.tsx`
- `frontend/components/billing/UpgradePrompt.tsx`
- `frontend/components/leaderboards/LeaderboardsDashboard.tsx`
- `frontend/components/outcomes/OutcomeLedgerClient.tsx`
- `frontend/components/screener/ClickableScreenerRow.tsx`
- `frontend/components/strategies/StrategyFollowButton.tsx`
- `frontend/components/ticker/TickerDiscoveryLink.tsx`
- `frontend/components/ticker/TickerInstitutionalSourceCardClient.tsx`
- `frontend/instrumentation-client.ts`
- `frontend/lib/analyticsContext.ts`
- `frontend/lib/analyticsEnvironment.ts`
- `frontend/lib/api.ts`
- `frontend/lib/funnelEvents.ts`
- `frontend/lib/googleAnalytics.ts`
- `frontend/lib/heycatch.ts`
- `frontend/lib/homepageContent.ts`
- `frontend/lib/productAnalytics.ts`
- `frontend/tests/admin-users-view.test.mjs`
- `frontend/tests/cookie-consent.test.mjs`
- `frontend/tests/funnel-analytics.test.mjs`
- `frontend/tests/landing-polish.test.mjs`
- `frontend/tests/leaderboards-page.test.mjs`
- `frontend/tests/outcome-integrity.test.mjs`
- `frontend/tests/ticker-hydration-contract.test.mjs`
