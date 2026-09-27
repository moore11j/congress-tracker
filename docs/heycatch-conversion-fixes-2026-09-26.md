# HeyCatch conversion fixes — implementation and release record

Based on production frontend revision `0e5605d28a968bb7f89d9da936c6be5b1290216d`, verified during the investigation. The owner subsequently approved committing and deploying the reviewed implementation. HeyCatch provider configuration remains unchanged; the reporting limitation below still applies.

## Changes

- Shared `UpgradeImpression` / `UpgradeLink` components use the existing consent, visibility, route-readiness and canonical-event pipeline. Links carry the required plan and a safe return destination through Pricing's existing signup/checkout flow. Analytics receives the destination pathname only, not the return URL.
- Existing Outcomes export/table, ticker ownership/valuation/analyst/macro, event calendar and research gates now emit canonical impressions and clicks. Research retains its existing legacy events and direct checkout/signup behavior.
- Leaderboard feature labels are explicit, so Congress and insider gates no longer get recorded as institutional gates. Screener results/export, strategy portfolio/follow and monitoring source gates have specific feature labels. CSV export targets its configured Premium or Pro tier.
- Top Stock Ideas copy describes the actual Top 5 → Top 10 benefit. Account settings offers an inline daily-ideas upgrade link after a confirmed entitlement denial; loading or failed entitlement checks do not show it.
- Admin identity is marked `is_internal` in browser events and HeyCatch identity. Identity refreshes after plan/admin changes. The first-party backend overrides this flag using authenticated identity and `ANALYTICS_EXCLUDED_USER_IDS`; browser clients do not receive that private test-user list.
- Strategies constrains the table's horizontal scrolling to its container, wraps long rule values, enlarges selection/filter targets, exposes pending and selected state, ignores duplicate selections, and focuses the selected panel on mobile after an explicit selection. Category counts come from the complete visible catalog, independent of the selected category, without exposing drafts.
- Local responsive verification uncovered an additional 1024px app-header overflow caused by the navigation's 34rem minimum width. The navigation can now shrink within its existing scroll container; scroll indicators remain available at desktop widths too.

No session-count offer, homepage redesign, Outcomes sorting change, or admin disclosure change was introduced.

## Canonical reporting contract

| Display label | Exact event name | Properties |
| --- | --- | --- |
| Viewed pricing | `pricing_viewed` | `route=/pricing`; path excludes queries |
| Viewed upgrade prompt / Saw upgrade prompt | `upgrade_prompt_viewed` | `gated_feature`, `target_plan` |
| Clicked upgrade | `upgrade_prompt_clicked` | Same feature/plan; `destination_page=/pricing`, or research's signup/checkout destination type |
| Checkout started | `checkout_started` | Emitted after a successful checkout URL response |
| Subscription completed | `subscription_completed` | Existing authoritative server payment event |

Do not add duplicate title-cased events to match dashboard labels. The installed SDK preserves the snake_case names and marks custom business events with `heycatch_custom_user_event=true`.

The production investigation found 12 `pricing_viewed` events across 9 browser sessions and 39 `upgrade_prompt_viewed` events across 14 sessions in the first-party 30-day report, with admin/test exclusion off. Those raw totals are **not** ordered-funnel counts or confirmed HeyCatch receipts. They disprove globally zero production events but do not prove HeyCatch's query or ingestion is correct.

HeyCatch's visible analytics settings expose SDK installation, and funnel changes are offered through “Edit in chat.” No raw event explorer or editable step predicates were exposed during inspection. Its saved predicates, ordering and ingestion remain unverified. No recommendation was marked complete in HeyCatch.

After an approved deployment, verify a consented non-admin journey on both production hosts: visible gate → pricing → signup/checkout. Compare exact raw event names, date range/timezone, identity stitching, optional-step ordering, attribution window, and internal-user exclusions before trusting funnel counts. Research can go directly to signup or checkout, so Pricing cannot be a mandatory predecessor for every paid journey. Configure `is_internal=true` exclusions in HeyCatch where supported, and separately exclude historical/configured test identities: the new flag does not retroactively classify old sessions.

## Validation

- Focused frontend analytics/conversion/navigation suite: 26 passing tests, including canonical SDK serialization, consent/readiness, impression deduplication, safe return paths, shared click properties, identity refresh/logout, and unchanged Outcomes sorting.
- Backend funnel and strategy-storage suites: 21 passing tests, including server-owned internal segmentation and category facets/draft isolation.
- Broader frontend checks: 100 passed, 24 failed. All 24 failures reproduce against the starting commit's source files; no new failures in that comparison. These are existing assertions in older leaderboard, profile, pricing and research tests.
- Full Next.js production build passed, including type checks and all 67 static pages. TypeScript check passed. Next-generated QA path changes were removed after stopping the local server.
- Local browser QA with synthetic strategy records: no document overflow at 320, 374, 390, 430, 640, 768, 1024 and 1280 CSS pixels; 44px strategy selection target; visible pending feedback; pointer/keyboard selection; mobile focus/scroll; cross-category switching without returning to All. A running-app Signals → Pricing comparison link and Pricing signup links preserved the Insider mode and selected plan. No production writes or checkout requests were made.

The repeatable fixture is `frontend/tests/strategies-visual-mock-server.mjs` (port 8086); run the frontend with `NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8086` on a separate local port. The backend and frontend should be released together for stable category metadata; the frontend falls back to the previous item-derived categories if the older backend is still serving requests.
