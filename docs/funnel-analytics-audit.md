# Funnel audit — before implementation

Repository inspection, September 9, 2026. No live HeyCatch dashboard or production dataset was inspected; these are confirmed code defects, not a claim that every reported discrepancy has a single verified dashboard cause.

## Providers and event ownership

- `frontend/instrumentation-client.ts` initializes HeyCatch 0.7. Its installed SDK automatically captures pageviews, history navigation and clicks. `lib/heycatch.ts` sends only signup completion from email registration; no canonical product page events reach it. Autocaptured visits therefore cannot populate funnels configured with absent named events.
- GA4 (`lib/googleAnalytics.ts`) receives global pageviews plus selected homepage, campaign and leaderboard events. Reddit Pixel handles optional campaign measurement; Vercel Speed Insights handles performance.
- `PageAnalyticsTracker` uses Next's pathname and sends pageviews to `/api/analytics/page-view`. Custom first-party calls go to `/api/analytics/event`; both persist in `PageViewEvent`. The backend has no canonical funnel contract. Events and pages are combined in the existing admin page report.
- Custom names vary (`homepage_view`, `leaderboards_view`, `top_stocks_view`, etc.). Outcomes, dynamic ticker pages, screener and strategy visits lack canonical event calls. Server-rendered pages do not acquire custom instrumentation automatically.

## Confirmed measurement gaps

1. HeyCatch init has neither environment nor consent checks, unlike GA and first-party tracking. A copied build with the project key can transmit from localhost, preview, staging or a file URL. This explains a plausible path for the suspicious local traffic; historical records must be inspected separately.
2. GA's tracker permanently excludes its initial pathname, so A → B → A omits the second A. Initial-page ownership is split between consent-manager config and the tracker. The path filter rejects *any* dot, including valid stock symbols such as BRK.B.
3. Backend pageview deduplication uses normalized routes and a 20-second window. AAPL → MSFT within that window collapses into one `/ticker/[symbol]` view. This is not React deduplication.
4. Anonymous custom events carry no session ID; backend sets `session_id_hash=None`. Authenticated pageviews also discard that hash, preventing first-party anonymous-to-account joins.
5. HeyCatch identifies only during login callbacks, includes unnecessary email/name, and is not restored on an existing authenticated session. SDK methods are no-ops before initialization.
6. Campaign helpers read only the current URL/referrer. Ordinary navigation and the marketing-to-app subdomain hop lose original attribution. Google OAuth has no authoritative new-account flag in its client flow. Checkout and paid completion are uninstrumented.
7. Existing gate components lack visible-impression events. Leaderboard clicks use separate names and omit stable entity identifiers. Some gates go to billing rather than public pricing.

## Product scope and implementation decisions

- Keep Free, Premium, Pro, prices, billing intervals and all entitlement checks. Existing leaderboard previews already show three rows; retain that exact boundary and protected metrics.
- Extend the existing provider adapters with a typed canonical facade, a single page-event owner and explicit success events. Keep HeyCatch autocaptured pageviews; do not add another HeyCatch pageview stream.
- Use consent-aware, production-only transmission, safe local debug payloads, path-only navigation context, stable internal IDs and bounded source labels. Preserve session/source across Walnut production subdomains using scoped cookies; never use emails/tokens as analytics identifiers.
- Make successful subscription measurement authoritative at the Stripe webhook. Never infer payment from a query string or a click.
- Improve the existing homepage CTA group, inline gate copy and pricing descriptions without redesigning unrelated pages.

## Verification boundary

Local tests can establish dispatch, route matching, deduplication, payload safety and unchanged gating/pricing. Live dashboard configuration, browser blockers, historical contamination, Stripe delivery and source reporting require deployment and subsequent production validation; no historic events will be fabricated or backfilled.

## Added scope: Outcomes and ticker discovery

The second user instruction was incorporated before those changes: investigate the actual Opened control, preserve all Outcomes calculations and certification, add only supported ticker discovery, and exclude email capture and lifecycle automation. No conflicting pricing or marketing work was introduced.

Existing ticker navigation already includes activity filters, research tabs, peers/comparison, a Similar Historical Setups card, and follow/watchlist controls. The implementation extends those surfaces rather than adding a generic navigation strip. Congress and insider source cards link to the same ticker's existing filtered activity section only when that source is present. Institutional discovery additionally requires the existing Pro entitlement. A returned historical top match links to that match ticker's Outcomes history. No strategy-to-stock membership is inferred.

## Outcomes investigation

Before the UI change, local existing QA fixtures demonstrated ascending and descending row order and keyboard activation. The original button measured roughly 54 by 16 pixels inside a substantially taller header cell. With the change, the button fills the header width with a minimum 44-pixel height and the header reports aria-sort; click and Space activation were verified at a 390 by 844 mobile viewport. The sorting function and entitlement branch were retained.

Chrome was subsequently used at the user's request with the existing production admin session. The live table initially showed August 5 and September 2 entries. An initial pointer attempt left the state unchanged; Enter then produced Opened ^ with August entries first, and a subsequent pointer click produced Opened v with September entries first. The live hit-test found the button itself at its center, pointer-events:auto, and a 16-pixel height. This does not establish a consistently broken handler or an intercepted overlay. The reproducible concern is the undersized click target, not a calculation defect. The six-click historical session itself was not replayed.
