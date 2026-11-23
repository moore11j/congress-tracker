# Walnut 2.0 shell preparation, November 23, 2026

Status: local implementation in `artifacts/release-walnut2-shell-nov22`, branch `codex/walnut2-discover-my-stocks-nov22`. No deployment, provider activation or default-route change. This is partial Phase 2 work under the [approved sequence](walnut-2-proposal-2026-11-14.md), while the institutional migration copy runs independently.

The existing draft adds `/discover` and `/my-stocks`. Discover reads the existing cached ranking projection, preserves supplied ranks and shared Confirmation labels, distinguishes failed/loading/empty results, and links to existing research tools. My Stocks requires the existing server and client session checks and reuses watchlists, monitoring and Research Memory. Watchlist loading failures remain distinct from an empty watchlist.

November 23 adds optional global navigation entries for Discover and My Stocks. `NEXT_PUBLIC_WALNUT_WORKSPACE_ENABLED=1` exposes those entries; absent or `0` preserves the existing navigation. Existing Feed, Insights, Profiles, Signals, Tools and Company destinations remain reachable. The middleware also applies the existing signed-out redirect to My Stocks, preserves its return query, and marks both new app routes noindex; marketing-host redirects preserve their path and query. Ask Walnut is not advertised before its grounded query capability exists. This additive navigation is an intermediate shell, not the final three-destination product.

Validation:

- Six actual rendered Discover cases pass: public rank/score protection, shared bearish label, missing projection flag, failure versus empty state, loading, and safe links/escaped company text.
- The navigation render matrix passes for both new routes, default-off behavior, retained destinations and exact off-state rollback. No browser requests or model calls occur in these render checks.
- TypeScript passes after the final navigation and middleware changes with `--noEmit --incremental false`; logs: `artifacts/walnut-shell-typecheck-nov23.log` and `artifacts/walnut-shell-typecheck-final-nov23.log`.
- Existing profile-navigation and protected-route suites have 28 passes and 12 failures. Reading the unchanged branch HEAD reproduces exactly the same 12 failure titles. Logs: `artifacts/walnut-shell-nav-current-tests-nov23.log` and `artifacts/walnut-shell-nav-baseline-tests-nov23.log`. This is not a full-suite pass; the failures concern existing chart/source expectations and protected-shell expectations.

The route/Discover run has 23 passes and one existing ticker-readiness test-harness failure (`fetch is not defined`); that same failure reproduces against unchanged HEAD. Both new route cases pass without a network implementation in the harness. Logs: `artifacts/walnut-shell-route-checks-nov23.log` and `artifacts/walnut-shell-route-baseline-nov23.log`. Source inspection traces Discover to the existing read-only, permission-projected cached Top Stocks endpoint. This is not browser network acceptance.

Remaining: full production build, mobile/desktop browser QA, full route/query/canonical compatibility, permission and passive-network checks, reviewed release, and the remaining approved Ask/material-change/monitoring capabilities. The feature flag has not been set in any hosting service. No price, plan, landing page, legacy route, recorded score or delivery schedule changed.
