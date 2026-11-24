# Walnut workspace layout revision

November 24, 2026. The owner rejected the preview's overall look and layout. This revision is a new visual candidate for review, not an accepted design or production release.

Discover now uses a compact title and search toolbar, ranked stocks separated by rules, and a secondary workspace column. The large gradient introduction, repeated rounded stock cards and full-width watchlist banner are removed. Research tools are grouped into market exploration, disclosed activity and testing an idea. All 19 existing tool destinations and their feature flags remain; the search form keeps its existing GET route. Small screens show stocks before the workspace and tools, with full-sized tool-link touch targets.

The stock list preserves server-supplied ranks, source timestamp, current-plan score projection, shared score labels, retry/loading/empty states and validated company links. The Premium/Pro explanation appears once below the list instead of once per stock. There are no invented prices, charts, changes or briefing results. Shared workspace tabs use a simple underline; production top navigation, the public landing page and Feed layout are unchanged.

Existing focused rendering, authentication and lifecycle checks pass (24 cases). TypeScript and deployed-preview desktop/mobile visual checks are recorded in the root acceptance receipt when complete. No new behavioral tests are added for this reversible presentation change. Preview Google authentication remains unavailable, as documented in the login-origin report. The active institutional replay remains untouched.
