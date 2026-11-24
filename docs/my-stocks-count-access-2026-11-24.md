# My Stocks unread count access

November 24, 2026. Local implementation and validation; not deployed.

The watchlist list endpoint counted all saved unread alerts belonging to the account and watchlist, even when the current plan could not view their event type. A synthetic signed-session reproduction with one ordinary alert, one signal and one institutional alert returned three for Free and Premium; existing policy permits one and two respectively. Pro and admin correctly returned three. This can affect retained alerts after a downgrade. The reproduction establishes incorrect badge counts, not disclosure of alert content.

The endpoint now uses the existing `watchlist_unread_counts` service. That preserves grouped reads, current entitlement filtering and checkpoint fallback for older watchlists. All three existing count fields retain the same response contract. No plan definitions, alert histories, delivery settings or provider configuration change.

Nine new endpoint cases cover Free, Premium, Pro, admin, ordinary-plus-restricted and restricted-only saved alerts, legacy checkpoint fallback, empty watchlists, foreign ownership, read/dismissed alerts and missing authentication. They invoke the real handler with signed synthetic sessions and prohibit network connections and SQL writes during the read. Sixteen tests pass locally including the existing unseen and recent-activity suites. The older unseen fixture now includes the existing feature/plan tables required by its mark-read path; it previously failed with a missing `plan_limits` table.

The workspace workflow runs these checks on Linux/Python 3.12 alongside its existing frontend checks. Exact new-head CI remains pending until pushed. Populated authenticated browser acceptance and broader migration acceptance remain separate gates. The detached institutional replay uses a different immutable checkout and was not modified by this fix.
