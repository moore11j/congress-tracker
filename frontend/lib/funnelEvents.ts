export const funnelEvents = [
  "ticker_related_content_viewed", "ticker_related_content_clicked",
  "homepage_viewed", "screener_opened", "screener_result_clicked", "leaderboard_viewed", "leaderboard_entity_clicked",
  "strategy_list_viewed", "strategy_viewed", "ticker_viewed", "congress_trades_viewed", "insider_activity_viewed",
  "institutional_activity_viewed", "outcomes_viewed", "confirmation_score_viewed", "upgrade_prompt_viewed",
  "upgrade_prompt_clicked", "pricing_viewed", "signup_started", "signup_completed", "signin_started", "signin_completed",
  "checkout_started", "subscription_completed", "watchlist_created", "ticker_added_to_watchlist", "strategy_followed", "alert_created",
] as const;
export type FunnelEvent = typeof funnelEvents[number];

export function routeFunnelEvent(path: string, query = "", marketingHome = false): { name: FunnelEvent; properties: Record<string, string> } | null {
  path = path.replace(/\/$/, "") || "/";
  const params = new URLSearchParams(query);
  if (path === "/landing" || (path === "/" && marketingHome)) return { name: "homepage_viewed", properties: {} };
  const routes: Record<string, FunnelEvent> = { "/screener": "screener_opened", "/walnut-public/screener": "screener_opened", "/strategies": "strategy_list_viewed", "/outcomes": "outcomes_viewed", "/pricing": "pricing_viewed" };
  if (routes[path]) return { name: routes[path], properties: {} };
  if (/^\/leaderboards(?:\/congress-traders)?$/.test(path) || path === "/top-stocks") return { name: "leaderboard_viewed", properties: { leaderboard_type: path.endsWith("congress-traders") ? "congress" : path === "/top-stocks" ? "stocks" : "all" } };
  const ticker = path.match(/^\/(?:walnut-public\/)?ticker\/([A-Za-z0-9.^-]+)$/);
  if (ticker) return { name: "ticker_viewed", properties: { ticker: ticker[1].toUpperCase() } };
  const strategy = path.match(/^\/strategies\/([^/]+)$/);
  if (strategy && strategy[1] !== "methodology") return { name: "strategy_viewed", properties: { strategy_id: strategy[1] } };
  if (path === "/feed" || path === "/") {
    const modes: Record<string, FunnelEvent> = { congress: "congress_trades_viewed", insider: "insider_activity_viewed", institutional: "institutional_activity_viewed" };
    const mode = params.get("mode") || "all";
    if (modes[mode]) return { name: modes[mode], properties: { entity_type: mode } };
  }
  return null;
}

/** Rerenders/consent refreshes don't create visits; A → B → A does. */
export function createVisitTracker() {
  let lastKey: string | null = null;
  return { enter(key: string) { if (key === lastKey) return false; lastKey = key; return true; } };
}
