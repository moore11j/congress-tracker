// Local responsive QA: point NEXT_PUBLIC_API_BASE_URL at this server.
// Synthetic records only. No production services or credentials are used.
import http from "node:http";

const items = [
  ["congress-quality", "Congress Quality", "congress"],
  ["insider-trend", "Insider + SMA50/SMA200 Trend", "insider"],
  ["cross-source", "Cross-source Confirmation", "cross_source"],
].map(([slug, name, category], index) => ({
  id: index + 1, slug, name, category, status: "published", accessTier: "premium",
  isFeatured: index === 0, sortOrder: index, shortDescription: "Synthetic stored strategy for responsive QA.",
  rule: { description: "long_unbroken_strategy_input_".repeat(5) },
  performance: { totalReturnPct: 12 - index, cagrPct: 8 - index, benchmarkReturnPct: 5, alphaCagrPct: 3 - index, sharpe: 1.2, maxDrawdownPct: -8 },
  latestRun: { benchmark: "SPY", backtestStartDate: "2025-01-01", runType: "backtest" },
  equityCurve: [],
}));
const categoryCounts = Object.fromEntries(items.map(item => [item.category, 1]));
const server = http.createServer((request, response) => {
  const url = new URL(request.url, "http://127.0.0.1");
  let payload;
  if (request.method !== "GET") {
    response.writeHead(405).end(); return;
  }
  if (url.pathname === "/api/strategies") {
    const category = url.searchParams.get("category");
    const filtered = items.filter(item => !category || category === item.category);
    payload = { items: filtered, metadata: { count: filtered.length, categoryCounts, period: "max", sort: "cagr", category, storage: "qa_fixture" } };
  } else if (url.pathname.startsWith("/api/strategies/")) {
    payload = items.find(item => item.slug === url.pathname.split("/").at(-1));
  } else if (url.pathname === "/api/auth/me") payload = { user: null };
  else if (url.pathname === "/api/entitlements") payload = { tier: "free", effective_tier: "free", limits: {}, features: [], upgrade_url: "/pricing" };
  const send = () => {
    response.writeHead(payload ? 200 : 404, { "content-type": "application/json", "access-control-allow-origin": "*" });
    response.end(JSON.stringify(payload ?? { detail: "Not in local QA fixture" }));
  };
  if (url.pathname.startsWith("/api/strategies")) setTimeout(send, 350);
  else send();
});
server.listen(Number(process.env.STRATEGIES_VISUAL_MOCK_PORT || 8086), "127.0.0.1", () => console.log("Strategies QA fixture ready"));
