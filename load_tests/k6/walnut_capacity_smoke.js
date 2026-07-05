import http from "k6/http";
import { check, fail, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.1/index.js";

const DEFAULT_BASE_URL = "http://localhost:3000";
const DEFAULT_API_BASE_URL = __ENV.API_BASE_URL || __ENV.BASE_URL || DEFAULT_BASE_URL;
const BASE_URL = normalizeBaseUrl(__ENV.BASE_URL || DEFAULT_BASE_URL);
const API_BASE_URL = normalizeBaseUrl(DEFAULT_API_BASE_URL);
const ALLOW_PRODUCTION = (__ENV.ALLOW_PRODUCTION_LOAD_TEST || "").toLowerCase() === "true";
const USER_STATE = __ENV.AUTH_TOKEN || __ENV.SESSION_COOKIE ? "authenticated" : "public";
const SMOKE_VUS = clampInt(__ENV.SMOKE_VUS, 1, 10, 3);
const SMOKE_DURATION = __ENV.SMOKE_DURATION || "90s";

guardProductionTarget(BASE_URL);
guardProductionTarget(API_BASE_URL);

export const options = {
  scenarios: {
    smoke: {
      executor: "constant-vus",
      vus: SMOKE_VUS,
      duration: SMOKE_DURATION,
      exec: "smoke",
    },
    bot_prefetch_guard: {
      executor: "constant-vus",
      vus: 1,
      duration: __ENV.BOT_GUARD_DURATION || "30s",
      exec: "botPrefetchGuard",
      startTime: "0s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    "checks{route_priority:core}": ["rate>0.99"],
    "http_req_duration{route_priority:core}": ["p(95)<1000"],
    http_req_duration: ["p(95)<1500"],
    five_xx_rate: ["rate<0.001"],
  },
};

const routeDuration = new Trend("walnut_route_duration", true);
const routeRequests = new Counter("walnut_route_requests");
const routeFailures = new Counter("walnut_route_failures");
const routeFiveXx = new Counter("walnut_route_5xx");
const statusCodes = new Counter("walnut_status_codes");
const fiveXxRate = new Rate("five_xx_rate");

const tickers = ["AAPL", "NVDA", "INTC", "PLTR", "LMT", "MSFT", "TSLA"];

const diagnosticRouteFamilies = [
  "health",
  "feed",
  "ticker",
  "market",
  "signals",
  "screener",
  "institution",
  "auth",
  "watchlists",
  "monitoring",
  "insider",
  "member",
];

const diagnosticEndpoints = [
  "health",
  "events_feed",
  "events_feed_10_enriched",
  "events_feed_50_enriched",
  "feed_page",
  "feed_mode_all_page",
  "home_page",
  "ticker_page",
  "ticker_aapl_page",
  "ticker_tsm_page",
  "ticker_context_bundle",
  "ticker_aapl_context_bundle",
  "ticker_tsm_context_bundle",
  "ticker_signals_summary",
  "ticker_government_contracts",
  "market_quotes",
  "signals_page",
  "screener_page",
  "watchlists_page",
  "monitoring_page",
  "institution_profile",
  "login_page",
  "pricing_page",
  "plan_config",
  "insider_profile",
  "member_profile",
  "ticker_bot_page",
  "ticker_context_prefetch",
  "government_contracts_prefetch",
];

export function diagnosticThresholds() {
  const thresholds = {};
  for (const family of diagnosticRouteFamilies) {
    thresholds[`walnut_route_duration{route_family:${family}}`] = ["p(95)<60000"];
    thresholds[`walnut_route_requests{route_family:${family}}`] = ["count>=0"];
    thresholds[`walnut_route_failures{route_family:${family}}`] = ["count>=0"];
    thresholds[`walnut_route_5xx{route_family:${family}}`] = ["count>=0"];
  }
  for (const endpoint of diagnosticEndpoints) {
    thresholds[`walnut_route_duration{endpoint_name:${endpoint}}`] = ["p(95)<60000"];
    thresholds[`walnut_route_requests{endpoint_name:${endpoint}}`] = ["count>=0"];
    thresholds[`walnut_route_failures{endpoint_name:${endpoint}}`] = ["count>=0"];
    thresholds[`walnut_route_5xx{endpoint_name:${endpoint}}`] = ["count>=0"];
  }
  return thresholds;
}

export function smoke() {
  runWeighted([
    [40, feedEventsFlow],
    [25, tickerFlow],
    [10, signalsFlow],
    [10, workflowFlow],
    [5, institutionFlow],
    [5, authAccountFlow],
    [5, insiderMemberFlow],
  ]);
  think();
}

export function botPrefetchGuard() {
  const headers = {
    "User-Agent": "WalnutCapacityBot/1.0",
    Purpose: "prefetch",
    "X-Middleware-Prefetch": "1",
    "Next-Router-Prefetch": "1",
  };
  requestPage("/ticker/AAPL", "ticker_bot_page", "ticker", "core", { headers }, [200, 204]);
  requestApi("/api/tickers/AAPL/context-bundle", "ticker_context_prefetch", "ticker", "core", { headers }, [200, 204]);
  requestApi("/api/tickers/NVDA/government-contracts", "government_contracts_prefetch", "ticker", "core", { headers }, [200, 204]);
  sleep(randomBetween(2, 5));
}

export function backendApiDiagnostic() {
  runWeighted([
    [6, () => requestApi("/health", "health", "health", "core")],
    [18, () => requestApi("/api/events?limit=50&enrich_prices=1", "events_feed_50_enriched", "feed", "core")],
    [14, () => requestApi("/api/events?limit=10&enrich_prices=1", "events_feed_10_enriched", "feed", "core")],
    [10, () => requestApi("/api/market/quotes?symbols=NVDA,AAPL,LMT,PLTR", "market_quotes", "market", "core")],
    [12, () => requestApi("/api/tickers/AAPL/context-bundle", "ticker_aapl_context_bundle", "ticker", "core")],
    [12, () => requestApi("/api/tickers/TSM/context-bundle", "ticker_tsm_context_bundle", "ticker", "core")],
    [8, () => requestApi("/api/tickers/NVDA/government-contracts", "ticker_government_contracts", "ticker", "core")],
    [10, () => requestApi("/api/tickers/AAPL/signals-summary", "ticker_signals_summary", "ticker", "core")],
    [6, () => requestApi("/api/institutions/0001067983", "institution_profile", "institution", "core")],
    [4, () => requestApi("/api/plan-config", "plan_config", "auth", "secondary")],
  ]);
  think();
}

export function appHostApiDiagnostic() {
  backendApiDiagnostic();
}

export function appHostPagesDiagnostic() {
  runWeighted([
    [10, () => requestPage("/", "home_page", "feed", "core")],
    [16, () => requestPage("/?mode=all", "feed_mode_all_page", "feed", "core")],
    [10, () => requestPage("/pricing", "pricing_page", "auth", "secondary")],
    [12, () => requestPage("/signals", "signals_page", "signals", "core")],
    [12, () => requestPage("/screener", "screener_page", "screener", "core")],
    [8, () => requestPage("/watchlists", "watchlists_page", "watchlists", "secondary", {}, [200, 302, 307])],
    [8, () => requestPage("/monitoring", "monitoring_page", "monitoring", "secondary", {}, [200, 302, 307])],
    [10, () => requestPage("/ticker/AAPL", "ticker_aapl_page", "ticker", "core")],
    [10, () => requestPage("/ticker/TSM", "ticker_tsm_page", "ticker", "core")],
    [4, () => requestPage("/institution/0001067983", "institution_profile", "institution", "core")],
  ]);
  think();
}

function feedEventsFlow() {
  if (Math.random() < 0.65) {
    requestApi("/api/events?limit=25&enrich_prices=0", "events_feed", "feed", "core");
    return;
  }
  requestPage("/feed", "feed_page", "feed", "core");
}

function tickerFlow() {
  const symbol = randomTicker();
  const roll = Math.random();
  if (roll < 0.30) requestPage(`/ticker/${symbol}`, "ticker_page", "ticker", "core");
  else if (roll < 0.58) requestApi(`/api/tickers/${symbol}/context-bundle`, "ticker_context_bundle", "ticker", "core");
  else if (roll < 0.76) requestApi(`/api/tickers/${symbol}/signals-summary`, "ticker_signals_summary", "ticker", "core");
  else if (roll < 0.90) requestApi(`/api/tickers/${symbol}/government-contracts`, "ticker_government_contracts", "ticker", "core");
  else requestApi("/api/market/quotes?symbols=NVDA,AAPL,LMT,PLTR", "market_quotes", "market", "core");
}

function signalsFlow() {
  requestPage("/signals", "signals_page", "signals", "core");
}

function workflowFlow() {
  const roll = Math.random();
  if (roll < 0.45) requestPage("/screener", "screener_page", "screener", "core");
  else if (roll < 0.72) requestPage("/watchlists", "watchlists_page", "watchlists", "secondary", {}, [200, 302, 307]);
  else requestPage("/monitoring", "monitoring_page", "monitoring", "secondary", {}, [200, 302, 307]);
}

function institutionFlow() {
  requestPage("/institution/0001067983", "institution_profile", "institution", "core");
}

function authAccountFlow() {
  const roll = Math.random();
  if (roll < 0.40) requestPage("/login", "login_page", "auth", "secondary");
  else if (roll < 0.75) requestPage("/pricing", "pricing_page", "auth", "secondary");
  else requestApi("/api/plan-config", "plan_config", "auth", "secondary");
}

function insiderMemberFlow() {
  if (Math.random() < 0.5) {
    requestPage("/insider/tim-cook-0001214156?lookback=1095", "insider_profile", "insider", "secondary");
  } else {
    requestPage("/member/NANCY_PELOSI", "member_profile", "member", "secondary");
  }
}

function requestPage(path, endpointName, routeFamily, routePriority, init = {}, expectedStatuses = [200]) {
  return request(BASE_URL, path, endpointName, routeFamily, routePriority, init, expectedStatuses);
}

function requestApi(path, endpointName, routeFamily, routePriority, init = {}, expectedStatuses = [200]) {
  return request(API_BASE_URL, path, endpointName, routeFamily, routePriority, init, expectedStatuses);
}

function request(baseUrl, path, endpointName, routeFamily, routePriority, init = {}, expectedStatuses = [200]) {
  const tags = {
    endpoint_name: endpointName,
    route_family: routeFamily,
    route_priority: routePriority,
    user_state: USER_STATE,
  };
  const headers = {
    ...authHeaders(),
    "X-Walnut-Load-Test": "capacity-smoke",
    "X-Walnut-Request-Source": "load_test",
    "X-Walnut-Route-Family": routeFamily,
    "X-Walnut-Panel": endpointName,
    ...(init.headers || {}),
  };
  const response = http.get(`${baseUrl}${path}`, {
    ...init,
    headers,
    tags,
    redirects: 2,
  });
  routeDuration.add(response.timings.duration, tags);
  routeRequests.add(1, tags);
  if (!expectedStatuses.includes(response.status)) {
    routeFailures.add(1, tags);
  }
  if (response.status >= 500) {
    routeFiveXx.add(1, tags);
  }
  statusCodes.add(1, { ...tags, status: String(response.status) });
  fiveXxRate.add(response.status >= 500, tags);
  check(response, {
    [`${endpointName} expected status`]: (res) => expectedStatuses.includes(res.status),
    [`${endpointName} no 5xx`]: (res) => res.status < 500,
  }, tags);
  return response;
}

export function handleSummary(data) {
  const routeRows = collectRouteRows(data);
  const topByP95 = [...routeRows]
    .filter((row) => row.kind === "family")
    .sort((a, b) => (b.p95 || 0) - (a.p95 || 0))
    .slice(0, 10);
  const endpointRows = routeRows
    .filter((row) => row.kind === "endpoint")
    .sort((a, b) => (b.p95 || 0) - (a.p95 || 0));

  return {
    stdout: [
      textSummary(data, { indent: " ", enableColors: true }),
      "",
      "Walnut route-family latency attribution",
      formatRouteTable(topByP95),
      "",
      "Walnut endpoint latency attribution",
      formatRouteTable(endpointRows),
      "",
    ].join("\n"),
  };
}

function collectRouteRows(data) {
  const rows = new Map();

  for (const [metricName, metric] of Object.entries(data.metrics || {})) {
    const parsed = parseTaggedMetric(metricName);
    if (!parsed) continue;

    const routeKey = parsed.tags.endpoint_name || parsed.tags.route_family;
    if (!routeKey) continue;

    const kind = parsed.tags.endpoint_name ? "endpoint" : "family";
    const key = `${kind}:${routeKey}`;
    const existing = rows.get(key) || {
      kind,
      route: routeKey,
      count: 0,
      failed: 0,
      five_xx: 0,
      p50: null,
      p90: null,
      p95: null,
      p99: null,
      max: null,
    };

    const values = metric.values || {};
    if (parsed.baseName === "walnut_route_duration") {
      existing.p50 = numericValue(values["p(50)"] ?? values.med);
      existing.p90 = numericValue(values["p(90)"]);
      existing.p95 = numericValue(values["p(95)"]);
      existing.p99 = numericValue(values["p(99)"]);
      existing.max = numericValue(values.max);
      existing.count = numericValue(values.count) ?? existing.count;
    } else if (parsed.baseName === "walnut_route_requests") {
      existing.count = numericValue(values.count) ?? existing.count;
    } else if (parsed.baseName === "walnut_route_failures") {
      existing.failed = numericValue(values.count) ?? existing.failed;
    } else if (parsed.baseName === "walnut_route_5xx") {
      existing.five_xx = numericValue(values.count) ?? existing.five_xx;
    }

    rows.set(key, existing);
  }

  return [...rows.values()].sort((a, b) => {
    if (a.kind !== b.kind) return a.kind.localeCompare(b.kind);
    return a.route.localeCompare(b.route);
  });
}

function parseTaggedMetric(metricName) {
  const match = metricName.match(/^([^{}]+)\{(.+)\}$/);
  if (!match) return null;
  const baseName = match[1];
  if (!["walnut_route_duration", "walnut_route_requests", "walnut_route_failures", "walnut_route_5xx"].includes(baseName)) {
    return null;
  }
  const tags = {};
  for (const part of match[2].split(",")) {
    const [key, ...rest] = part.split(":");
    if (!key || rest.length === 0) continue;
    tags[key.trim()] = rest.join(":").trim();
  }
  return { baseName, tags };
}

function numericValue(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatRouteTable(rows) {
  if (!rows.length) return "  no route attribution metrics found";
  const header = ["route", "requests", "failed", "5xx", "p50", "p90", "p95", "p99", "max"];
  const tableRows = rows.map((row) => [
    row.route,
    formatCount(row.count),
    formatCount(row.failed),
    formatCount(row.five_xx),
    formatMs(row.p50),
    formatMs(row.p90),
    formatMs(row.p95),
    formatMs(row.p99),
    formatMs(row.max),
  ]);
  return renderTextTable(header, tableRows);
}

function renderTextTable(header, rows) {
  const widths = header.map((cell, index) =>
    Math.max(cell.length, ...rows.map((row) => String(row[index] || "").length))
  );
  const formatRow = (row) =>
    row.map((cell, index) => String(cell || "").padEnd(widths[index])).join("  ");
  return [formatRow(header), formatRow(widths.map((width) => "-".repeat(width))), ...rows.map(formatRow)]
    .map((line) => `  ${line}`)
    .join("\n");
}

function formatMs(value) {
  return value === null || value === undefined ? "-" : `${Math.round(value)}ms`;
}

function formatCount(value) {
  return value === null || value === undefined ? "-" : String(Math.round(value));
}

function authHeaders() {
  const headers = {};
  if (__ENV.AUTH_TOKEN) headers.Authorization = `Bearer ${__ENV.AUTH_TOKEN}`;
  if (__ENV.SESSION_COOKIE) headers.Cookie = __ENV.SESSION_COOKIE;
  return headers;
}

function runWeighted(entries) {
  const total = entries.reduce((sum, [weight]) => sum + weight, 0);
  let cursor = Math.random() * total;
  for (const [weight, fn] of entries) {
    cursor -= weight;
    if (cursor <= 0) return fn();
  }
  return entries[entries.length - 1][1]();
}

function randomTicker() {
  return tickers[Math.floor(Math.random() * tickers.length)];
}

function think() {
  sleep(randomBetween(1, 5));
}

function randomBetween(min, max) {
  return min + Math.random() * (max - min);
}

function normalizeBaseUrl(value) {
  return String(value || DEFAULT_BASE_URL).replace(/\/+$/, "");
}

function clampInt(value, min, max, fallback) {
  const parsed = Number.parseInt(value || "", 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function guardProductionTarget(url) {
  const lower = url.toLowerCase();
  const isProduction =
    lower.includes("app.walnutmarkets.com") ||
    lower.includes("walnutmarkets.com") ||
    lower.includes("congress-tracker-api.fly.dev");
  if (isProduction && !ALLOW_PRODUCTION) {
    fail(`Refusing production load test for ${url}. Set ALLOW_PRODUCTION_LOAD_TEST=true only after explicit approval.`);
  }
}
