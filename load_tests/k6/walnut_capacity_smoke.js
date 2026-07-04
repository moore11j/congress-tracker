import http from "k6/http";
import { check, fail, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const DEFAULT_BASE_URL = "http://localhost:3000";
const DEFAULT_API_BASE_URL = __ENV.API_BASE_URL || __ENV.BASE_URL || DEFAULT_BASE_URL;
const BASE_URL = normalizeBaseUrl(__ENV.BASE_URL || DEFAULT_BASE_URL);
const API_BASE_URL = normalizeBaseUrl(DEFAULT_API_BASE_URL);
const ALLOW_PRODUCTION = (__ENV.ALLOW_PRODUCTION_LOAD_TEST || "").toLowerCase() === "true";
const USER_STATE = __ENV.AUTH_TOKEN || __ENV.SESSION_COOKIE ? "authenticated" : "public";
const SMOKE_VUS = clampInt(__ENV.SMOKE_VUS, 1, 5, 3);
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
const statusCodes = new Counter("walnut_status_codes");
const fiveXxRate = new Rate("five_xx_rate");

const tickers = ["AAPL", "NVDA", "INTC", "PLTR", "LMT", "MSFT", "TSLA"];

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
  statusCodes.add(1, { ...tags, status: String(response.status) });
  fiveXxRate.add(response.status >= 500, tags);
  check(response, {
    [`${endpointName} expected status`]: (res) => expectedStatuses.includes(res.status),
    [`${endpointName} no 5xx`]: (res) => res.status < 500,
  }, tags);
  return response;
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
