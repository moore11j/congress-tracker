import http from "node:http";

const horizons = [7, 30, 90, 180, 365];
const symbols = ["NVDA", "AAPL", "PLTR", "AMZN", "META", "MSFT"];
const returns = [18.4, -7.25, 12.8, 4.5, -3.4, 9.1];
const entries = [181.42, 224.18, 132.06, 196.31, 611.52, 497.88];

function outcome(raw, days) {
  const spy = Number((days * 0.08).toFixed(6));
  const scaled = Number((raw * (days / 30)).toFixed(8));
  return {
    status: "matured",
    horizon_days: days,
    target_date: `2026-${days === 7 ? "07-13" : days === 30 ? "08-05" : "09-01"}`,
    price: 200,
    price_date: "2026-08-05",
    price_type: "official_close",
    return_pct: scaled,
    directional_return_pct: scaled,
    raw_directionally_correct: scaled > 0,
    spy_return_pct: spy,
    excess_return_pct: Number((scaled - spy).toFixed(8)),
    directional_excess_return_pct: Number((scaled - spy).toFixed(8)),
    benchmark_directionally_correct: scaled > spy,
    directionally_correct: scaled > 0 || scaled > spy,
    grading_basis: "raw_or_vs_spy",
    audit_version: "outcomes-integrity-v1",
  };
}

const items = symbols.map((ticker, index) => ({
  id: 101 + index,
  ticker,
  calculated_at: `2026-07-${String(6 + index).padStart(2, "0")}T12:45:00Z`,
  market_date: `2026-07-${String(6 + index).padStart(2, "0")}`,
  score: 82 - index * 4,
  direction: "bullish",
  strength: index < 2 ? "exceptional" : "strong",
  reference_price: entries[index],
  reference_price_at: `2026-07-${String(6 + index).padStart(2, "0")}T13:30:00Z`,
  reference_price_source: "authoritative-test-feed",
  entry_price_type: "official_open",
  data_integrity_status: "verified",
  active_source_count: 3,
  active_sources: ["congress", "insiders", "price_volume"],
  methodology: "outcomes-v3-next-executable-open-calendar-horizons",
  outcomes: Object.fromEntries(horizons.map((days) => [`${days}D`, outcome(returns[index], days)])),
  lifecycle_status: "open",
  calculation_type: "live",
}));

function summary(horizon = "30D") {
  return {
    horizon,
    completed_events: 42,
    directional_sample_count: 42,
    accuracy: 64,
    average_directional_return: 5.7,
    average_spy_return: 2.4,
    average_directional_excess_return: 3.3,
    benchmarked_events: 42,
    matured_horizon_count: 156,
    score_bands: [
      { band: "0-39", accuracy: null, count: 0 },
      { band: "40-59", accuracy: 51, count: 8 },
      { band: "60-64", accuracy: 56, count: 9 },
      { band: "65-69", accuracy: 61, count: 9 },
      { band: "70-74", accuracy: 67, count: 6 },
      { band: "75-79", accuracy: 72, count: 5 },
      { band: "80+", accuracy: 80, count: 5 },
    ],
  };
}

const status = {
  enabled: true,
  tracking_status: "live",
  current_methodology_version: "confirmation-v2",
  first_live_snapshot_date: "2026-05-01T12:00:00Z",
  most_recent_snapshot_timestamp: "2026-09-03T16:00:00Z",
  unique_securities_captured: 42,
  total_live_snapshots: 42,
  data_quality_status: "ok",
  verified_outcome_entries: 42,
  outcomes_on_audit_hold: 0,
};

const pathPoints = Array.from({ length: 23 }, (_, index) => {
  const stock = Number((index * 0.84 + Math.sin(index / 2) * 1.8).toFixed(8));
  const spy = Number((index * 0.18 + Math.sin(index / 3) * 0.35).toFixed(8));
  return {
    date: new Date(Date.UTC(2026, 6, 6 + index, 20)).toISOString(),
    session_date: new Date(Date.UTC(2026, 6, 6 + index)).toISOString().slice(0, 10),
    price_type: index === 0 ? "official_open" : "official_close",
    security_return_pct: index === 0 ? 0 : stock,
    benchmark_return_pct: index === 0 ? 0 : spy,
    excess_return_pct: index === 0 ? 0 : Number((stock - spy).toFixed(8)),
  };
});

const server = http.createServer((request, response) => {
  const url = new URL(request.url ?? "/", "http://127.0.0.1:8083");
  let payload;
  if (url.pathname === "/api/auth/me") {
    payload = { user: null };
  } else if (url.pathname === "/api/entitlements") {
    payload = { tier: "premium", effective_tier: "premium", limits: {}, features: [], upgrade_url: "/pricing" };
  } else if (url.pathname === "/api/outcomes/overview") {
    payload = { status, summaries: { "7D": summary("7D"), "30D": summary("30D") }, snapshots: { items, page: 0, limit: 100, total: items.length, has_next: false }, default_horizon: "30D" };
  } else if (url.pathname === "/api/outcomes/summary") {
    payload = summary(url.searchParams.get("horizon") ?? "30D");
  } else if (url.pathname === "/api/outcomes/snapshots") {
    payload = { items, page: 0, limit: 100, total: items.length, has_next: false };
  } else if (/^\/api\/outcomes\/snapshots\/\d+\/price-path$/.test(url.pathname)) {
    payload = { snapshot_id: 101, symbol: "NVDA", benchmark_symbol: "SPY", horizon_days: 30, methodology: "outcomes-v3-next-executable-open-calendar-horizons", points: pathPoints };
  } else {
    response.writeHead(404, { "content-type": "application/json" });
    response.end(JSON.stringify({ detail: "not found" }));
    return;
  }
  response.writeHead(200, { "content-type": "application/json", "access-control-allow-origin": "*" });
  response.end(JSON.stringify(payload));
});

server.listen(8083, "127.0.0.1", () => console.log("Outcomes visual fixture listening on 8083"));
