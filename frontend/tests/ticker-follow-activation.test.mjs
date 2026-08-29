import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");
const followButton = read("components/ticker/TickerFollowButton.tsx");
const tickerPage = read("app/ticker/[symbol]/page.tsx");
const api = read("lib/api.ts");
const analytics = read("lib/googleAnalytics.ts");

test("ticker header uses the follow activation control without changing admin research actions", () => {
  assert.match(tickerPage, /<ResearchActions[\s\S]*canCreateResearch=\{canCreateResearch\}/);
  assert.match(tickerPage, /<TickerFollowButton key=\{normalizedSymbol\} symbol=\{normalizedSymbol\} \/>/);
  assert.doesNotMatch(tickerPage, /<AddTickerToWatchlist symbol=\{normalizedSymbol\} \/>/);
  assert.match(followButton, /★ Follow \$\{normalizedSymbol\}/);
  assert.match(followButton, /✓ Following \$\{normalizedSymbol\}/);
});

test("anonymous follow explains monitoring and preserves a durable post-auth ticker intent", () => {
  assert.match(followButton, /Follow \$\{normalizedSymbol\} with Walnut/);
  assert.match(followButton, /We’ll monitor \$\{normalizedSymbol\} and alert you when something important changes\./);
  assert.match(followButton, /Confirmation Score changes/);
  assert.match(followButton, /Insider activity/);
  assert.match(followButton, /Congress trades/);
  assert.match(followButton, /Institutional activity/);
  assert.match(followButton, /Major fundamental changes/);
  assert.match(followButton, /searchParams\.get\("follow"\) === "1"/);
  assert.match(followButton, /params\.delete\("follow"\)/);
  assert.match(followButton, /login\?mode=register&return_to=/);
  assert.match(followButton, /Create free account to follow/);
  assert.match(followButton, /Already have an account\? Log in/);
});

test("ticker follow reuses watchlist APIs and emits the complete GA4 funnel", () => {
  assert.match(api, /buildApiUrl\("\/api\/watchlists\/follow"\)/);
  assert.match(followButton, /followTicker\(normalizedSymbol\)/);
  assert.match(followButton, /router\.push\(`\/watchlists\/\$\{followingWatchlist\.id\}`\)/);
  for (const eventName of [
    "ticker_follow_impression",
    "ticker_follow_click",
    "ticker_follow_auth_prompt_view",
    "ticker_follow_signup_click",
    "ticker_follow_login_click",
    "ticker_follow_complete",
  ]) {
    assert.match(followButton, new RegExp(eventName));
  }
  assert.match(followButton, /source_page_type: "ticker"/);
  assert.match(followButton, /auth_state: source\.user \? "authenticated" : "anonymous"/);
  assert.match(followButton, /acquisition_source:/);
  assert.match(followButton, /follow_method: eventName === "ticker_follow_complete" \? "ticker_header"/);
  assert.match(analytics, /export function recordGoogleAnalyticsEvent/);
});

test("follow prompt uses a mobile bottom sheet while preserving keyboard-safe modal behavior", () => {
  assert.match(followButton, /className="items-end p-0 sm:items-center/);
  assert.match(followButton, /rounded-b-none/);
  assert.match(followButton, /aria-label=\{following \? `Following/);
  assert.match(followButton, /aria-live="polite"/);
});
