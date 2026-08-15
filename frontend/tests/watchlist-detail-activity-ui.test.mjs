import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const confirmationPanelPath = path.join(process.cwd(), "components", "watchlists", "ConfirmationMonitoringRefreshButton.tsx");
const watchlistPagePath = path.join(process.cwd(), "app", "watchlists", "[id]", "page.tsx");
const watchlistClientPath = path.join(process.cwd(), "components", "watchlists", "WatchlistDetailClient.tsx");
const recentActivityPath = path.join(process.cwd(), "components", "watchlists", "WatchlistRecentActivity.tsx");
const tickerManagerPath = path.join(process.cwd(), "components", "watchlists", "WatchlistTickerManager.tsx");
const feedCardPath = path.join(process.cwd(), "components", "feed", "FeedCard.tsx");

const confirmationPanelSource = fs.readFileSync(confirmationPanelPath, "utf8");
const watchlistPageSource = fs.readFileSync(watchlistPagePath, "utf8");
const watchlistClientSource = fs.readFileSync(watchlistClientPath, "utf8");
const recentActivitySource = fs.readFileSync(recentActivityPath, "utf8");
const tickerManagerSource = fs.readFileSync(tickerManagerPath, "utf8");
const feedCardSource = fs.readFileSync(feedCardPath, "utf8");

test("confirmation monitor clears through the shared confirmation dialog", () => {
  assert.match(confirmationPanelSource, /<WalnutConfirmDialog/);
  assert.match(confirmationPanelSource, /title=\{confirmEvent \? `Clear this \$\{confirmEvent\.ticker\} change\?` : "Clear all confirmation changes\?"\}/);
  assert.match(confirmationPanelSource, /onClose=\{\(\) => setConfirmTarget\(null\)\}/);
});

test("watchlist recent activity keeps the selected window while showing new items", () => {
  assert.match(watchlistPageSource, /Activity is hydrated in the client/);
  assert.match(recentActivitySource, /recent_days: Number\(nextState\.recentDays\)/);
  assert.match(recentActivitySource, /unread_only: nextState\.onlyNew \? 1 : undefined/);
  assert.match(recentActivitySource, /void fetchActivity\(initialState\)/);
  assert.match(watchlistClientSource, /recent_days: Number\(hydratedState\.recentDays\)/);
  assert.match(watchlistClientSource, /unread_only: hydratedState\.onlyNew \? 1 : undefined/);
  assert.match(recentActivitySource, /Showing new activity since/);
  assert.match(recentActivitySource, /Switch to All to see every item inside the selected/);
});

test("watchlist trade cards use disclosure-safe labels plus filed-after placement", () => {
  assert.match(feedCardSource, /\{isInstitutional \? "Filing date" : "Trade"\}/);
  assert.match(feedCardSource, /\{isInstitutional \? "Report period" : "Report"\}/);
  assert.match(feedCardSource, /isWatchlist && !isInstitutional/);
  assert.match(feedCardSource, /institutionalSecuritySecondaryLabel/);
  assert.match(feedCardSource, /Filed after:/);
  assert.match(feedCardSource, /tradeSide \? <span className="inline-flex justify-start">\{badge\}<\/span> : null/);
});

test("watchlist non-ticker targets link to their profile pages", () => {
  assert.match(tickerManagerSource, /import \{ departmentHref \} from "@\/lib\/departments"/);
  assert.match(tickerManagerSource, /import \{ insiderHref \} from "@\/lib\/insider"/);
  assert.match(tickerManagerSource, /import \{ memberHref \} from "@\/lib\/memberSlug"/);
  assert.match(tickerManagerSource, /function watchlistTargetHref\(target: WatchlistTarget\): string \| null/);
  assert.match(tickerManagerSource, /if \(target\.type === "member"\) return memberHref\(\{ name: label, memberId: value \}\)/);
  assert.match(tickerManagerSource, /if \(target\.type === "insider"\) return insiderHref\(label, value\)/);
  assert.match(tickerManagerSource, /if \(target\.type === "department"\) return departmentHref\(label \?\? value\)/);
  assert.match(tickerManagerSource, /className=\{`\$\{tickerLinkClassName\} block truncate`\}/);
});
