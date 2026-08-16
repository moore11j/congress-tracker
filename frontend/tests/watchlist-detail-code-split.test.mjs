import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("watchlist detail defers supplemental panels without removing them", () => {
  const source = readFileSync(join(process.cwd(), "components/watchlists/WatchlistDetailContent.tsx"), "utf8");

  assert.match(source, /dynamic\(\s*\(\) => import\("@\/components\/notifications\/NotificationPreferences"/);
  assert.match(source, /dynamic\(\s*\(\) => import\("@\/components\/watchlists\/ConfirmationMonitoringRefreshButton"/);
  assert.match(source, /<WatchlistTickerManager/);
  assert.match(source, /<WatchlistRecentActivity/);
});

test("watchlist detail keeps the session-recovery client in a separate route chunk", () => {
  const source = readFileSync(join(process.cwd(), "app/watchlists/[id]/page.tsx"), "utf8");

  assert.match(source, /nextDynamic\(\s*\(\) => import\("@\/components\/watchlists\/WatchlistDetailClient"/);
  assert.match(source, /<WatchlistDetailClient/);
});
