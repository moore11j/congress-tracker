import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const frameUtil = read("components/ui/resultsTableFrame.ts");
const screenerPage = read("app/screener/page.tsx");
const screenerClient = read("components/screener/ScreenerResultsClient.tsx");
const leaderboardPage = read("app/leaderboards/congress-traders/page.tsx");
const leaderboardTable = read("components/leaderboards/CongressTraderLeaderboardTable.tsx");
const leaderboardClient = read("components/leaderboards/CongressTraderLeaderboardClientResults.tsx");
const signalsPage = read("app/signals/page.tsx");
const signalsClient = read("components/signals/SignalsResultsClient.tsx");

test("results table frame stays uncapped through ten rows and caps longer result sets", () => {
  assert.match(frameUtil, /rowCount > 10/);
  assert.match(frameUtil, /max-h-\[35\.25rem\]/);
  assert.match(frameUtil, /overflow-y-auto/);
  assert.match(frameUtil, /overflow-x-auto/);
  assert.match(frameUtil, /stickyResultsTableHeaderClassName = "sticky top-0 z-10"/);
  assert.match(frameUtil, /uncappedFrameClassName = "max-w-full overflow-x-auto overflow-y-hidden"/);
});

test("screener uses pagination plus an internal scroll frame for long selected page sizes", () => {
  assert.match(screenerPage, /PAGE_SIZE_OPTIONS/);
  assert.match(screenerPage, /pageSize=\{pageSize\}/);
  assert.match(screenerPage, /resultsTableFrameClassName\(rows\.length\)/);
  assert.match(screenerClient, /resultsTableFrameClassName\(rows\.length\)/);
  assert.match(screenerPage, /stickyResultsTableHeaderClassName/);
  assert.match(screenerClient, /stickyResultsTableHeaderClassName/);
});

test("leaderboard long result sets use the shared internal scroll frame", () => {
  assert.match(leaderboardPage, /const LIMIT_OPTIONS = \[10, 25, 50, 100\] as const/);
  assert.match(leaderboardPage, /LIMIT_OPTIONS\.map\(\(option\) =>/);
  assert.match(leaderboardPage, /limit: option/);
  assert.match(leaderboardTable, /resultsTableFrameClassName\(data\.rows\.length\)/);
  assert.match(leaderboardTable, /stickyResultsTableHeaderClassName/);
  assert.match(leaderboardClient, /limit,/);
});

test("signals results are capped without reducing fetched or rendered rows", () => {
  assert.match(signalsPage, /getSignalsAll\(\{[\s\S]*?limit,/);
  assert.match(signalsClient, /getSignalsAll\(\{[\s\S]*?limit,/);
  assert.match(signalsPage, /resultsTableFrameClassName\(items\.length, \{ always: true \}\)/);
  assert.match(signalsClient, /resultsTableFrameClassName\(items\.length, \{ always: true \}\)/);
  assert.match(signalsPage, /mobileResultsScrollFrameClassName/);
  assert.match(signalsClient, /mobileResultsScrollFrameClassName/);
  assert.match(signalsPage, /items\.map\(\(it\) =>/);
  assert.match(signalsClient, /items\.map\(\(item\) =>/);
});

test("mobile signal rows keep constrained labels and avoid horizontal page overflow", () => {
  assert.match(frameUtil, /mobileResultsScrollFrameClassName =\s*"max-w-full overflow-x-hidden overflow-y-auto/);
  assert.match(signalsPage, /grid grid-cols-\[minmax\(0,1fr\)_auto\]/);
  assert.match(signalsClient, /grid grid-cols-\[minmax\(0,1fr\)_auto\]/);
  assert.match(signalsPage, /min-w-0 truncate/);
  assert.match(signalsClient, /min-w-0 truncate/);
});
