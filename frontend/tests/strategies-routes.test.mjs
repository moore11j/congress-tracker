import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";

const read = (path) => readFile(new URL(path, import.meta.url), "utf8");

test("Strategies routes use the persisted API and do not hardcode mock performance", async () => {
  const [directory, detail, api] = await Promise.all([
    read("../app/strategies/page.tsx"),
    read("../app/strategies/[slug]/page.tsx"),
    read("../lib/api.ts"),
  ]);

  assert.match(directory, /getStrategies\(/);
  assert.match(detail, /getStrategy\(/);
  assert.match(api, /buildApiUrl\("\/api\/strategies"/);
  assert.doesNotMatch(directory, /42\.6%|38\.2%|Bullish Confirmation/);
});

test("Strategies is a feature-flagged primary navigation destination", async () => {
  const nav = await read("../components/AppTopNav.tsx");
  assert.match(nav, /NEXT_PUBLIC_STRATEGIES_ENABLED/);
  assert.match(nav, /href: "\/strategies", label: "Strategies"/);
});
