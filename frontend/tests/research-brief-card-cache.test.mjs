import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("published research cards use a short shared server cache without caching browser reads", () => {
  const api = readFileSync(join(process.cwd(), "lib", "api.ts"), "utf8");

  assert.match(api, /const GENERATED_RESEARCH_BRIEF_CARDS_SERVER_CACHE_TTL_MS = 60_000/);
  assert.match(api, /const request = \(\) => fetchPublicJson<\{ items: PublicResearchBriefCard\[\] \}>\(url/);
  assert.match(api, /if \(typeof window !== "undefined"\) return request\(\)/);
  assert.match(api, /serverCachedJson\(\s*`generated-research-brief-cards:\$\{url\}`,[\s\S]*GENERATED_RESEARCH_BRIEF_CARDS_SERVER_CACHE_TTL_MS/);
});
