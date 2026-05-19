import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

test("feed saved views and URL params preserve advanced filters", () => {
  const page = read("app/page.tsx");
  const filters = read("components/feed/FeedFiltersServer.tsx");

  for (const key of ["filed_after_max", "pnl_min", "pnl_max", "signal_min"]) {
    assert.match(page, new RegExp(`"${key}"`));
    assert.match(filters, new RegExp(`"${key}"`));
    assert.match(filters, new RegExp(`name="${key}"`));
  }
  assert.match(filters, /value="etf_fund"/);
  assert.match(filters, /FeedRoleAutosuggestEnhancer/);
});

test("feed cards distinguish actor and ticker net flow labels", () => {
  const card = read("components/feed/FeedCard.tsx");

  assert.match(card, /Member Net 30D:/);
  assert.match(card, /Insider Net 30D:/);
  assert.match(card, /Ticker Net 30D:/);
  assert.match(card, /\(isInsider \|\| isCongress\) && symbol && symbolNet30d !== null/);
});

test("global search UI advertises insider search and renders insider grouping", () => {
  const search = read("components/GlobalSearch.tsx");

  assert.match(search, /insider: "Insiders"/);
  assert.match(search, /insider: "Insider"/);
  assert.match(search, /members, insiders/);
});

test("insider profile preserves issuer-scoped search context", () => {
  const page = read("app/insider/[slug]/page.tsx");
  const api = read("lib/api.ts");

  assert.match(page, /one\(sp, "issuer"\)/);
  assert.match(page, /issuer && companyText/);
  assert.match(api, /issuer\?: string/);
  assert.match(api, /issuer: params\?\.issuer/);
});
