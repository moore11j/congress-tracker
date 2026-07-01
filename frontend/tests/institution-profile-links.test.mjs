import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

const root = path.resolve(process.cwd());

function read(relativePath) {
  return readFileSync(path.join(root, relativePath), "utf8");
}

test("institutional actors link to institution profile routes instead of member routes", () => {
  const helper = read("lib/institution.ts");
  const feedCard = read("components/feed/FeedCard.tsx");
  const signalsPage = read("app/signals/page.tsx");

  assert.match(helper, /return normalized \? `\/institution\/\$\{encodeURIComponent\(normalized\)\}` : null/);
  assert.match(feedCard, /const institutionProfileHref = isInstitutional/);
  assert.match(feedCard, /institutionHref\(item\.member\?\.bioguide_id/);
  assert.match(feedCard, /isInstitutional \? \(/);
  assert.match(feedCard, /href=\{memberHref\(\{ name: item\.member\?\.name, memberId: item\.member\?\.bioguide_id \}\)\}/);
  assert.match(signalsPage, /institutionHref\(it\.reporting_cik \?\? it\.reportingCik \?\? it\.member_bioguide_id\)/);
  assert.match(signalsPage, /href=\{memberHref\(\{ name: it\.who, memberId: it\.member_bioguide_id \}\)\}/);
});

test("cik-null institutional actors render as text rather than fake institution pages", () => {
  const feedCard = read("components/feed/FeedCard.tsx");
  const signalsPage = read("app/signals/page.tsx");

  assert.match(feedCard, /institutionProfileHref \? \(/);
  assert.match(feedCard, /<span className="min-w-0 truncate text-lg font-semibold text-white">/);
  assert.match(signalsPage, /institutionProfileHref \? \(/);
  assert.match(signalsPage, /<span className="min-w-0 truncate text-slate-100">/);
});

test("institution profile page uses clean 13F copy and no source plumbing terms", () => {
  const page = read("app/institution/[cik]/page.tsx");
  const chart = read("components/institution/HoldingsAllocationChart.tsx");
  const forbidden = /\b(FMP|provider|vendor|cache|Buy|Sell|Bought|Sold|Real-time)\b/i;

  assert.match(page, /Institutional profiles are available on Pro\./);
  assert.match(page, /Unlock reported 13F holdings, portfolio allocation, and recent institutional activity\./);
  assert.match(page, /Reported Increase/);
  assert.match(page, /Reported Reduction/);
  assert.match(page, /Reported Exit/);
  assert.match(page, /New Position/);
  assert.doesNotMatch(page, forbidden);
  assert.doesNotMatch(chart, forbidden);
});

test("institution profile renders holdings allocation before activity and holdings tables", () => {
  const page = read("app/institution/[cik]/page.tsx");

  assert.match(page, /import \{ HoldingsAllocationChart \}/);
  assert.match(page, /<HoldingsAllocationChart/);
  assert.ok(page.indexOf("<HoldingsAllocationChart") < page.indexOf("<ActivitySection"));
  assert.ok(page.indexOf("<ActivitySection") < page.indexOf("<HoldingsSection"));
});

test("holdings allocation chart uses top ten plus other and compact dark responsive layout", () => {
  const chart = read("components/institution/HoldingsAllocationChart.tsx");

  assert.match(chart, /const TOP_HOLDINGS_LIMIT = 10/);
  assert.match(chart, /symbol: "Other"/);
  assert.match(chart, /Remaining reported holdings/);
  assert.match(chart, /chartSlices\.length >= 2/);
  assert.match(chart, /No reported holdings are available for this institution yet\./);
  assert.match(chart, /Filing detected; reported holdings are not available yet\./);
  assert.match(chart, /Reported holdings allocation/);
  assert.match(chart, /Reported value/);
  assert.match(chart, /Portfolio weight/);
  assert.match(chart, /Shares/);
  assert.match(chart, /overflow-hidden/);
  assert.match(chart, /min-w-0/);
  assert.match(chart, /lg:grid-cols-\[minmax\(220px,340px\)_minmax\(0,1fr\)\]/);
});
