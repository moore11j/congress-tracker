import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();

function read(path) {
  return readFileSync(join(root, path), "utf8");
}

test("ticker activity sections use shared paginated footers", () => {
  const page = read("app/ticker/[symbol]/page.tsx");

  assert.match(page, /const ACTIVITY_PAGE_SIZE = 20/);
  assert.match(page, /const congressPage = clampPage\(one\(sp, "congress_page"\)\)/);
  assert.match(page, /const insiderPage = clampPage\(one\(sp, "insider_page"\)\)/);
  assert.match(page, /offset: congressPage \* ACTIVITY_PAGE_SIZE/);
  assert.match(page, /offset: insiderPage \* ACTIVITY_PAGE_SIZE/);
  assert.match(page, /include_total: 1/);
  assert.match(page, /tape: "congress"/);
  assert.match(page, /tape: "insider"/);
  assert.match(page, /pageParam="congress_page"/);
  assert.match(page, /pageParam="insider_page"/);
  assert.match(page, /pageParam="contracts_page"/);
  assert.match(page, /sectionId="congress-activity"/);
  assert.match(page, /sectionId="insider-activity"/);
  assert.match(page, /sectionId="government-contracts-activity"/);
});

test("activity footer preserves scroll and anchors back to the section", () => {
  const footer = read("components/ticker/TickerActivityPaginationFooter.tsx");
  const page = read("app/ticker/[symbol]/page.tsx");

  assert.match(footer, /event\.preventDefault\(\)/);
  assert.match(footer, /router\.push\(nextUrl,\s*\{ scroll: false \}\)/);
  assert.match(footer, /href=\{buildHref\(page \+ 1, true\)\}/);
  assert.match(footer, /role="button"/);
  assert.match(footer, /window\.sessionStorage\.setItem\(pendingScrollKey, `\$\{sectionId\}:\$\{nextPage\}`\)/);
  assert.match(footer, /section\.scrollIntoView\(\{ behavior: "smooth", block: "start" \}\)/);
  assert.match(footer, /scrollRegion\.scrollTop = 0/);
  assert.match(page, /data-activity-scroll-region/);
});

test("activity footer renders showing range and show-more controls", () => {
  const footer = read("components/ticker/TickerActivityPaginationFooter.tsx");

  assert.match(footer, /const showingStart = total > 0 \? Math\.min\(page \* safeLimit \+ 1, total\) : 0/);
  assert.match(footer, /const showingEnd = total > 0 \? Math\.min\(showingStart \+ Math\.max\(itemCount - 1, 0\), total\) : 0/);
  assert.match(footer, /Showing \{showingStart\}&ndash;\{showingEnd\} of \{total\}/);
  assert.match(footer, />\s*Previous\s*</);
  assert.match(footer, />\s*Show more\s*</);
});

test("insider activity cards render one filed-price-first price", () => {
  const page = read("app/ticker/[symbol]/page.tsx");
  const tradeDisplay = read("lib/tradeDisplay.ts");

  assert.match(tradeDisplay, /const displayPrice = reported\.price \?\? price/);
  assert.match(page, /price=\{formatActivityPrice\(display\.displayPrice\)\}/);
  assert.match(page, /minimumFractionDigits: hasDecimals \? 2 : 0/);
  assert.match(page, /maximumFractionDigits: hasDecimals \? 2 : 0/);
  assert.doesNotMatch(page, /priceSubtext=\{display\.reportedLabel\}/);
  assert.doesNotMatch(page, /Reported: USD/);
});
