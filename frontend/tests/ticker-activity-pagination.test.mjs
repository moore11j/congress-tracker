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
  assert.match(page, /const ACTIVITY_FETCH_SIZE = ACTIVITY_PAGE_SIZE \+ 1/);
  assert.match(page, /const congressPage = clampPage\(one\(sp, "congress_page"\)\)/);
  assert.match(page, /const insiderPage = clampPage\(one\(sp, "insider_page"\)\)/);
  assert.match(page, /offset: congressPage \* ACTIVITY_PAGE_SIZE/);
  assert.match(page, /offset: insiderPage \* ACTIVITY_PAGE_SIZE/);
  assert.match(page, /limit: ACTIVITY_FETCH_SIZE/);
  assert.doesNotMatch(page, /include_total: 1/);
  assert.match(page, /function visibleActivityItems\(response: EventsResponse, limit = ACTIVITY_PAGE_SIZE\)/);
  assert.match(page, /tape: "congress"/);
  assert.match(page, /tape: "insider"/);
  assert.match(page, /pageParam="congress_page"/);
  assert.match(page, /pageParam="insider_page"/);
  assert.match(page, /pageParam="contracts_page"/);
  assert.match(page, /sectionId="congress-activity"/);
  assert.match(page, /sectionId="insider-activity"/);
  assert.match(page, /sectionId="government-contracts-activity"/);
  assert.match(page, /id="congress-activity-status"/);
  assert.match(page, /id="insider-activity-status"/);
  assert.match(page, /activityCountLabel\(congressEventsTotal, congressEvents\.length, "event"\)/);
  assert.match(page, /activityCountLabel\(insiderEventsTotal, insiderEvents\.length, "event"\)/);
  assert.match(page, /statusElementId="congress-activity-status"/);
  assert.match(page, /statusElementId="insider-activity-status"/);
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
  const signalClient = read("components/ticker/TickerSignalActivityClient.tsx");
  const detailClient = read("components/ticker/TickerActivityDetailClient.tsx");

  assert.match(footer, /const hasExactTotal = typeof total === "number"/);
  assert.match(footer, /const showingStart = itemCount > 0 \? page \* safeLimit \+ 1 : 0/);
  assert.match(footer, /More available/);
  assert.doesNotMatch(footer, /Showing \{showingStart\}&ndash;\{showingEnd\} of \{total\}/);
  assert.match(footer, />\s*Previous\s*</);
  assert.match(footer, />\s*Show more\s*</);
  assert.match(signalClient, /function ActivityRangeFooter\(\{ itemCount, total \}/);
  assert.match(signalClient, /Showing 1-\$\{Math\.min\(itemCount, exactTotal\)\} of \$\{exactTotal\}/);
  assert.match(signalClient, /<ActivityRangeFooter itemCount=\{visibleItems\.slice\(0, 20\)\.length\} total=\{total\} \/>/);
  assert.match(detailClient, /function ActivityRangeFooter\(\{ itemCount \}/);
  assert.match(detailClient, /Showing 1-\{itemCount\}/);
  assert.match(detailClient, /activityStatusLabel\(\{ loading, unavailable, itemCount: items\.length \}\)/);
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

test("ticker trade activity grids disclose price without gain loss and preserve participant summaries", () => {
  const page = read("app/ticker/[symbol]/page.tsx");
  const detailClient = read("components/ticker/TickerActivityDetailClient.tsx");
  const signalClient = read("components/ticker/TickerSignalActivityClient.tsx");
  const tradeDisplay = read("lib/tradeDisplay.ts");

  assert.match(page, /import \{ resolveCongressActivityPrice, resolveInsiderActivityDisplay \} from "@\/lib\/tradeDisplay"/);
  assert.match(tradeDisplay, /export function resolveCongressActivityPrice\(record: Record<string, unknown>\)/);
  assert.match(tradeDisplay, /"transactionPricePerShare"/);
  assert.match(tradeDisplay, /"trade_price",\s*"tradePrice"/);
  assert.match(page, /const congressParticipantEvents = side === "all"[\s\S]*\? congressEvents/);
  assert.match(page, /const insiderParticipantEvents = side === "all"[\s\S]*\? insiderEvents/);
  assert.match(page, /for \(const event of congressParticipantEvents\)/);
  assert.match(page, /for \(const event of insiderParticipantEvents\)/);
  assert.match(page, /price=\{displayPrice !== null \? formatCurrency\(displayPrice\) : "-"\}/);
  assert.match(page, /price=\{formatActivityPrice\(display\.displayPrice\)\}/);
  assert.match(page, /showGainLoss=\{false\}/);
  assert.match(page, /memberHref\(\{ name: memberName, memberId: event\.member_bioguide_id \?\? undefined \}\)/);
  assert.match(page, /const strengthLabel = formatSignalStrengthText\(signal\.band\)/);
  assert.match(page, /const strengthLabel = formatSignalStrengthText\(display\.signal\.band\)/);
  assert.match(page, /dateLabel=\{formatDateShort\(resolveCongressReportDate\(event\)\)\}/);
  assert.match(page, /dateLabel=\{formatDateShort\(display\.filingDate \?\? resolveInsiderFilingDate\(event\)\)\}/);

  assert.match(detailClient, /import \{ resolveCongressActivityPrice, resolveInsiderActivityDisplay \} from "@\/lib\/tradeDisplay"/);
  assert.match(detailClient, /resolveCongressActivityPrice\(event as Record<string, unknown>\)/);
  assert.match(detailClient, /price=\{formatPrice\(price\)\}/);
  assert.match(detailClient, /SmartSignalPill score=\{smartSignal\.score\}/);
  assert.match(detailClient, /LockedSmartSignalPill band=\{smartSignal\.band\}/);
  assert.match(detailClient, /memberHref\(\{ name: memberName, memberId: event\.member_bioguide_id \}\)/);
  assert.match(detailClient, /formatSignalStrengthText\(signal\.band\)/);
  assert.match(detailClient, /formatSignalStrengthText\(display\.signal\.band\)/);
  assert.match(tradeDisplay, /"trade_price", "tradePrice", "reported_price", "reportedPrice"/);

  assert.match(signalClient, /SmartSignalPill score=\{signal\.smart_score \?\? null\}/);
  assert.match(signalClient, /sm:grid-cols-\[minmax\(170px,1\.6fr\)_minmax\(92px,\.7fr\)_minmax\(128px,\.95fr\)_minmax\(92px,auto\)\]/);
  assert.doesNotMatch(signalClient, /gainLossLabel/);
  assert.doesNotMatch(signalClient, /pnlClass/);
  assert.doesNotMatch(signalClient, /pnl=\{/);
});
