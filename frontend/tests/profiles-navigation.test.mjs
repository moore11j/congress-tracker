import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

test("top nav adds Profiles dropdown between Insights and Signals", () => {
  const nav = read("components/AppTopNav.tsx");

  assert.match(nav, /Profiles <span aria-hidden="true">&#9662;<\/span>/);
  assert.match(nav, /href: "\/profiles", label: "Overview"/);
  assert.match(nav, /href: "\/members", label: "Congress"/);
  assert.match(nav, /href: "\/insiders", label: "Insiders"/);
  assert.match(nav, /href: "\/institutions", label: "Institutions"/);
  assert.match(nav, /href: "\/departments", label: "Departments"/);
  assert.match(nav, /link\.href !== "\/insights"[\s\S]*<Fragment key="insights-profiles">[\s\S]*\{navLink\}[\s\S]*Profiles/);
});

test("terminal dropdowns open on hover and close when the pointer leaves", () => {
  const nav = read("components/AppTopNav.tsx");

  assert.match(nav, /onMouseEnter=\{\(\) => \{[\s\S]*setProfilesOpen\(true\)[\s\S]*setToolsOpen\(false\)[\s\S]*setCompanyOpen\(false\)/);
  assert.match(nav, /onMouseLeave=\{\(\) => setProfilesOpen\(false\)\}/);
  assert.match(nav, /onMouseLeave=\{\(\) => setToolsOpen\(false\)\}/);
  assert.match(nav, /onMouseLeave=\{\(\) => setCompanyOpen\(false\)\}/);
  assert.doesNotMatch(nav, /setProfilesOpen\(\(open\) => !open\)/);
  assert.doesNotMatch(nav, /setToolsOpen\(\(open\) => !open\)/);
  assert.doesNotMatch(nav, /setCompanyOpen\(\(open\) => !open\)/);
});

test("top nav adds Company dropdown after Pricing with matching arrows", () => {
  const nav = read("components/AppTopNav.tsx");

  assert.match(nav, /Tools <span aria-hidden="true">&#9662;<\/span>/);
  assert.match(nav, /Company <span aria-hidden="true">&#9662;<\/span>/);
  assert.match(nav, /const companyNavItems = \[[\s\S]*href: "\/about", label: "About"[\s\S]*href: "\/faq", label: "FAQ"[\s\S]*href: "\/contact", label: "Contact"[\s\S]*href: "\/terms", label: "Terms"[\s\S]*href: "\/privacy", label: "Privacy"/);
  assert.match(nav, /href="\/pricing"[\s\S]*Pricing[\s\S]*Company <span aria-hidden="true">&#9662;<\/span>/);
  assert.match(nav, /aria-label="Company"/);
  assert.match(nav, /data-company-link/);
});

test("profile landing routes include SEO metadata and enhanced database dashboards", () => {
  const pages = [
    ["app/profiles/page.tsx", "Investor & Government Profiles", "getProfilesSummary"],
    ["app/members/page.tsx", "Congress Stock Trading & Member Portfolios", "getCongressOverview"],
    ["app/insiders/page.tsx", "Insider Trading Activity & Corporate Insider Purchases", "getInsidersOverview"],
    ["app/institutions/page.tsx", "Institutional Holdings & 13F Position Changes", "getInstitutionsOverview"],
    ["app/departments/page.tsx", "Government Contracts & Department Spending"],
  ];

  for (const [path, title, apiFunction] of pages) {
    const source = read(path);
    assert.match(source, new RegExp(title.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
    assert.match(source, /EnhancedProfilesOverview|EnhancedCongressDashboard|EnhancedInsiderDashboard|EnhancedInstitutionDashboard|EnhancedGovernmentDashboard|ProfilePageHeader|SummaryCards|MetricGrid|DataPanel/);
    if (apiFunction) assert.match(source, new RegExp(apiFunction));
    assert.doesNotMatch(source, /sidebar/i);
  }
});

test("profile landing routes do not render static demo dashboards", () => {
  const pages = [
    "app/profiles/page.tsx",
    "app/members/page.tsx",
    "app/insiders/page.tsx",
    "app/institutions/page.tsx",
  ];

  for (const path of pages) {
    const source = read(path);
    assert.doesNotMatch(source, /ProfilesOverviewDashboard|CongressProfilesDashboard|InsiderProfilesDashboard|InstitutionProfilesDashboard/);
  }
});

test("institution profile landing uses the database overview", () => {
  const source = read("app/institutions/page.tsx");

  assert.match(source, /Institutional Holdings & 13F Position Changes/);
  assert.match(source, /getInstitutionsOverview/);
});

test("congress profile landing uses the database overview", () => {
  const source = read("app/members/page.tsx");

  assert.match(source, /Congress Stock Trading & Member Portfolios/);
  assert.match(source, /getCongressOverview/);
});

test("insider profile landing uses the database overview", () => {
  const source = read("app/insiders/page.tsx");

  assert.match(source, /Insider Trading Activity & Corporate Insider Purchases/);
  assert.match(source, /getInsidersOverview/);
});

test("profiles overview uses database summary instead of static dashboard data", () => {
  const source = read("app/profiles/page.tsx");

  assert.match(source, /Investor & Government Profiles/);
  assert.match(source, /getProfilesSummary/);
  assert.doesNotMatch(source, /ActivityFeed/);
  assert.doesNotMatch(source, /searchParams/);
});

test("profiles overview activity mix uses aggregate summary data", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /data\.activity_mix/);
  assert.match(source, /label="Records"/);
  assert.doesNotMatch(source, /const counts = categories\.map\(\(type\) => activity\.filter/);
});

test("profiles overview renders a 12-month line chart by profile type", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /<Panel title="Activity by profile type" action="12M">/);
  assert.match(source, /const periods = series\.slice\(-12\)/);
  assert.match(source, /const yTicks = \[axisMax, axisMax \* \.75, axisMax \* \.5, axisMax \* \.25, 0\]/);
  assert.match(source, /const diamondPoints/);
  assert.match(source, /aria-label="Monthly profile activity by type"/);
  assert.doesNotMatch(source, /profileActivityQuarters/);
});

test("latest profile activity keeps value visible on mobile", () => {
  const source = read("components/profiles/LatestProfileActivity.tsx");

  assert.match(source, /hidden w-\[4\.9rem\][^"]*sm:table-cell">Type/);
  assert.match(source, /hidden px-1\.5 py-1\.5 sm:table-cell/);
  assert.match(source, /w-\[4\.4rem\][^"]*text-right">Value/);
  assert.match(source, /formatValue\(item\.value\)/);
});

test("latest profile activity shows profile-level actions and relative dates", () => {
  const source = read("components/profiles/LatestProfileActivity.tsx");

  assert.match(source, /formatRelativeTime\(item\.time\)/);
  assert.match(source, /daysAgo <= 0 \? "Today" : `\$\{daysAgo\}D ago`/);
  assert.match(source, /function activityLabel\(item: ProfileActivityItem\)/);
  assert.match(source, /return "Contract Award"/);
  assert.match(source, /return "New Position"/);
  assert.match(source, /return "Decreased"/);
  assert.match(source, /return "Increased"/);
  assert.match(source, /prefetch=\{false\} className="text-emerald-200 hover:text-emerald-100"/);
});

test("congress top moving sectors uses bounded sparklines", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /function CompactSparkline/);
  assert.match(source, /grid-cols-\[minmax\(0,1fr\)_4\.75rem_5\.75rem\]/);
  assert.match(source, /h-5 w-full overflow-hidden/);
  assert.match(source, /<CompactSparkline points=\{row\.trend\.slice\(-12\)\} positive=/);
  assert.doesNotMatch(source, /<CongressTrend points=\{row\.trend\.slice\(-6\)\} \/>/);
});

test("congress trading snapshot chart renders area fill and full axes", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /const monthly = \(data\.monthly_activity \?\? \[\]\)\.slice\(-12\)/);
  assert.match(source, /<CongressSnapshotTrend points=\{monthly\.map\(\(row\) => \(\{ label: row\.period, value: row\.trades \}\)\)\} \/>/);
  assert.match(source, /function CongressSnapshotTrend/);
  assert.match(source, /const ticks = \[axisMax, axisMax \* \.75, axisMax \* \.5, axisMax \* \.25, 0\]/);
  assert.match(source, /fill="url\(#congress-trades-area\)"/);
  assert.match(source, /const diamondPoints = \(x: number, y: number\)/);
  assert.match(source, /key=\{`\$\{point\.label\}-diamond`\}/);
  assert.match(source, /gridTemplateColumns: `repeat\(\$\{points\.length\}, minmax\(0, 1fr\)\)`/);
  assert.match(source, /points\.map\(\(point\) => <span key=\{point\.label\}/);
});

test("congress dashboard adds full-width monthly buy sell activity chart", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /<Panel title="Activity over time" action="Monthly"><CongressActivityOverTime rows=\{monthly\} \/><\/Panel>/);
  assert.match(source, /function CongressActivityOverTime/);
  assert.match(source, /Buy Value \(USD\)/);
  assert.match(source, /Sell Value \(USD\)/);
  assert.match(source, /Total Trades/);
  assert.match(source, /const valueTicks = \[valueLimit, valueLimit \* 2 \/ 3, valueLimit \/ 3, 0, -valueLimit \/ 3, -valueLimit \* 2 \/ 3, -valueLimit\]/);
  assert.match(source, /const tradeTicks = \[tradeLimit, tradeLimit \* 5 \/ 6, tradeLimit \* 4 \/ 6, tradeLimit \* 3 \/ 6, tradeLimit \* 2 \/ 6, tradeLimit \/ 6, 0\]/);
  assert.match(source, /fill="#42d3a7"/);
  assert.match(source, /fill="#fb7185"/);
  assert.match(source, /stroke="#60a5fa"/);
});

test("insider trading snapshot chart renders monthly net value and trade axes", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /monthly_activity\?: Array<\{ period: string; net_value: number; buy_value: number; sell_value: number; trades: number \}>/);
  assert.match(source, /snapshotContent=\{<InsiderSnapshotChart rows=\{data\.monthly_activity \?\? \[\]\} \/>\}/);
  assert.match(source, /function InsiderSnapshotChart/);
  assert.match(source, /Net Value \(USD\)/);
  assert.match(source, /Total Trades/);
  assert.match(source, /const valueTicks = hasNegativeValue \?/);
  assert.match(source, /const tradeTicks = \[tradeLimit, tradeLimit \* \.8, tradeLimit \* \.6, tradeLimit \* \.4, tradeLimit \* \.2, 0\]/);
  assert.match(source, /fill="url\(#insider-net-area\)"/);
  assert.match(source, /stroke="#60a5fa"/);
  assert.match(source, /rounded-full border border-blue-100 bg-blue-400/);
  assert.match(source, /gridTemplateColumns: `repeat\(\$\{points\.length\}, minmax\(0, 1fr\)\)`/);
});

test("insider snapshot cards use icons and replace recent purchases with sector breadth", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /snapshotCards=\{\[/);
  assert.match(source, /label: "Top net buyer", row: snapshotDetailRow\(data\.top_insiders\[0\], "trades", "trade"\), icon: "buyer", subLabelKey: "snapshot_detail"/);
  assert.match(source, /label: "Most traded ticker", row: snapshotDetailRow\(data\.most_traded_stocks\[0\], "actor_count", "insider"\), icon: "ticker", subLabelKey: "snapshot_detail"/);
  assert.match(source, /label: "Cluster buying", row: snapshotDetailRow\(data\.cluster_buying\[0\], "unique_insiders", "insider"\), icon: "cluster", subLabelKey: "snapshot_detail"/);
  assert.match(source, /label: "Sector breadth", row: sectorBreadth, icon: "sector"/);
  assert.match(source, /function SnapshotIcon/);
  assert.match(source, /function snapshotDetailRow/);
  assert.match(source, /`\$\{formatNumber\(value\)\} \$\{value === 1 \? noun : `\$\{noun\}s`\}`/);
  assert.match(source, /function insiderSectorBreadth/);
  assert.match(source, /flavor === "insiders" \? \["Top net buyer", "Most traded ticker", "Cluster buying", "Sector breadth"\]/);
  assert.doesNotMatch(source, /flavor === "insiders" \? \["Top net buyer", "Most traded ticker", "Recent purchases", "Cluster buying"\]/);
});

test("insider net activity by sector uses signed red and green bar chart", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /sector_net_activity\?: Array<\{ sector: string; current_value: number; buy_value: number; sell_value: number; trades: number \}>/);
  assert.match(source, /netActivityContent=\{<InsiderNetSectorBars rows=\{data\.sector_net_activity \?\? \[\]\} \/>\}/);
  assert.match(source, /function InsiderNetSectorBars/);
  assert.match(source, /action=\{flavor === "insiders" \? "TTM" : undefined\}/);
  assert.match(source, /left-1\/2 w-px bg-slate-400\/45/);
  assert.match(source, /bg-emerald-400\/85/);
  assert.match(source, /bg-rose-400\/85/);
  assert.match(source, /formatSignedMoney\(row\.current_value\)/);
});

test("insider dashboard renders role mix, top movers, then notable trades", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /role_mix\?: Array<\{ label: string; value: number; percent: number \}>/);
  assert.match(api, /top_moving_sectors\?: Array<\{ sector: string; current_value: number; previous_value: number; current_activity_value: number; previous_activity_value: number; change_pct: number \| null; buy_value: number; sell_value: number; trades: number; trend: Array<\{ label: string; value: number \}> \}>/);
  assert.match(api, /recent_notable_trades\?: ProfileActivityItem\[\]/);
  assert.match(source, /insiderRoleMix=\{data\.role_mix \?\? \[\]\}/);
  assert.match(source, /insiderTopMovingSectors=\{data\.top_moving_sectors \?\? \[\]\}/);
  assert.match(source, /insiderRecentNotableTrades=\{data\.recent_notable_trades \?\? \[\]\}/);
  assert.match(source, /flavor === "insiders" \? <section className="grid gap-3 xl:grid-cols-\[\.85fr_1\.05fr_1\.1fr\]"><Panel title="Transaction mix by role \(TTM\)"><InsiderRoleMixDonut rows=\{insiderRoleMix\} \/><\/Panel><Panel title="Top moving sectors \(vs prior TTM\)"><InsiderTopMovingSectors rows=\{insiderTopMovingSectors\} \/><\/Panel><InsiderNotableTrades items=\{insiderRecentNotableTrades\.length \? insiderRecentNotableTrades : recent\} \/><\/section>/);
  assert.match(source, /function InsiderRoleMixDonut/);
  assert.match(source, /label="Total trades"/);
  assert.match(source, /function InsiderTopMovingSectors/);
  assert.match(source, /Net activity \(TTM\)/);
  assert.match(source, /Vs prior TTM/);
  assert.match(source, /function InsiderNotableTrades/);
  assert.match(source, /\["Insider", "Ticker", "Action", "Value", "Date"\]/);
  assert.match(source, /const insider = item\.profile_href \? <Link href=\{item\.profile_href\}/);
});

test("insider dashboard adds full-width monthly activity chart after notable trades", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /insiderMonthlyActivity=\{data\.monthly_activity \?\? \[\]\}/);
  assert.match(source, /insiderMonthlyActivity\?: NonNullable<InsidersOverviewResponse\["monthly_activity"\]>/);
  const notableIndex = source.indexOf("<InsiderNotableTrades items={insiderRecentNotableTrades.length ? insiderRecentNotableTrades : recent} />");
  const activityIndex = source.indexOf('<Panel title="Activity over time (USD)" action="Monthly"><InsiderActivityOverTime rows={insiderMonthlyActivity} /></Panel>');
  assert.ok(notableIndex > 0);
  assert.ok(activityIndex > notableIndex);
  assert.match(source, /function InsiderActivityOverTime/);
  assert.match(source, /Buy Value \(USD\)/);
  assert.match(source, /Sell Value \(USD\)/);
  assert.match(source, /Total Trades/);
  assert.match(source, /const valueTicks = \[valueLimit, valueLimit \/ 2, 0, -valueLimit \/ 2, -valueLimit\]/);
  assert.match(source, /const tradeTicks = \[tradeLimit, tradeLimit \* \.75, tradeLimit \* \.5, tradeLimit \* \.25, 0\]/);
  assert.match(source, /fill="#42d3a7"/);
  assert.match(source, /fill="#fb7185"/);
  assert.match(source, /stroke="#60a5fa"/);
  assert.match(source, /rounded-full border border-blue-100 bg-blue-400/);
  assert.match(source, /gridTemplateColumns: `repeat\(\$\{points\.length\}, minmax\(0, 1fr\)\)`/);
});

test("congress metric cards use shaded semantic trend colors", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /Latest 12 months/);
  assert.match(source, /function CongressTrend\(\{ points, tone = "green" \}/);
  assert.match(source, /fill=\{`url\(#\$\{gradientId\}\)`\}/);
  assert.match(source, /metric\.label === "Total Sell Value" \? "red"/);
  assert.match(source, /metric\.label === "Active Members" \|\| metric\.label === "Average Trade Size" \? "blue"/);
  assert.match(source, /metric\.label === "Total Trades" && typeof metric\.change_pct === "number" && metric\.change_pct < 0 \? "red" : "green"/);
});

test("profile snapshot trend chart renders usable axes", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /grid-cols-\[3\.8rem_minmax\(0,1fr\)\]/);
  assert.match(source, /formatAxisValue\(tick\)/);
  assert.match(source, /key=\{`\$\{point\.label\}-snapshot-diamond`\}/);
  assert.match(source, /fill="#55e3b0" stroke="#07101f"/);
  assert.match(source, /gridTemplateColumns: `repeat\(\$\{series\.length\}, minmax\(0, 1fr\)\)`/);
  assert.match(source, /series\.map\(\(point\) => <span key=\{point\.label\}/);
});

test("congress snapshot cards label real net values and sector trade share", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /<CongressSnapshot icon="member" tone="green" label="Most active member"/);
  assert.match(source, /<CongressSnapshot icon="ticker" tone="purple" label="Most traded ticker"/);
  assert.match(source, /<CongressSnapshot icon="buyer" tone="green" label="Top buyer"/);
  assert.match(source, /<CongressSnapshot icon="sector" tone="amber" label="Most active sector"/);
  assert.match(source, /function CongressSnapshotIcon/);
  assert.match(source, /border-violet-300\/35 bg-violet-400\/15 text-violet-200/);
  assert.match(source, /border-amber-300\/35 bg-amber-400\/15 text-amber-200/);
  assert.match(source, /valueKey="net_value" suffix=" Net Value"/);
  assert.match(source, /valueKey="value" suffix=" Net Buys"/);
  assert.match(source, /valueKey="trade_percent" format="percent" suffix=" of Trades"/);
  assert.match(source, /function formatCongressSnapshotValue/);
  assert.match(source, /format === "percent"/);
  assert.match(source, /const valueClass = typeof value === "number" && value < 0 \? "text-rose-300" : "text-emerald-300"/);
});

test("congress most traded stocks table shows trades and signed net values", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /\["net_value", "Net value", "signed_currency"\], \["trades", "Trades", "number"\]/);
  assert.match(source, /type CellFormat = "currency" \| "signed_currency"/);
  assert.match(source, /format === "signed_currency"/);
  assert.match(source, /text-emerald-300/);
  assert.match(source, /text-rose-300/);
});

test("congress notable trades normalize and color action labels", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /const member = item\.profile_href \? <Link href=\{item\.profile_href\}/);
  assert.match(source, /hover:text-emerald-100 hover:underline/);
  assert.match(source, /const action = congressTradeAction\(item\.activity\)/);
  assert.match(source, /action\.label/);
  assert.match(source, /normalized\.includes\("purchase"\) \|\| normalized === "buy"/);
  assert.match(source, /label: "Buy", className: "text-emerald-300"/);
  assert.match(source, /normalized\.includes\("sale"\) \|\| normalized === "sell"/);
  assert.match(source, /label: "Sell", className: "text-rose-300"/);
});

test("departments vendors expose ticker comparisons and linked departments", () => {
  const source = read("components/profiles/EnhancedGovernmentDashboard.tsx");

  assert.match(source, /function GrowingVendors/);
  assert.match(source, /\["Vendor", "Ticker", "Current value", "Prior period", "Change vs prior"\]/);
  assert.match(source, /rows\.slice\(0, 10\)/);
  assert.match(source, /<Link href=\{href\} prefetch=\{false\}/);
  assert.match(source, /\["department", "Department", "department_link"\]/);
  assert.match(source, /format === "department_link"/);
  assert.match(source, /title="Top departments by contract value"[\s\S]*?\["previous_value", "Prior period", "currency"\][\s\S]*?\["change_pct", "Change", "percent"\]/);
  assert.doesNotMatch(source, /Vs \$\{comparison\}/);
  assert.match(source, /title="Recent notable awards"[\s\S]*?tableClassName="min-w-\[26rem\]"[\s\S]*?\["symbol", "Ticker", "link"\]/);
  assert.doesNotMatch(source, /title="Recent notable awards"[\s\S]*?\["company", "Vendor", "link"\]/);
  assert.match(source, /xl:grid-cols-\[\.85fr_1\.5fr_\.95fr\]/);
});

test("institutional dashboard uses mockup-style net change charts", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /institutionalActivity=\{data\.institutional_activity_over_time \?\? \[\]\}/);
  assert.match(source, /Net position change by sector/);
  assert.match(source, /function NetPositionChangeBySector/);
  assert.match(source, /function InstitutionalActivityOverTime/);
  assert.match(source, /institutionalActivity=\{institutionalActivity\}/);
  assert.match(source, /function institutionalMetricSeries\(label: string, rows: InstitutionalActivityPeriod\[\]\)/);
  assert.match(source, /position_increase_count/);
  assert.match(source, /position_decrease_count/);
  assert.match(source, /net_value_change/);
  assert.match(source, /\["qoq_change", "QoQ change", "signed_percent"\]/);
  assert.match(source, /\["increase_value", "Increase", "positive_currency"\]/);
  assert.match(source, /format === "signed_percent"/);
  assert.match(source, /format === "signed_currency" \|\| format === "positive_currency"/);
  assert.match(source, /\(sell\|decrease\)\/\.test\(metric\.label\.toLowerCase\(\)\) \? "red" : "green"/);
  assert.match(source, /const valueLimit = 2_000_000_000_000/);
  assert.match(source, /Position increases/);
  assert.match(source, /Position decreases/);
  assert.match(source, /Total positions/);
  assert.match(source, /rounded-full border border-blue-100 bg-blue-400/);
  assert.match(source, /flavor === "institutions" \? <section className="grid gap-3 2xl:grid-cols-\[1\.15fr_1\.25fr_\.65fr\]">/);
  assert.match(source, /<DataTable \{\.\.\.left\} tableClassName="min-w-\[36rem\]" \/>/);
  assert.match(source, /tableClassName = "min-w-\[38rem\]"/);
  assert.doesNotMatch(source, /flavor === "institutions"[\s\S]{0,140}Recent notable activity/);
});
