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
  assert.match(source, /gridTemplateColumns: `repeat\(\$\{points\.length\}, minmax\(0, 1fr\)\)`/);
  assert.match(source, /points\.map\(\(point\) => <span key=\{point\.label\}/);
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
  assert.match(source, /label: "Top net buyer", row: data\.top_insiders\[0\], icon: "buyer"/);
  assert.match(source, /label: "Most traded ticker", row: data\.most_traded_stocks\[0\], icon: "ticker"/);
  assert.match(source, /label: "Cluster buying", row: data\.cluster_buying\[0\], icon: "cluster"/);
  assert.match(source, /label: "Sector breadth", row: sectorBreadth, icon: "sector"/);
  assert.match(source, /function SnapshotIcon/);
  assert.match(source, /function insiderSectorBreadth/);
  assert.match(source, /flavor === "insiders" \? \["Top net buyer", "Most traded ticker", "Cluster buying", "Sector breadth"\]/);
  assert.doesNotMatch(source, /flavor === "insiders" \? \["Top net buyer", "Most traded ticker", "Recent purchases", "Cluster buying"\]/);
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
  assert.match(source, /gridTemplateColumns: `repeat\(\$\{series\.length\}, minmax\(0, 1fr\)\)`/);
  assert.match(source, /series\.map\(\(point\) => <span key=\{point\.label\}/);
});

test("congress snapshot cards label real net values and sector trade share", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /valueKey="net_value" suffix=" Net Value"/);
  assert.match(source, /valueKey="value" suffix=" Net Buys"/);
  assert.match(source, /valueKey="trade_percent" format="percent" suffix=" of Trades"/);
  assert.match(source, /function formatCongressSnapshotValue/);
  assert.match(source, /format === "percent"/);
});

test("congress notable trades normalize and color action labels", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /const action = congressTradeAction\(item\.activity\)/);
  assert.match(source, /action\.label/);
  assert.match(source, /normalized\.includes\("purchase"\) \|\| normalized === "buy"/);
  assert.match(source, /label: "Buy", className: "text-emerald-300"/);
  assert.match(source, /normalized\.includes\("sale"\) \|\| normalized === "sell"/);
  assert.match(source, /label: "Sell", className: "text-rose-300"/);
});

test("institutional dashboard uses mockup-style net change charts", () => {
  const source = read("components/profiles/EnhancedProfileDashboards.tsx");

  assert.match(source, /institutionalActivity=\{data\.institutional_activity_over_time \?\? \[\]\}/);
  assert.match(source, /Net position change by sector/);
  assert.match(source, /function NetPositionChangeBySector/);
  assert.match(source, /function InstitutionalActivityOverTime/);
  assert.match(source, /const valueLimit = 2_000_000_000_000/);
  assert.match(source, /Position increases/);
  assert.match(source, /Position decreases/);
  assert.match(source, /Total positions/);
  assert.match(source, /rounded-full border border-blue-100 bg-blue-400/);
  assert.doesNotMatch(source, /flavor === "institutions"[\s\S]{0,140}Recent notable activity/);
});
