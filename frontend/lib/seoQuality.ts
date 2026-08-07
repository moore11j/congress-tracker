import type { Metadata } from "next";
import type { MemberProfile, TickerProfile } from "@/lib/types";
import type { DepartmentProfileResponse, InsiderSummary, InstitutionProfileResponse } from "@/lib/api";

export type SeoEntityType = "ticker" | "member" | "insider" | "institution" | "department" | "research" | "comparison" | "screener" | "market";

export type SeoPilotPage = {
  type: SeoEntityType;
  path: string;
  lastmod: string;
  rationale: string;
};

export const seoPilotPages = {
  tickers: [
    { type: "ticker", path: "/ticker/NVDA", lastmod: "2026-08-01", rationale: "Liquid large-cap pilot with broad ticker research modules." },
    { type: "ticker", path: "/ticker/AAPL", lastmod: "2026-08-01", rationale: "Widely searched issuer with insider, fundamentals, and market context." },
    { type: "ticker", path: "/ticker/MSFT", lastmod: "2026-08-01", rationale: "Large-cap pilot for fundamentals and market context." },
    { type: "ticker", path: "/ticker/TSLA", lastmod: "2026-08-01", rationale: "High-search pilot with public-market and disclosure context." },
    { type: "ticker", path: "/ticker/PLTR", lastmod: "2026-08-01", rationale: "Alternative-data and government-contract research pilot." },
    { type: "ticker", path: "/ticker/LMT", lastmod: "2026-08-01", rationale: "Government-contract research pilot with issuer context." },
  ],
  members: [
    { type: "member", path: "/member/nancy-pelosi", lastmod: "2026-08-01", rationale: "Canonical public member page with meaningful disclosure activity and profile context." },
  ],
  insiders: [
    { type: "insider", path: "/insider/tim-cook-0001214156", lastmod: "2026-08-01", rationale: "Canonical public insider pilot with issuer relationship and filing history." },
  ],
  institutions: [],
  departments: [
    { type: "department", path: "/departments/department-of-defense", lastmod: "2026-08-01", rationale: "Department pilot with public-company contract exposure." },
    { type: "department", path: "/departments/nasa", lastmod: "2026-08-01", rationale: "Department pilot with public-company contract exposure." },
  ],
  research: [
    { type: "research", path: "/research/nbis-vs-crwv-ai-neoclouds", lastmod: "2026-07-23", rationale: "Published Walnut research brief with original analysis." },
    { type: "research", path: "/research/ai-earnings-dd", lastmod: "2026-07-23", rationale: "Published Walnut research brief with original analysis." },
    { type: "research", path: "/research/mu-dd", lastmod: "2026-07-23", rationale: "Published Walnut research brief with original analysis." },
  ],
  comparisons: [
    { type: "comparison", path: "/compare/NVDA/MU", lastmod: "2026-08-01", rationale: "Approved pilot stock comparison route with useful side-by-side research context." },
  ],
} satisfies Record<string, readonly SeoPilotPage[]>;

export const seoIndexationRules: Record<SeoEntityType, readonly string[]> = {
  ticker: [
    "Valid ticker identity and canonical symbol",
    "Not an unresolved, loading, or unknown shell",
    "Enough useful research context from market data, fundamentals, disclosures, ownership, contracts, confirmation, or research notes",
  ],
  member: [
    "Canonical member slug and valid identity",
    "Meaningful disclosed activity or useful historical/profile context",
    "No empty duplicate profile",
  ],
  insider: [
    "Valid reporting CIK and insider identity",
    "Issuer relationship or filing/activity history",
    "No ambiguous identity-only shell",
  ],
  institution: [
    "Valid normalized CIK and holder name",
    "Reported holdings, filing date, or meaningful holdings context",
    "No locked or unavailable shell indexed as public research",
  ],
  department: [
    "Canonical department slug and valid department identity",
    "Contract summary with linked public-company exposure",
    "No empty department shell",
  ],
  research: [
    "Published brief with original Walnut analysis",
    "Clear title, summary, and visible article body",
    "Draft or unavailable briefs stay noindex",
  ],
  comparison: [
    "Approved editorial pilot pair",
    "Server-rendered comparison context",
    "Canonical pair URL with no arbitrary pair indexing",
  ],
  screener: [
    "Public route must answer a durable research intent, not expose arbitrary filtered result parameters",
    "Indexable screen pages require an editorially named preset, stable canonical URL, and meaningful result set",
    "Private, user-specific, empty, paginated, or query-driven screens stay out of public sitemaps",
  ],
  market: [
    "Market or sector page must describe a real, stable universe with useful public context",
    "Indexable pages require available data, clear methodology, and crawlable links to relevant public ticker pages",
    "Thin sector shells, transient market states, and authenticated-only views stay noindex or excluded",
  ],
};

const seoPilotPathSet = new Set(
  Object.values(seoPilotPages)
    .flat()
    .map((page) => page.path.toLowerCase()),
);

export function isApprovedSeoPilotPath(pathname: string): boolean {
  const normalized = pathname === "/" ? "/" : `/${pathname.replace(/^\/+/, "").replace(/\/+$/, "")}`;
  return seoPilotPathSet.has(normalized.toLowerCase());
}

export function noindexFollowMetadata(title: string, description?: string): Metadata {
  return {
    title,
    description,
    robots: {
      index: false,
      follow: true,
    },
  };
}

export function tickerHasIndexableContent(profile: TickerProfile | null | undefined): boolean {
  const ticker = profile?.ticker;
  if (!ticker?.symbol) return false;
  if (["loading", "unknown", "unresolved"].includes(String(ticker.identity_status ?? "").toLowerCase())) return false;

  const modules = [
    Number(ticker.price_history_points ?? 0) >= 30,
    Boolean(profile?.technical_indicators),
    Boolean(profile?.confirmation_score_bundle),
    Boolean(profile?.why_now),
    Boolean(profile?.signal_freshness),
    (profile?.top_members?.length ?? 0) > 0,
    (profile?.trades?.length ?? 0) > 0,
  ];
  return modules.filter(Boolean).length >= 2;
}

export function memberHasIndexableContent(profile: MemberProfile | null | undefined): boolean {
  if (!profile?.member?.name) return false;
  return (profile.trades?.length ?? 0) > 0 || (profile.top_tickers?.length ?? 0) > 0;
}

export function insiderHasIndexableContent(summary: InsiderSummary | null | undefined): boolean {
  if (!summary?.reporting_cik || !summary.insider_name) return false;
  return (summary.total_trades ?? 0) > 0 || (summary.role_contexts?.length ?? 0) > 0 || Boolean(summary.primary_symbol);
}

export function institutionHasIndexableContent(profile: InstitutionProfileResponse | null | undefined): boolean {
  if (!profile?.cik || profile.locked || profile.availability_status === "pro_locked") return false;
  return Boolean(profile.holder_name) && ((profile.holdings_count ?? 0) > 0 || (profile.top_holdings?.length ?? 0) > 0 || Boolean(profile.latest_filing_date));
}

export function departmentHasIndexableContent(profile: DepartmentProfileResponse | null | undefined): boolean {
  if (!profile?.name) return false;
  const summary = profile.summary;
  return Boolean(summary) && ((summary.contractCount ?? 0) > 0 || (summary.linkedTickerCount ?? 0) > 0);
}

export function sitemapUrlset(appUrl: string, pages: readonly SeoPilotPage[]) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${pages.map((page) => `  <url><loc>${appUrl}${page.path}</loc><lastmod>${page.lastmod}</lastmod></url>`).join("\n")}
</urlset>
`;
}
