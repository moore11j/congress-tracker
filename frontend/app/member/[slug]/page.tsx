import Link from "next/link";
import { redirect } from "next/navigation";
import type { Metadata } from "next";
import { Badge } from "@/components/Badge";
import { ShareLinks } from "@/components/member/ShareLinks";
import { MemberAnalyticsClient } from "@/components/member/MemberAnalyticsClient";
import {
  getMemberAlphaSummary,
  getMemberProfile,
  getMemberProfileBySlug,
  getMemberTrades,
} from "@/lib/api";
import { chamberBadge, partyBadge } from "@/lib/format";
import { isBioguideId, nameToSlug } from "@/lib/memberSlug";
import {
  DEFAULT_PORTFOLIO_LOOKBACK_DAYS,
  PORTFOLIO_LOOKBACK_OPTIONS,
  isPortfolioLookbackDays,
} from "@/lib/portfolioPerformance.mjs";
import { resolveWikipediaHeadshot } from "@/lib/wikipediaHeadshot";

type Props = {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

const DEFAULT_SITE_URL = "https://congress-tracker-two.vercel.app";

function getSiteUrl() {
  return process.env.NEXT_PUBLIC_SITE_URL ?? DEFAULT_SITE_URL;
}

function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const v = sp[key];
  return typeof v === "string" ? v : "";
}

function toQueryString(sp: Record<string, string | string[] | undefined>) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(sp)) {
    if (typeof value === "string") {
      query.set(key, value);
      continue;
    }
    if (Array.isArray(value)) {
      value.forEach((entry) => query.append(key, entry));
    }
  }
  return query.toString();
}

function getLookbackParam(sp: Record<string, string | string[] | undefined>) {
  const lb = getParam(sp, "lb");
  if (["90", "180", "365"].includes(lb)) return lb;
  return "";
}

function getChartMetricParam(sp: Record<string, string | string[] | undefined>) {
  const metric = getParam(sp, "am");
  if (metric === "alpha" || metric === "return") return metric;
  return "return";
}

function getPortfolioLookbackParam(sp: Record<string, string | string[] | undefined>) {
  const raw = Number(getParam(sp, "portfolio_lb"));
  return isPortfolioLookbackDays(raw) ? raw : DEFAULT_PORTFOLIO_LOOKBACK_DAYS;
}

function buildMemberPath(
  prettySlug: string,
  lbParam: string,
  chartMetric?: "return" | "alpha",
  portfolioLookbackDays?: number,
) {
  const path = `/member/${prettySlug}`;
  const query = new URLSearchParams();
  if (lbParam) query.set("lb", lbParam);
  if (chartMetric && chartMetric !== "return") query.set("am", chartMetric);
  if (portfolioLookbackDays && portfolioLookbackDays !== DEFAULT_PORTFOLIO_LOOKBACK_DAYS) {
    query.set("portfolio_lb", String(portfolioLookbackDays));
  }
  const qs = query.toString();
  return qs ? `${path}?${qs}` : path;
}

function buildMemberBacktestHref(memberId: string, lookbackDays: number) {
  const query = new URLSearchParams({
    strategy: "congress",
    scope: "member",
    member_id: memberId,
    lookback_days: String(lookbackDays),
    hold_days: "90",
    benchmark: "SPY",
  });
  return `/backtesting?${query.toString()}`;
}

function memberNameFallback(slug: string) {
  return slug.replace(/[_-]+/g, " ").trim() || "Member";
}

function profileMemberName(name: string | null | undefined, slug: string) {
  return (name ?? "").trim() || memberNameFallback(slug);
}

function initialsForName(name: string) {
  const parts = name.split(/\s+/).filter(Boolean);
  const first = parts[0]?.[0] ?? "M";
  const last = parts.length > 1 ? parts[parts.length - 1]?.[0] : parts[0]?.[1];
  return `${first}${last ?? ""}`.toUpperCase();
}

function VerifiedBadge() {
  return (
    <span className="grid h-4 w-4 place-items-center rounded-full bg-sky-500 text-white shadow-[0_0_12px_rgba(14,165,233,0.35)]">
      <svg viewBox="0 0 12 12" aria-hidden="true" className="h-2.5 w-2.5" fill="none">
        <path d="M3 6.2 5 8l4-4.5" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" />
      </svg>
    </span>
  );
}

async function resolveMetadataMemberSlug(slug: string) {
  if (!isBioguideId(slug)) return slug;
  try {
    const data = await getMemberProfileBySlug(slug, { include_trades: false });
    return nameToSlug(profileMemberName(data.member.name, slug));
  } catch {
    return slug;
  }
}

export async function generateMetadata({ params, searchParams }: Props): Promise<Metadata> {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const lbParam = getLookbackParam(sp);
  const siteUrl = getSiteUrl();
  const fallbackName = slug.replace(/-/g, " ");
  const chartMetric = getChartMetricParam(sp);
  const portfolioLookbackDays = getPortfolioLookbackParam(sp);
  const prettySlug = await resolveMetadataMemberSlug(slug);
  const canonicalPath = buildMemberPath(prettySlug, lbParam, chartMetric, portfolioLookbackDays);
  const canonicalUrl = new URL(canonicalPath, siteUrl).toString();
  const title = `${fallbackName || "Member"} - Member Profile`;

  return {
    metadataBase: new URL(siteUrl),
    title,
    alternates: { canonical: canonicalPath },
    openGraph: { title, type: "website", url: canonicalUrl },
    twitter: { card: "summary", title },
  };
}

export default async function MemberPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const lbRaw = getLookbackParam(sp);
  const chartMetric = getChartMetricParam(sp);
  const portfolioLookbackDays = getPortfolioLookbackParam(sp);
  const lb = lbRaw === "90" || lbRaw === "180" ? Number(lbRaw) : 365;

  const upperSlug = slug.toUpperCase();
  if (upperSlug.startsWith("FMP_")) {
    const legacyData = await getMemberProfile(slug, { source: "MemberProfile" });
    const cleanSlug = nameToSlug(profileMemberName(legacyData.member.name, slug));
    const query = toQueryString(sp);
    redirect(`/member/${cleanSlug}${query ? `?${query}` : ""}`);
  }

  const data = await getMemberProfileBySlug(slug, { include_trades: true, source: "MemberProfile" });
  const memberName = profileMemberName(data.member.name, slug);
  const canonicalSlug = nameToSlug(memberName);
  if (slug !== canonicalSlug) {
    const query = toQueryString(sp);
    redirect(`/member/${canonicalSlug}${query ? `?${query}` : ""}`);
  }

  const canonicalPath = buildMemberPath(canonicalSlug, lbRaw, chartMetric, portfolioLookbackDays);
  const canonicalUrl = new URL(canonicalPath, getSiteUrl()).toString();
  const canonicalMemberId = data.member.bioguide_id;
  const portfolioLookbackLinks = PORTFOLIO_LOOKBACK_OPTIONS.map((option) => ({
    ...option,
    href: buildMemberPath(canonicalSlug, lbRaw, chartMetric, option.value),
  }));
  const chamber = chamberBadge(data.member.chamber);
  const party = partyBadge(data.member.party);
  const districtLabel = data.member.district ? `${data.member.state ?? ""}-${data.member.district}` : data.member.state;
  const activityStatus =
    data.trades.length >= 50 ? "Very Active Trader" : data.trades.length > 0 ? "Active Trader" : "Activity profile";
  const headerContext = data.top_tickers[0]
    ? `Most active disclosed ticker: ${data.top_tickers[0].symbol}`
    : "Disclosure history from public filings";
  const actionClassName =
    "inline-flex h-9 min-w-0 items-center justify-center rounded-lg border border-white/10 bg-slate-950/20 px-4 text-xs font-semibold text-slate-100 transition hover:border-white/25 hover:bg-white/[0.04] sm:text-sm";
  const primaryActionClassName =
    "inline-flex h-9 min-w-0 items-center justify-center rounded-lg border border-emerald-400/35 bg-emerald-500/10 px-4 text-xs font-semibold text-emerald-100 transition hover:bg-emerald-500/18 sm:text-sm";
  const [initialAlphaSummaryResult, initialTradesResult, headshotResult] = await Promise.allSettled([
    getMemberAlphaSummary(canonicalMemberId, { lookback_days: lb, source: "MemberProfileInitialAlpha" }),
    getMemberTrades(canonicalMemberId, { lookback_days: lb, limit: 100, source: "MemberProfileInitialTrades" }),
    resolveWikipediaHeadshot(memberName, { kind: "member" }),
  ]);
  const initialAlphaSummary =
    initialAlphaSummaryResult.status === "fulfilled" ? initialAlphaSummaryResult.value : undefined;
  const headshot = headshotResult.status === "fulfilled" ? headshotResult.value : null;
  const fallbackInitialTrades =
    data.trades.length > 0
      ? { member_id: canonicalMemberId, lookback_days: lb, limit: data.trades.length, items: data.trades }
      : undefined;
  const endpointInitialTrades =
    initialTradesResult.status === "fulfilled" && initialTradesResult.value.items.length > 0
      ? initialTradesResult.value
      : undefined;
  const initialTrades = endpointInitialTrades ?? fallbackInitialTrades;

  return (
    <div className="space-y-3">
      <section className="relative overflow-hidden rounded-lg border border-white/10 bg-[linear-gradient(135deg,rgba(9,20,35,0.98),rgba(4,10,20,0.98))] px-4 pt-3 shadow-[0_18px_48px_rgba(0,0,0,0.32)] sm:px-5">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <Link href="/?mode=congress" className="text-[10px] font-semibold uppercase tracking-[0.22em] text-emerald-300/80 hover:text-emerald-200">
            Back to feed
          </Link>
          <div className="grid w-full grid-cols-2 gap-2 sm:flex sm:w-auto sm:flex-wrap sm:justify-end lg:absolute lg:right-5 lg:top-3">
            <span className="hidden">
              <span className="sm:hidden">Backtest</span>
              <span className="hidden sm:inline">Backtest following this member</span>
              <span className="sm:hidden">Feed</span>
              <span className="hidden sm:inline">Back to feed</span>
            </span>
            <Link href="/?mode=congress" className={actionClassName}>
              Follow Member
            </Link>
            <ShareLinks canonicalUrl={canonicalUrl} showCopyButton={false} buttonClassName={actionClassName} />
            <Link href={buildMemberBacktestHref(canonicalMemberId, lb)} prefetch={false} className={primaryActionClassName}>
              Create Research
            </Link>
          </div>
        </div>
        <div className="mt-3 flex min-w-0 gap-4 pb-2 lg:pr-[28rem]">
            {headshot ? (
              <img
                src={headshot.src}
                alt={`${memberName} headshot from Wikipedia`}
                className="h-20 w-20 shrink-0 rounded-full border border-white/15 bg-slate-950/70 object-cover shadow-inner"
                referrerPolicy="no-referrer"
              />
            ) : (
              <div className="grid h-20 w-20 shrink-0 place-items-center rounded-full border border-white/15 bg-slate-950/70 text-2xl font-semibold text-emerald-100 shadow-inner">
                {initialsForName(memberName)}
              </div>
            )}
            <div className="min-w-0 pt-0.5">
              <div className="mt-1.5 flex flex-wrap items-center gap-2">
                <h1 className="truncate text-2xl font-semibold leading-tight text-white sm:text-3xl">{memberName}</h1>
                <VerifiedBadge />
              </div>
              <p className="mt-2 text-sm text-slate-300">
                {chamber.label !== "-" ? `U.S. ${chamber.label}` : "U.S. Congress"} - {party.label !== "-" ? party.label : "Party unavailable"}
                {districtLabel ? ` - ${districtLabel}` : ""}
              </p>
              <p className="mt-1 text-sm text-slate-300">{headerContext}</p>
              <div className="mt-2 flex flex-wrap gap-1.5 text-[10px] text-slate-400">
                <Badge tone="pos">{activityStatus}</Badge>
              </div>
            </div>
        </div>
        <nav className="flex gap-7 overflow-x-auto border-t border-white/10 pt-2 text-sm font-medium text-slate-400">
          {["Overview", "Trades", "Performance", "Holdings", "Activity", "Committees", "About"].map((item) => (
            <a
              key={item}
              href={item === "Overview" ? "#overview" : item === "Trades" ? "#recent-trades" : "#member-performance"}
              className={`shrink-0 border-b-2 pb-2 ${item === "Overview" ? "border-amber-300 text-amber-200" : "border-transparent hover:text-white"}`}
            >
              {item}
            </a>
          ))}
        </nav>
      </section>

      <div id="overview">
        <MemberAnalyticsClient
          memberId={canonicalMemberId}
          memberName={memberName}
          lookbackDays={lb}
          portfolioLookbackDays={portfolioLookbackDays}
          portfolioLookbackLinks={portfolioLookbackLinks}
          initialTopTickers={data.top_tickers}
          initialAlphaSummary={initialAlphaSummary}
          initialTrades={initialTrades}
        />
      </div>
    </div>
  );
}
