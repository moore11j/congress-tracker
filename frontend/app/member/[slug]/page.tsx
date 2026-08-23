import Link from "next/link";
import { headers } from "next/headers";
import { redirect } from "next/navigation";
import type { Metadata } from "next";
import { Suspense } from "react";
import { Badge } from "@/components/Badge";
import { ShareLinks } from "@/components/member/ShareLinks";
import { MemberAnalyticsClient } from "@/components/member/MemberAnalyticsClient";
import { AddWatchlistTarget } from "@/components/watchlists/AddWatchlistTarget";
import {
  getMemberAlphaSummary,
  getMemberProfile,
  getMemberProfileBySlug,
  getMemberTrades,
} from "@/lib/api";
import { chamberBadge, partyBadge } from "@/lib/format";
import { nameToSlug } from "@/lib/memberSlug";
import {
  DEFAULT_PORTFOLIO_LOOKBACK_DAYS,
  PORTFOLIO_LOOKBACK_OPTIONS,
  PORTFOLIO_MODE,
  PORTFOLIO_MODE_OPTIONS,
  isPortfolioLookbackDays,
} from "@/lib/portfolioPerformance.mjs";
import { resolveWikipediaHeadshot } from "@/lib/wikipediaHeadshot";
import { optionalPageAuthState, requestMayHavePageAuthState } from "@/lib/serverAuth";
import { WALNUT_APP_URL, appCanonicalUrl, appPageMetadata } from "@/lib/marketingMetadata";
import { conciseSeoDescription, conciseSeoTitle, hasNonCanonicalSearchParams, memberHasIndexableContent, noindexFollowMetadata } from "@/lib/seoQuality";

type Props = {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

const MEMBER_ACTIVITY_TREND_INITIAL_LOOKBACK_DAYS = 730;
const MEMBER_ACTIVITY_TREND_LIMIT = 200;
const MEMBER_NAV_ITEMS = [
  { label: "Overview", href: "#overview" },
  { label: "Trades", href: "#recent-trades" },
  { label: "Performance", href: "#member-performance" },
  { label: "Holdings", href: "#member-holdings" },
  { label: "Activity", href: "#member-activity-trend" },
  { label: "Committees", href: "#member-committees" },
] as const;
const MEMBER_COMMITTEE_ASSIGNMENTS: Record<string, { headline: string; committees: Array<{ name: string; subcommittees: string[] }> }> = {
  K000389: {
    headline: "House Armed Services; Oversight and Government Reform",
    committees: [
      {
        name: "Committee on Armed Services",
        subcommittees: ["Cyber, Information Technologies, and Innovation", "Seapower and Projection Forces"],
      },
      {
        name: "Committee on Oversight and Government Reform",
        subcommittees: ["Cybersecurity, Information Technology, and Government Innovation", "Economic Growth, Energy Policy, and Regulatory Affairs"],
      },
    ],
  },
};

function getSiteUrl() {
  return process.env.NEXT_PUBLIC_SITE_URL ?? WALNUT_APP_URL;
}

function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const v = sp[key];
  return typeof v === "string" ? v : "";
}

function isGoogleLinkerParam(key: string) {
  return key === "_gl" || key === "_ga" || key.startsWith("_ga_");
}

function toQueryString(sp: Record<string, string | string[] | undefined>) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(sp)) {
    if (isGoogleLinkerParam(key)) continue;
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

function getPortfolioModeParam(sp: Record<string, string | string[] | undefined>) {
  const raw = getParam(sp, "portfolio_mode").trim();
  return raw === "theoretical_transaction_date" ? raw : PORTFOLIO_MODE;
}

function buildMemberPath(
  prettySlug: string,
  lbParam: string,
  chartMetric?: "return" | "alpha",
  portfolioLookbackDays?: number,
  portfolioMode?: string,
) {
  const path = `/member/${prettySlug}`;
  const query = new URLSearchParams();
  if (lbParam) query.set("lb", lbParam);
  if (chartMetric && chartMetric !== "return") query.set("am", chartMetric);
  if (portfolioLookbackDays && portfolioLookbackDays !== DEFAULT_PORTFOLIO_LOOKBACK_DAYS) {
    query.set("portfolio_lb", String(portfolioLookbackDays));
  }
  if (portfolioMode && portfolioMode !== PORTFOLIO_MODE) {
    query.set("portfolio_mode", portfolioMode);
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

function buildCommitteeSourceHref(memberId: string, memberName: string, chamber?: string | null) {
  if ((chamber ?? "").toLowerCase() === "house") {
    return `https://clerk.house.gov/Members/${encodeURIComponent(memberId)}`;
  }
  return `https://www.congress.gov/member/${encodeURIComponent(nameToSlug(memberName))}/${encodeURIComponent(memberId)}`;
}

function committeeAssignmentsFor(memberId: string) {
  return MEMBER_COMMITTEE_ASSIGNMENTS[memberId] ?? null;
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

function MemberHeadshotFallback({ memberName }: { memberName: string }) {
  return (
    <div className="grid h-20 w-20 shrink-0 place-items-center rounded-full border border-white/15 bg-slate-950/70 text-2xl font-semibold text-emerald-100 shadow-inner">
      {initialsForName(memberName)}
    </div>
  );
}

async function StreamedMemberHeadshot({
  memberName,
  headshotPromise,
}: {
  memberName: string;
  headshotPromise: ReturnType<typeof resolveWikipediaHeadshot>;
}) {
  const headshot = await headshotPromise;
  if (!headshot) return <MemberHeadshotFallback memberName={memberName} />;
  return (
    <img
      src={headshot.src}
      alt={`${memberName} headshot from Wikipedia`}
      className="h-20 w-20 shrink-0 rounded-full border border-white/15 bg-slate-950/70 object-cover shadow-inner"
      referrerPolicy="no-referrer"
    />
  );
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

export async function generateMetadata({ params, searchParams }: Props): Promise<Metadata> {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const profile = await getMemberProfileBySlug(slug, { include_trades: true, source: "MemberMetadataProfile", stalePageCache: true })
    .catch(() => null);
  const memberName = profileMemberName(profile?.member?.name, slug);
  const canonicalSlug = nameToSlug(memberName);
  const canonicalPath = `/member/${encodeURIComponent(canonicalSlug)}`;
  const fallbackTitle = `${memberName} Stock Trades | Walnut Markets`;
  const fallbackDescription = `Research ${memberName}'s disclosed stock trades, recent activity, traded tickers and public congressional profile in Walnut Markets.`;
  const title = conciseSeoTitle(fallbackTitle, "Congress Member Stock Trades | Walnut Markets");
  const description = conciseSeoDescription(fallbackDescription, "Research disclosed congressional stock trades, recent activity, traded tickers and public member profiles in Walnut Markets.");
  if (!memberHasIndexableContent(profile) || hasNonCanonicalSearchParams(sp)) {
    return {
      ...noindexFollowMetadata(title, description),
      metadataBase: new URL(WALNUT_APP_URL),
      alternates: { canonical: appCanonicalUrl(canonicalPath) },
    };
  }

  return appPageMetadata(canonicalPath, {
    title,
    description,
    alternates: { canonical: appCanonicalUrl(canonicalPath) },
    openGraph: { title, description, type: "profile", url: appCanonicalUrl(canonicalPath) },
  });
}

export default async function MemberPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const lbRaw = getLookbackParam(sp);
  const chartMetric = getChartMetricParam(sp);
  const portfolioLookbackDays = getPortfolioLookbackParam(sp);
  const portfolioMode = getPortfolioModeParam(sp);
  const lb = lbRaw === "90" || lbRaw === "180" ? Number(lbRaw) : 365;
  const requestHeaders = await headers();
  const authState = requestMayHavePageAuthState(requestHeaders)
    ? await optionalPageAuthState()
    : { token: null, hasAuthHint: false, entitlementHint: null };
  const publicStalePageCache = !authState.token && !authState.hasAuthHint;

  const upperSlug = slug.toUpperCase();
  if (upperSlug.startsWith("FMP_")) {
    const legacyData = await getMemberProfile(slug, { source: "MemberProfile" });
    const cleanSlug = nameToSlug(profileMemberName(legacyData.member.name, slug));
    const query = toQueryString(sp);
    redirect(`/member/${cleanSlug}${query ? `?${query}` : ""}`);
  }

  const data = await getMemberProfileBySlug(slug, { include_trades: true, source: "MemberProfile", stalePageCache: publicStalePageCache });
  const memberName = profileMemberName(data.member.name, slug);
  const canonicalSlug = nameToSlug(memberName);
  if (slug !== canonicalSlug) {
    const query = toQueryString(sp);
    redirect(`/member/${canonicalSlug}${query ? `?${query}` : ""}`);
  }

  const sharePath = buildMemberPath(canonicalSlug, lbRaw, chartMetric, portfolioLookbackDays, portfolioMode);
  const shareUrl = new URL(sharePath, getSiteUrl()).toString();
  const canonicalMemberId = data.member.bioguide_id;
  const portfolioLookbackLinks = PORTFOLIO_LOOKBACK_OPTIONS.map((option) => ({
    ...option,
    href: buildMemberPath(canonicalSlug, lbRaw, chartMetric, option.value, portfolioMode),
  }));
  const portfolioModeLinks = PORTFOLIO_MODE_OPTIONS.map((option) => ({
    ...option,
    href: buildMemberPath(canonicalSlug, lbRaw, chartMetric, portfolioLookbackDays, option.value),
  }));
  const chamber = chamberBadge(data.member.chamber);
  const party = partyBadge(data.member.party);
  const districtLabel = data.member.district ? `${data.member.state ?? ""}-${data.member.district}` : data.member.state;
  const activityStatus =
    data.trades.length >= 50 ? "Very Active Trader" : data.trades.length > 0 ? "Active Trader" : "Activity profile";
  const committeeAssignments = committeeAssignmentsFor(canonicalMemberId);
  const headerContext = committeeAssignments?.headline
    ? `Committees: ${committeeAssignments.headline}`
    : "Committee assignments from official profile sources";
  const committeeSourceHref = buildCommitteeSourceHref(canonicalMemberId, memberName, data.member.chamber);
  const actionClassName =
    "inline-flex h-9 min-w-0 items-center justify-center rounded-lg border border-white/10 bg-slate-950/20 px-4 text-xs font-semibold text-slate-100 transition hover:border-white/25 hover:bg-white/[0.04] sm:text-sm";
  const primaryActionClassName =
    "inline-flex h-9 min-w-0 items-center justify-center rounded-lg border border-emerald-400/35 bg-emerald-500/10 px-4 text-xs font-semibold text-emerald-100 transition hover:bg-emerald-500/18 sm:text-sm";
  const headshotPromise = resolveWikipediaHeadshot(memberName, { kind: "member" });
  const [initialAlphaSummaryResult, initialTradesResult, initialTrendTradesResult] = await Promise.allSettled([
    getMemberAlphaSummary(canonicalMemberId, { lookback_days: lb, source: "MemberProfileInitialAlpha", stalePageCache: publicStalePageCache }),
    getMemberTrades(canonicalMemberId, { lookback_days: lb, limit: 100, source: "MemberProfileInitialTrades", stalePageCache: publicStalePageCache }),
    getMemberTrades(canonicalMemberId, {
      lookback_days: MEMBER_ACTIVITY_TREND_INITIAL_LOOKBACK_DAYS,
      limit: MEMBER_ACTIVITY_TREND_LIMIT,
      source: "MemberProfileInitialActivityTrend",
      stalePageCache: publicStalePageCache,
    }),
  ]);
  const initialAlphaSummary =
    initialAlphaSummaryResult.status === "fulfilled" ? initialAlphaSummaryResult.value : undefined;
  const fallbackInitialTrades =
    data.trades.length > 0
      ? { member_id: canonicalMemberId, lookback_days: lb, limit: data.trades.length, items: data.trades }
      : undefined;
  const endpointInitialTrades =
    initialTradesResult.status === "fulfilled" && initialTradesResult.value.items.length > 0
      ? initialTradesResult.value
      : undefined;
  const initialTrades = endpointInitialTrades ?? fallbackInitialTrades;
  const initialTrendTrades =
    initialTrendTradesResult.status === "fulfilled" && initialTrendTradesResult.value.items.length > 0
      ? initialTrendTradesResult.value
      : initialTrades;

  return (
    <div className="space-y-3">
      <section className="relative overflow-hidden rounded-lg border border-white/10 bg-[linear-gradient(135deg,rgba(9,20,35,0.98),rgba(4,10,20,0.98))] px-4 pt-3 shadow-[0_18px_48px_rgba(0,0,0,0.32)] sm:px-5">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <nav aria-label="Breadcrumb" className="flex min-w-0 flex-wrap items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">
            <Link href="/" className="text-slate-400 hover:text-slate-200">Home</Link>
            <span>/</span>
            <Link href="/members" className="text-emerald-300/80 hover:text-emerald-200">Congress</Link>
            <span>/</span>
            <span className="truncate text-slate-300">{memberName}</span>
          </nav>
          <div className="grid w-full grid-cols-2 gap-2 sm:flex sm:w-auto sm:flex-wrap sm:justify-end lg:absolute lg:right-5 lg:top-3">
            <span className="hidden">
              <span className="sm:hidden">Backtest</span>
              <span className="hidden sm:inline">Backtest following this member</span>
              <span className="sm:hidden">Feed</span>
              <span className="hidden sm:inline">Back to feed</span>
            </span>
            <AddWatchlistTarget targetType="member" targetValue={canonicalMemberId} targetLabel={memberName} buttonLabel="Follow Member" className={actionClassName} />
            <ShareLinks canonicalUrl={shareUrl} showCopyButton={false} buttonClassName={actionClassName} />
            <Link href={buildMemberBacktestHref(canonicalMemberId, lb)} prefetch={false} className={primaryActionClassName}>
              Backtest this Member
            </Link>
          </div>
        </div>
        <div className="mt-3 flex min-w-0 gap-4 pb-2 lg:pr-[28rem]">
            <Suspense fallback={<MemberHeadshotFallback memberName={memberName} />}>
              <StreamedMemberHeadshot memberName={memberName} headshotPromise={headshotPromise} />
            </Suspense>
            <div className="min-w-0 pt-0.5">
              <div className="mt-1.5 flex flex-wrap items-center gap-2">
                <h1 className="truncate text-2xl font-semibold leading-tight text-white sm:text-3xl">{memberName} Stock Trades</h1>
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
          {MEMBER_NAV_ITEMS.map((item) => (
            <a
              key={item.label}
              href={item.href}
              className={`shrink-0 border-b-2 pb-2 ${item.label === "Overview" ? "border-amber-300 text-amber-200" : "border-transparent hover:text-white"}`}
            >
              {item.label}
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
          portfolioMode={portfolioMode}
          portfolioModeLinks={portfolioModeLinks}
          initialTopTickers={data.top_tickers}
          initialAlphaSummary={initialAlphaSummary}
          initialTrades={initialTrades}
          initialTrendTrades={initialTrendTrades}
        />
      </div>

      <section id="member-committees" className="scroll-mt-6 rounded-lg border border-white/10 bg-[#0a1726]/95 p-3 shadow-[0_14px_34px_rgba(0,0,0,0.22)]">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-[11px] font-semibold uppercase leading-none tracking-[0.14em] text-slate-200">Committees</h2>
            <p className="mt-2 text-sm text-slate-400">
              Current committee and subcommittee assignments from official congressional profile sources.
            </p>
          </div>
          <a
            href={committeeSourceHref}
            target="_blank"
            rel="noreferrer"
            className="inline-flex h-9 items-center justify-center rounded-lg border border-white/10 bg-slate-950/30 px-3 text-xs font-semibold text-sky-200 transition hover:border-sky-300/40 hover:bg-sky-400/10"
          >
            Open official profile
          </a>
        </div>
        {committeeAssignments ? (
          <div className="mt-3 grid gap-2 md:grid-cols-2">
            {committeeAssignments.committees.map((committee) => (
              <div key={committee.name} className="rounded-lg border border-white/8 bg-white/[0.025] p-3">
                <p className="text-sm font-semibold text-slate-100">{committee.name}</p>
                <div className="mt-2 space-y-1">
                  {committee.subcommittees.map((subcommittee) => (
                    <p key={subcommittee} className="text-xs leading-5 text-slate-400">{subcommittee}</p>
                  ))}
                </div>
              </div>
            ))}
          </div>
        ) : null}
      </section>
    </div>
  );
}
