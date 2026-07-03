import Link from "next/link";
import { redirect } from "next/navigation";
import type { Metadata } from "next";
import { Badge } from "@/components/Badge";
import { ShareLinks } from "@/components/member/ShareLinks";
import { MemberAnalyticsClient } from "@/components/member/MemberAnalyticsClient";
import { getMemberProfile, getMemberProfileBySlug } from "@/lib/api";
import { cardClassName, ghostButtonClassName, pillClassName, subtlePrimaryButtonClassName } from "@/lib/styles";
import { chamberBadge, partyBadge } from "@/lib/format";
import { isBioguideId, nameToSlug } from "@/lib/memberSlug";
import {
  DEFAULT_PORTFOLIO_LOOKBACK_DAYS,
  PORTFOLIO_LOOKBACK_OPTIONS,
  isPortfolioLookbackDays,
} from "@/lib/portfolioPerformance.mjs";

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
    benchmark: "^GSPC",
  });
  return `/backtesting?${query.toString()}`;
}

async function resolveMetadataMemberSlug(slug: string) {
  if (!isBioguideId(slug)) return slug;
  try {
    const data = await getMemberProfileBySlug(slug, { include_trades: false });
    return nameToSlug(data.member.name);
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
  const title = `${fallbackName || "Member"} — Member Profile`;

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
    const cleanSlug = nameToSlug(legacyData.member.name);
    const query = toQueryString(sp);
    redirect(`/member/${cleanSlug}${query ? `?${query}` : ""}`);
  }

  const data = await getMemberProfileBySlug(slug, { include_trades: false, source: "MemberProfile" });
  const canonicalSlug = nameToSlug(data.member.name);
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

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">
            Member profile
          </p>
          <h1 className="text-3xl font-semibold text-white">{data.member.name}</h1>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
            <Badge tone={party.tone}>{party.label}</Badge>
            <Badge tone={chamber.tone}>{chamber.label}</Badge>
            <span className={pillClassName}>{(data.member.state ?? "").split("-")[0] || "—"}</span>
          </div>
        </div>
        <div className="grid w-full grid-cols-2 gap-2 sm:flex sm:w-auto sm:flex-wrap sm:items-center sm:justify-end">
          <Link href={buildMemberBacktestHref(canonicalMemberId, lb)} prefetch={false} className={`${subtlePrimaryButtonClassName} min-w-0 whitespace-nowrap px-3 text-xs sm:px-4 sm:text-sm`}>
            <span className="sm:hidden">Backtest</span>
            <span className="hidden sm:inline">Backtest following this member</span>
          </Link>
          <ShareLinks canonicalUrl={canonicalUrl} />
          <Link href="/?mode=all" className={`${ghostButtonClassName} min-w-0 whitespace-nowrap px-3 py-2 text-xs sm:px-4 sm:text-sm`}>
            <span className="sm:hidden">Feed</span>
            <span className="hidden sm:inline">Back to feed</span>
          </Link>
        </div>
      </div>

      <section className={`${cardClassName} p-4 sm:p-6`}>
        <h2 className="text-lg font-semibold text-white">Analytics</h2>
        <p className="mt-2 max-w-2xl text-sm text-white/45">
          The profile shell renders first. Portfolio performance, trade outcomes, top tickers, and recent trades load below without blocking the page.
        </p>
      </section>

      <MemberAnalyticsClient
        memberId={canonicalMemberId}
        memberName={data.member.name}
        lookbackDays={lb}
        portfolioLookbackDays={portfolioLookbackDays}
        portfolioLookbackLinks={portfolioLookbackLinks}
        initialTopTickers={data.top_tickers}
      />
    </div>
  );
}
