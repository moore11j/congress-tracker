import { cache } from "react";
import { ApiError, getInsiderSummary, getInsiderTrades, type InsiderSummary, type SeoEntitySnapshot } from "@/lib/api";
import { getInsiderDisplayName, insiderSlug, reportingCikFromInsiderSlug } from "@/lib/insider";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { hasNonCanonicalSearchParams, insiderHasIndexableContent } from "@/lib/seoQuality";
import { withServerTimeout } from "@/lib/serverTimeout";

export type InsiderPublicProfile = {
  status: "ready" | "unavailable" | "missing";
  summary: InsiderSummary | null;
  trades: Awaited<ReturnType<typeof getInsiderTrades>> | null;
};

// Share exactly the public data used in the rendered profile with metadata.
// No request cookies, session tokens, entitlement hints or private endpoints.
export const loadPublicInsiderProfile = cache(async (
  reportingCik: string, lookbackDays: number, issuer: string | undefined, page: number, stalePageCache: boolean,
): Promise<InsiderPublicProfile> => {
  const [summary, trades] = await Promise.allSettled([
    withServerTimeout(getInsiderSummary(reportingCik, lookbackDays, issuer, { source: "InsiderPublicProfile", stalePageCache }), "Insider summary"),
    withServerTimeout(getInsiderTrades(reportingCik, lookbackDays, 20, issuer, { page, source: "InsiderPublicTrades", stalePageCache }), "Insider trades"),
  ]);
  if (summary.status === "rejected") {
    return { status: summary.reason instanceof ApiError && summary.reason.status === 404 ? "missing" : "unavailable", summary: null, trades: null };
  }
  if (summary.value.reporting_cik !== reportingCik) return { status: "unavailable", summary: null, trades: null };
  const availability = summary.value as InsiderSummary & { locked?: boolean; status?: string; availability_status?: string };
  if (availability.locked || /loading|unavailable|unresolved|error|locked|auth_required/i.test(`${availability.status ?? ""} ${availability.availability_status ?? ""}`)) {
    return { status: "unavailable", summary: null, trades: null };
  }
  return {
    status: "ready",
    summary: summary.value,
    trades: trades.status === "fulfilled" && trades.value.reporting_cik === reportingCik ? trades.value : null,
  };
});

export function resolvedInsiderName(summary: InsiderSummary | null): string | null {
  const name = getInsiderDisplayName(summary?.insider_name);
  return name && !/^(unknown(?: insider)?|insider|loading|unavailable|n\/a)$/i.test(name) ? name : null;
}

export function insiderCanonicalSlug(slug: string, profile: InsiderPublicProfile): string {
  const cik = reportingCikFromInsiderSlug(slug);
  const name = profile.status === "ready" && profile.summary?.reporting_cik === cik ? resolvedInsiderName(profile.summary) : null;
  return (name && insiderSlug(name, cik)) || slug.replace(/\/+$/, "");
}

export function insiderProfileMetadata(slug: string, sp: Record<string, string | string[] | undefined>, profile: InsiderPublicProfile) {
  const cik = reportingCikFromInsiderSlug(slug);
  const name = profile.status === "ready" && profile.summary?.reporting_cik === cik ? resolvedInsiderName(profile.summary) : null;
  const substantive = Boolean(name && insiderHasIndexableContent(profile.summary, profile.trades?.items));
  const canonicalPath = `/insider/${encodeURIComponent(insiderCanonicalSlug(slug, profile))}`;
  return appPageMetadata(canonicalPath, {
    title: name ? `${name} Insider Trades & SEC Form 4 Activity | Walnut` : "Insider Profile Unavailable | Walnut",
    description: substantive
      ? `Track ${name}'s disclosed insider transactions, SEC Form 4 activity and reported company relationships on Walnut Markets.`
      : name ? `View ${name}'s public insider profile on Walnut Markets. Substantive transaction history is currently unavailable.`
        : "Public insider profile data is currently unavailable. Search for another insider or try again later on Walnut Markets.",
    robots: { index: substantive && !hasNonCanonicalSearchParams(sp), follow: true },
    openGraph: { type: "profile" },
  });
}

// Existing materialized sitemap candidates only; no per-request fan-out over
// thousands of profiles. Require public identity and dated filing detail.
export function insiderSitemapPages(items: readonly SeoEntitySnapshot[]) {
  const pages = new Map<string, { type: "insider"; path: string; lastmod: string; rationale: string }>();
  for (const item of items) {
    if (!item.indexable || item.entity_type !== "insider" || !/^\d{10}$/.test(item.entity_key) || /^0+$/.test(item.entity_key)) continue;
    const payload = item.payload;
    if (payload.reporting_cik !== item.entity_key || typeof payload.insider_name !== "string") continue;
    if (payload.locked || /loading|unavailable|unresolved|error|locked|auth_required/i.test(`${payload.status ?? ""} ${payload.availability_status ?? ""}`)) continue;
    const name = getInsiderDisplayName(payload.insider_name);
    if (!name || /^(insider|unknown(?: insider)?|loading|unavailable|n\/a)$/i.test(name)) continue;
    const activity = payload.recent_activity;
    if (!Array.isArray(activity) || !activity.some((row) =>
      row && typeof row.symbol === "string" && /^[A-Z][A-Z0-9.-]*$/.test(row.symbol)
      && typeof row.transaction_type === "string" && row.transaction_type.trim()
      && typeof (row.filing_date ?? row.transaction_date) === "string"
      && Number.isFinite(Date.parse(String(row.filing_date ?? row.transaction_date))),
    )) continue;
    const stamp = item.data_as_of ?? item.updated_at;
    if (!stamp || !Number.isFinite(Date.parse(stamp))) continue;
    const slug = insiderSlug(name, item.entity_key);
    if (!slug) continue;
    const path = `/insider/${encodeURIComponent(slug)}`;
    const lastmod = new Date(stamp).toISOString().slice(0, 10);
    if (!pages.has(path) || pages.get(path)!.lastmod < lastmod) {
      pages.set(path, { type: "insider", path, lastmod, rationale: "Public insider identity with dated disclosure activity." });
    }
  }
  return [...pages.values()];
}
