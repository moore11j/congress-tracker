import { cache } from "react";
import { unstable_cache } from "next/cache";
import { getDepartments, getPublicInstitutionIndex, getSeoSnapshotIndex } from "@/lib/api";
import { departmentHref } from "@/lib/departments";
import { insiderSitemapPages } from "@/lib/insiderSeo";
import { nameToSlug } from "@/lib/memberSlug";
import { sitemapCorrections } from "@/lib/sitemapCorrections";

export const directoryCategories = {
  stocks: { title: "Stocks", description: "Browse stock profiles with company context, market data, public disclosures, and related research." },
  members: { title: "Congress members", description: "Find current and former members with tracked stock disclosures. Filing dates can lag transaction dates; reported trades are not recommendations." },
  insiders: { title: "Corporate insiders", description: "Explore executives, directors, and reporting owners with SEC Form 4 activity. Open a profile to review company relationships and dated transactions." },
  institutions: { title: "Institutional investors", description: "Browse investment managers with reported 13F filings. Holdings are periodic disclosures rather than a live view of an investor's portfolio." },
  departments: { title: "Government agencies", description: "Explore federal agencies with tracked awards linked to public companies. Compare recipients, award dates, and reported amounts in each profile." },
} as const;
export type DirectoryCategory = keyof typeof directoryCategories;
export type DirectoryEntry = { path: string; name: string; date?: string | null };
export const DIRECTORY_PAGE_SIZE = 100;

export function isDirectoryCategory(value: string): value is DirectoryCategory {
  return Object.prototype.hasOwnProperty.call(directoryCategories, value);
}

export function directoryPath(category: DirectoryCategory, page = 1) {
  return `/explore/${category}${page > 1 ? `/${page}` : ""}`;
}

export function normalizeDirectoryEntries(entries: DirectoryEntry[]): DirectoryEntry[] {
  const result = new Map<string, DirectoryEntry>();
  for (const entry of entries) {
    const original = new URL(entry.path, "https://app.walnutmarkets.com");
    const correction = sitemapCorrections[original.href];
    if (correction?.exclude || !entry.name.trim()) continue;
    const url = new URL(correction?.canonical ?? original.href);
    if (url.origin !== "https://app.walnutmarkets.com" || url.search || url.hash) continue;
    if (!/^\/(ticker|member|insider|institution|departments)\/[^/]+$/.test(url.pathname)) continue;
    if (sitemapCorrections[url.href]?.exclude) continue;
    result.set(url.pathname, { ...entry, path: url.pathname });
  }
  return [...result.values()].sort((a, b) => a.name.localeCompare(b.name, "en") || a.path.localeCompare(b.path, "en"));
}

async function loadEntries(category: DirectoryCategory): Promise<DirectoryEntry[]> {
  if (category === "institutions") {
    const data = await getPublicInstitutionIndex({ source: "PublicDirectory", stalePageCache: true });
    return normalizeDirectoryEntries(data.items.filter(row => row.cik && row.holder_name && row.latest_filing_date)
      .map(row => ({ path: `/institution/${encodeURIComponent(row.cik)}`, name: row.holder_name, date: row.latest_filing_date })));
  }
  if (category === "departments") {
    const data = await getDepartments();
    return normalizeDirectoryEntries(data.items.filter(row => row.slug && row.contractCount > 0 && row.linkedTickerCount > 0)
      .map(row => ({ path: departmentHref(row.name)!, name: row.name, date: row.latestAwardDate })));
  }
  const type = category === "stocks" ? "ticker" : category === "members" ? "member" : "insider";
  const data = await getSeoSnapshotIndex(type, { source: "PublicDirectory", limit: 50_000 });
  if (type === "insider") {
    const names = new Map(data.items.map(row => [row.entity_key, String(row.payload.insider_name ?? "")]));
    return normalizeDirectoryEntries(insiderSitemapPages(data.items).map(row => ({
      path: row.path, name: names.get(row.path.match(/(\d{10})$/)?.[1] ?? "") ?? "", date: row.lastmod,
    })));
  }
  return normalizeDirectoryEntries(data.items.filter(row => row.indexable).map(row => {
    const name = String(row.payload.member_name ?? "").trim();
    const slug = row.canonical_path.match(/^\/member\/([^/?#]+)/)?.[1] ?? row.entity_key;
    return type === "member"
      ? { path: `/member/${encodeURIComponent(nameToSlug(name || decodeURIComponent(slug).replace(/[_-]+/g, " ")))}`, name: name || slug.replace(/[_-]+/g, " "), date: row.data_as_of }
      : { path: row.canonical_path, name: String(row.payload.company_name ?? row.payload.name ?? row.entity_key) === row.entity_key ? row.entity_key : `${row.entity_key} — ${row.payload.company_name ?? row.payload.name}`, date: row.data_as_of };
  }));
}

// Cache only public directory data. A failed upstream read throws and is never
// stored as a successful empty directory. No profile-by-profile API fan-out.
export const getDirectoryEntries = cache(unstable_cache(loadEntries, ["public-directory-v1"], { revalidate: 1800 }));
