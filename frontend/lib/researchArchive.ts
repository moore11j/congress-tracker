import { cache } from "react";
import { getGeneratedResearchBriefCards } from "@/lib/api";
import { getPublishedResearchBriefs, type ResearchBriefCard } from "@/lib/researchBriefs";

export const BRIEFS_PER_PAGE = 6;

export function researchArchiveHref(page: number): string {
  return `https://walnutmarkets.com/research${page > 1 ? `?page=${page}` : ""}`;
}

export function researchArchivePage(value: string | string[] | undefined): number | null {
  if (value === undefined) return 1;
  if (typeof value !== "string" || !/^[1-9]\d*$/.test(value)) return null;
  const page = Number(value);
  return Number.isSafeInteger(page) ? page : null;
}

export function mergeResearchBriefs(staticBriefs: ResearchBriefCard[], generated: ResearchBriefCard[]): ResearchBriefCard[] {
  const bySlug = new Map<string, ResearchBriefCard>();
  for (const brief of [...staticBriefs, ...generated]) {
    if (!/^\/research\/[^/?#]+$/.test(brief.route) || bySlug.has(brief.slug)) continue;
    bySlug.set(brief.slug, brief);
  }
  return [...bySlug.values()].sort((a, b) =>
    (Date.parse(b.publishedAt) || 0) - (Date.parse(a.publishedAt) || 0) || a.slug.localeCompare(b.slug),
  );
}

// Public card metadata only. Never pass session credentials or full articles into
// the shared archive. Propagate outages instead of publishing an incomplete list.
export const loadResearchArchive = cache(async (): Promise<ResearchBriefCard[]> => {
  const { items } = await getGeneratedResearchBriefCards();
  return mergeResearchBriefs(getPublishedResearchBriefs(), items.map((item) => ({
    ...item,
    judgment: item.judgment === "neutral" ? "mixed" : item.judgment as ResearchBriefCard["judgment"],
    premium: Boolean(item.premium),
  })));
});
