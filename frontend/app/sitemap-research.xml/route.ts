import { NextResponse } from "next/server";
import { getGeneratedResearchBriefCards } from "@/lib/api";
import { getPublishedResearchBriefs } from "@/lib/researchBriefs";
import { sitemapUrlset, type SeoPilotPage } from "@/lib/seoQuality";

const MARKETING_URL = "https://walnutmarkets.com";

export const dynamic = "force-dynamic";
export const revalidate = 1800;

export async function GET() {
  const staticBriefs = getPublishedResearchBriefs();
  const generatedBriefs = await getGeneratedResearchBriefCards()
    .then((response) => response.items)
    .catch(() => []);
  const pagesByPath = new Map<string, SeoPilotPage>();

  for (const brief of [...staticBriefs, ...generatedBriefs]) {
    const path = researchSitemapPath(brief.route, brief.slug);
    if (pagesByPath.has(path)) continue;
    pagesByPath.set(path, {
      type: "research",
      path,
      lastmod: sitemapLastmod(brief.publishedAt),
      rationale: "Published Walnut research brief.",
    });
  }

  return new NextResponse(sitemapUrlset(MARKETING_URL, [...pagesByPath.values()]), {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}

function researchSitemapPath(route: string | null | undefined, slug: string) {
  const candidate = (route ?? "").trim();
  if (/^\/research\/[a-z0-9]+(?:-[a-z0-9]+)*$/i.test(candidate)) return candidate;
  return `/research/${encodeURIComponent(slug)}`;
}

function sitemapLastmod(value: string | null | undefined) {
  return value?.match(/^\d{4}-\d{2}-\d{2}/)?.[0] ?? new Date().toISOString().slice(0, 10);
}
