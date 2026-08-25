import { NextResponse } from "next/server";
import { getPublicInstitutionIndex } from "@/lib/api";
import { sitemapUrlset } from "@/lib/seoQuality";

const APP_URL = "https://app.walnutmarkets.com";

export const dynamic = "force-dynamic";
export const revalidate = 1800;

export async function GET() {
  const pages = await getPublicInstitutionIndex({ source: "InstitutionSitemap", stalePageCache: true })
    .then((response) => response.items
      .filter((item) => item.cik && item.holder_name && item.latest_filing_date)
      .map((item) => ({
        type: "institution" as const,
        path: `/institution/${encodeURIComponent(item.cik)}`,
        lastmod: item.latest_filing_date!.slice(0, 10),
        rationale: "Public institutional profile identity with a reported 13F filing.",
      })))
    .catch(() => []);

  return new NextResponse(sitemapUrlset(APP_URL, pages), {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
