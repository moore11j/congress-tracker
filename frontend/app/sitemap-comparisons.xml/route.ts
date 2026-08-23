import { NextResponse } from "next/server";
import { comparisonCheckedOn, comparisonPageList, comparisonPath } from "@/lib/comparisonPages";
import { sitemapUrlset } from "@/lib/seoQuality";

const MARKETING_URL = "https://walnutmarkets.com";

export const dynamic = "force-static";

export function GET() {
  const pages = comparisonPageList.map((page) => ({
    type: "comparison" as const,
    path: comparisonPath(page.slug),
    lastmod: comparisonCheckedOn,
    rationale: "Editorial competitor comparison page on the marketing site.",
  }));

  return new NextResponse(sitemapUrlset(MARKETING_URL, pages), {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
