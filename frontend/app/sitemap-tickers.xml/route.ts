import { NextResponse } from "next/server";
import { getSeoSnapshotIndex } from "@/lib/api";
import { sitemapUrlset } from "@/lib/seoQuality";

const APP_URL = "https://app.walnutmarkets.com";

export const dynamic = "force-dynamic";
export const revalidate = 1800;

export async function GET() {
  const pages = await getSeoSnapshotIndex("ticker", { source: "TickerSitemap", limit: 50_000 })
    .then((response) => response.items.map((item) => ({
      type: "ticker" as const,
      path: item.canonical_path,
      lastmod: (item.data_as_of ?? item.updated_at ?? new Date().toISOString()).slice(0, 10),
      rationale: "Indexable cached ticker profile page.",
    })))
    .catch(() => null);
  if (!pages) return new NextResponse("Sitemap temporarily unavailable", {
    status: 503, headers: { "cache-control": "no-store", "retry-after": "300" },
  });

  return xmlResponse(sitemapUrlset(APP_URL, pages));
}

function xmlResponse(body: string) {
  return new NextResponse(body, {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
