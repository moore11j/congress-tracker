import { NextResponse } from "next/server";
import { getSeoSnapshotIndex } from "@/lib/api";
import { sitemapUrlset } from "@/lib/seoQuality";
import { insiderSitemapPages } from "@/lib/insiderSeo";

const APP_URL = "https://app.walnutmarkets.com";

export const dynamic = "force-dynamic";
export const revalidate = 1800;

export async function GET() {
  const pages = await getSeoSnapshotIndex("insider", { source: "InsiderSitemap" })
    .then((response) => insiderSitemapPages(response.items))
    .catch(() => null);
  if (!pages) return new NextResponse("Sitemap temporarily unavailable", {
    status: 503, headers: { "cache-control": "no-store", "retry-after": "300" },
  });

  return new NextResponse(sitemapUrlset(APP_URL, pages), {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
