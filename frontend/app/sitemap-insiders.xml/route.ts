import { NextResponse } from "next/server";
import { getSeoSnapshotIndex } from "@/lib/api";
import { sitemapUrlset } from "@/lib/seoQuality";

const APP_URL = "https://app.walnutmarkets.com";

export const dynamic = "force-dynamic";
export const revalidate = 1800;

export async function GET() {
  const pages = await getSeoSnapshotIndex("insider", { source: "InsiderSitemap" })
    .then((response) => response.items.map((item) => ({
      type: "insider" as const,
      path: item.canonical_path,
      lastmod: (item.data_as_of ?? item.updated_at ?? new Date().toISOString()).slice(0, 10),
      rationale: "Indexable precomputed insider SEO snapshot.",
    })))
    .catch(() => []);
  return new NextResponse(sitemapUrlset(APP_URL, pages), {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
