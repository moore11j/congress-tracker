import { NextResponse } from "next/server";
import { getSeoSnapshotIndex, type SeoEntitySnapshot } from "@/lib/api";
import { nameToSlug } from "@/lib/memberSlug";
import { sitemapUrlset } from "@/lib/seoQuality";

const APP_URL = "https://app.walnutmarkets.com";

export const dynamic = "force-dynamic";
export const revalidate = 1800;

export async function GET() {
  const pages = await getSeoSnapshotIndex("member", { source: "MemberSitemap" })
    .then((response) => response.items.map((item) => ({
      type: "member" as const,
      path: memberSitemapPath(item),
      lastmod: (item.data_as_of ?? item.updated_at ?? new Date().toISOString()).slice(0, 10),
      rationale: "Indexable Congress member guest profile page.",
    })))
    .catch(() => []);
  return new NextResponse(sitemapUrlset(APP_URL, pages), {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}

function memberSitemapPath(item: SeoEntitySnapshot) {
  const memberName = typeof item.payload?.member_name === "string" ? item.payload.member_name.trim() : "";
  return memberName ? `/member/${encodeURIComponent(nameToSlug(memberName))}` : item.canonical_path;
}
