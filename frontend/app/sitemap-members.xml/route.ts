import { NextResponse } from "next/server";
import { getSeoSnapshotIndex, type SeoEntitySnapshot } from "@/lib/api";
import { nameToSlug } from "@/lib/memberSlug";
import { sitemapUrlset, type SeoPilotPage } from "@/lib/seoQuality";

const APP_URL = "https://app.walnutmarkets.com";

export const dynamic = "force-dynamic";
export const revalidate = 1800;

export async function GET() {
  const pages = await getSeoSnapshotIndex("member", { source: "MemberSitemap" })
    .then((response) => {
      const pagesByPath = new Map<string, SeoPilotPage>();
      for (const item of response.items) {
        const path = memberSitemapPath(item);
        if (pagesByPath.has(path)) continue;
        pagesByPath.set(path, {
          type: "member",
          path,
          lastmod: (item.data_as_of ?? item.updated_at ?? new Date().toISOString()).slice(0, 10),
          rationale: "Indexable Congress member guest profile page.",
        });
      }
      return [...pagesByPath.values()];
    })
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
  const rawSlug = item.canonical_path.match(/^\/member\/([^/?#]+)/)?.[1] ?? item.entity_key;
  const canonicalSlug = memberName
    ? nameToSlug(memberName)
    : nameToSlug(decodeMemberSitemapSlug(rawSlug).replace(/[_-]+/g, " "));
  return `/member/${encodeURIComponent(canonicalSlug)}`;
}

function decodeMemberSitemapSlug(slug: string) {
  try {
    return decodeURIComponent(slug);
  } catch {
    return slug;
  }
}
