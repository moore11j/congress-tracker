import { NextResponse } from "next/server";
import { getSeoSnapshotIndex } from "@/lib/api";
import { seoPilotPages, sitemapUrlset } from "@/lib/seoQuality";

const APP_URL = "https://app.walnutmarkets.com";
const DEFAULT_LASTMOD = "2026-08-01";

export const dynamic = "force-dynamic";
export const revalidate = 1800;

export async function GET() {
  const pages = await getSeoSnapshotIndex("department")
    .then((response) => response.items
      .filter((item) => item.indexable && item.canonical_path)
      .map((item) => ({
        type: "department" as const,
        path: item.canonical_path,
        lastmod: safeDepartmentLastmod(item.data_as_of),
        rationale: "Public department profile with mapped government-contract exposure.",
      })))
    .catch(() => seoPilotPages.departments);

  return new NextResponse(sitemapUrlset(APP_URL, pages), {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}

function safeDepartmentLastmod(value: string | null | undefined) {
  const candidate = String(value ?? "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(candidate)) return DEFAULT_LASTMOD;
  const today = new Date().toISOString().slice(0, 10);
  return candidate <= today ? candidate : DEFAULT_LASTMOD;
}
