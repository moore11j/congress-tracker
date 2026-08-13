import { NextResponse } from "next/server";
import { seoPilotPages, sitemapUrlset } from "@/lib/seoQuality";

const MARKETING_URL = "https://walnutmarkets.com";

export const dynamic = "force-static";

export function GET() {
  return new NextResponse(sitemapUrlset(MARKETING_URL, seoPilotPages.research), {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
