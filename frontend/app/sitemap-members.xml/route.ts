import { NextResponse } from "next/server";
import { seoPilotPages, sitemapUrlset } from "@/lib/seoQuality";

const APP_URL = "https://app.walnutmarkets.com";

export const dynamic = "force-static";

export function GET() {
  return new NextResponse(sitemapUrlset(APP_URL, seoPilotPages.members), {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
