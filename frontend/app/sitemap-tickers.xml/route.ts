import { NextResponse } from "next/server";
import { seoPilotPages, sitemapUrlset } from "@/lib/seoQuality";

const APP_URL = "https://app.walnutmarkets.com";

export const dynamic = "force-static";

export function GET() {
  return xmlResponse(sitemapUrlset(APP_URL, seoPilotPages.tickers));
}

function xmlResponse(body: string) {
  return new NextResponse(body, {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
