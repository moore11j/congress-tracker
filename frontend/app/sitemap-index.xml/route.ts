import { NextResponse } from "next/server";

const APP_URL = "https://app.walnutmarkets.com";
const SITEMAPS = [
  "/sitemap-tickers.xml",
  "/sitemap-members.xml",
  "/sitemap-insiders.xml",
  "/sitemap-institutions.xml",
  "/sitemap-departments.xml",
  "/sitemap-research.xml",
  "/sitemap-comparisons.xml",
] as const;

export const dynamic = "force-static";

export function GET() {
  const body = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${SITEMAPS.map((path) => `  <sitemap><loc>${APP_URL}${path}</loc></sitemap>`).join("\n")}
</sitemapindex>
`;
  return new NextResponse(body, {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
