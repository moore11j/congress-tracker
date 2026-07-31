import { NextResponse } from "next/server";

const APP_URL = "https://app.walnutmarkets.com";
const TICKERS = ["NVDA", "AAPL", "MSFT", "TSLA", "PLTR", "LMT"] as const;

export const dynamic = "force-static";

export function GET() {
  return xmlResponse(urlset(TICKERS.map((symbol) => `/ticker/${symbol}`)));
}

function urlset(paths: readonly string[]) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${paths.map((path) => `  <url><loc>${APP_URL}${path}</loc></url>`).join("\n")}
</urlset>
`;
}

function xmlResponse(body: string) {
  return new NextResponse(body, {
    headers: {
      "content-type": "application/xml; charset=utf-8",
      "cache-control": "public, max-age=3600, s-maxage=86400",
    },
  });
}
