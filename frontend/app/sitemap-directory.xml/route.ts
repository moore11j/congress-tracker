import { NextResponse } from "next/server";
import { directoryCategories, directoryPath, DIRECTORY_PAGE_SIZE, getDirectoryEntries, type DirectoryCategory } from "@/lib/publicDirectory";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const groups = await Promise.all((Object.keys(directoryCategories) as DirectoryCategory[]).map(async category => {
      const entries = await getDirectoryEntries(category);
      return Array.from({ length: Math.ceil(entries.length / DIRECTORY_PAGE_SIZE) }, (_, index) => directoryPath(category, index + 1));
    }));
    const paths = ["/explore", ...groups.flat()];
    return new NextResponse(`<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${paths.map(path => `  <url><loc>https://app.walnutmarkets.com${path}</loc></url>`).join("\n")}\n</urlset>`, {
      headers: { "content-type": "application/xml; charset=utf-8", "cache-control": "public, max-age=300, s-maxage=1800" },
    });
  } catch {
    return new NextResponse("Directory sitemap temporarily unavailable", { status: 503, headers: { "cache-control": "no-store", "retry-after": "300" } });
  }
}
