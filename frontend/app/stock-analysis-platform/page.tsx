import type { Metadata } from "next";
import { CommercialFeaturePage } from "@/components/landing/CommercialFeaturePage";
import { commercialFeaturePages } from "@/lib/commercialFeaturePages";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";
import { HomepageResearchExample } from "@/components/landing/HomepageResearchExample";
import { publicHomepageResearch } from "@/lib/homepagePreview";
import { API_BASE } from "@/lib/api";

const page = commercialFeaturePages.stockAnalysisPlatform;

export const dynamic = "force-dynamic";
export const revalidate = 300;

export const metadata: Metadata = marketingSeoPageMetadata(page.pathname, {
  title: page.title,
  description: page.description,
});

export default async function StockAnalysisPlatformPage() {
  // Anonymous, bounded and cached: never project a signed-in paid response.
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 4500);
  const example = await fetch(`${API_BASE}/api/tickers/MSFT/context-bundle`, {next: {revalidate: 300}, signal: controller.signal})
    .then(async response => response.ok ? publicHomepageResearch({rank: 0, symbol: "MSFT", companyName: "Microsoft", drivers: [], updatedAt: null}, await response.json()) : null)
    .catch(() => null).finally(() => clearTimeout(timer));
  return <CommercialFeaturePage page={page} example={<HomepageResearchExample example={example} appUrl="https://app.walnutmarkets.com" rankingAt={null} ranked={false} />} />;
}
