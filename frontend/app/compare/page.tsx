import type { Metadata } from "next";
import { ComparisonHubPage } from "@/components/landing/ComparisonPages";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";

export const dynamic = "force-static";
export const revalidate = false;

export const metadata: Metadata = marketingSeoPageMetadata("/compare", {
  title: "Compare Stock Research Platforms | Walnut Markets",
  description:
    "Compare Walnut Markets with StockAnalysis, Insider Screener, Quiver Quantitative, Unusual Whales, Finviz, Capitol Trades, and TrendSpider.",
});

export default function CompareHubRoute() {
  return <ComparisonHubPage />;
}
