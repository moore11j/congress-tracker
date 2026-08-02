import type { Metadata } from "next";
import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { ComparisonHubPage } from "@/components/landing/ComparisonPages";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";

export const metadata: Metadata = marketingSeoPageMetadata("/compare", {
  title: "Compare Stock Research Platforms | Walnut Markets",
  description:
    "Compare Walnut Markets with StockAnalysis, Insider Screener, Quiver Quantitative, Unusual Whales, Finviz, Capitol Trades, and TrendSpider.",
});

export default async function CompareHubRoute() {
  const requestHeaders = await headers();
  if (requestHeaders.get("x-walnut-public-landing") !== "1") redirect("/compare/_/_");
  return <ComparisonHubPage />;
}
