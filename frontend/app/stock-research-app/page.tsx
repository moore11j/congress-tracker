import type { Metadata } from "next";
import { ResearchSeoPage } from "@/components/landing/ResearchSeoPage";
import { seoLandingPages } from "@/lib/seoLandingPages";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";

const page = seoLandingPages.stockResearchApp;

export const metadata: Metadata = marketingSeoPageMetadata(page.pathname, {
  title: page.title,
  description: page.description,
});

export default function StockResearchAppPage() {
  return <ResearchSeoPage page={page} />;
}
