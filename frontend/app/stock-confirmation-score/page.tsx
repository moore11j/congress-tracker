import type { Metadata } from "next";
import { ResearchSeoPage } from "@/components/landing/ResearchSeoPage";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";
import { seoLandingPages } from "@/lib/seoLandingPages";

export const dynamic = "force-dynamic";

const page = seoLandingPages.stockConfirmationScore;

export const metadata: Metadata = marketingSeoPageMetadata(page.pathname, {
  title: page.title,
  description: page.description,
});

export default function StockConfirmationScorePage() {
  return <ResearchSeoPage page={page} />;
}
