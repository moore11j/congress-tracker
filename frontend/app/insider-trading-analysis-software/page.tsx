import type { Metadata } from "next";
import { CommercialFeaturePage } from "@/components/landing/CommercialFeaturePage";
import { commercialFeaturePages } from "@/lib/commercialFeaturePages";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";

const page = commercialFeaturePages.insiderTradingAnalysisSoftware;

export const dynamic = "force-static";
export const revalidate = false;

export const metadata: Metadata = marketingSeoPageMetadata(page.pathname, {
  title: page.title,
  description: page.description,
});

export default function InsiderTradingAnalysisSoftwarePage() {
  return <CommercialFeaturePage page={page} />;
}
