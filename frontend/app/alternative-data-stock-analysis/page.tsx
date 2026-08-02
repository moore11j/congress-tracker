import type { Metadata } from "next";
import { CommercialFeaturePage } from "@/components/landing/CommercialFeaturePage";
import { commercialFeaturePages } from "@/lib/commercialFeaturePages";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";

const page = commercialFeaturePages.alternativeDataStockAnalysis;

export const dynamic = "force-dynamic";
export const revalidate = 300;

export const metadata: Metadata = marketingSeoPageMetadata(page.pathname, {
  title: page.title,
  description: page.description,
});

export default function AlternativeDataStockAnalysisPage() {
  return <CommercialFeaturePage page={page} />;
}
