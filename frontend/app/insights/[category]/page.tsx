import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { MarketSnapshotCategoryClient } from "@/components/insights/MarketSnapshotCategoryClient";
import { MARKET_SNAPSHOT_CATEGORIES, marketSnapshotCategory } from "@/lib/marketSnapshot";
import { WALNUT_APP_URL, appCanonicalUrl } from "@/lib/marketingMetadata";

type Props = {
  params: Promise<{ category: string }>;
};

export function generateStaticParams() {
  return MARKET_SNAPSHOT_CATEGORIES.map((category) => ({ category: category.slug }));
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { category: slug } = await params;
  const category = marketSnapshotCategory(slug);
  if (!category) {
    return {
      metadataBase: new URL(WALNUT_APP_URL),
      title: "Stock Market Insights | Walnut Markets",
      robots: { index: false, follow: true },
    };
  }

  const title = `${category.title} Stock Market Insights | Walnut Markets`;
  return {
    metadataBase: new URL(WALNUT_APP_URL),
    title,
    description: category.description,
    alternates: { canonical: appCanonicalUrl(`/insights/${category.slug}`) },
    openGraph: {
      title,
      description: category.description,
      url: appCanonicalUrl(`/insights/${category.slug}`),
    },
    twitter: { card: "summary", title, description: category.description },
  };
}

export default async function MarketSnapshotCategoryPage({ params }: Props) {
  const { category: slug } = await params;
  const category = marketSnapshotCategory(slug);
  if (!category) notFound();

  return (
    <div className="w-full max-w-[calc(100vw-2rem)] sm:max-w-[calc(100vw-3rem)] lg:max-w-none">
      <MarketSnapshotCategoryClient category={category} />
    </div>
  );
}
