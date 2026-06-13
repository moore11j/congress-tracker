import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { MarketSnapshotCategoryClient } from "@/components/insights/MarketSnapshotCategoryClient";
import { MARKET_SNAPSHOT_CATEGORIES, marketSnapshotCategory } from "@/lib/marketSnapshot";

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
      title: "Insights | Walnut Market Terminal",
    };
  }

  return {
    title: `${category.title} | Walnut Market Terminal`,
    description: category.description,
    openGraph: {
      title: `${category.title} | Walnut Market Terminal`,
      description: category.description,
    },
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
