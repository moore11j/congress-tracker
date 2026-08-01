import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { CompetitorComparisonPageView } from "@/components/landing/ComparisonPages";
import { comparisonPageForSlug, comparisonPageList, comparisonPath } from "@/lib/comparisonPages";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";

type PageProps = {
  params: Promise<{ left: string }>;
};

export const dynamic = "force-static";
export const revalidate = false;

export function generateStaticParams() {
  return comparisonPageList.map((page) => ({ left: page.slug }));
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
  const { left } = await params;
  const page = comparisonPageForSlug(left);
  if (!page) {
    return {
      title: "Comparison Not Found | Walnut Markets",
      robots: { index: false, follow: true },
    };
  }
  return marketingSeoPageMetadata(comparisonPath(page.slug), {
    title: page.title,
    description: page.description,
  });
}

export default async function CompetitorComparisonRoute({ params }: PageProps) {
  const { left } = await params;
  const page = comparisonPageForSlug(left);
  if (!page) notFound();
  return <CompetitorComparisonPageView page={page} />;
}
