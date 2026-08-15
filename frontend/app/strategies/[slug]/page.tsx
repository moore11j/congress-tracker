import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { StrategyDetail } from "@/components/strategies/StrategyDetail";
import { getStrategy } from "@/lib/api";
import { optionalPageAuthToken } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";
type Props = { params: Promise<{ slug: string }>; searchParams?: Promise<Record<string, string | string[] | undefined>> };
const one = (value: string | string[] | undefined) => typeof value === "string" ? value : "";

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params;
  return { title: `${slug.replaceAll("-", " ")} | Walnut Strategies` };
}

export default async function StrategyPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const resolvedSearchParams = await searchParams;
  const period = one(resolvedSearchParams?.period) || "max";
  const positions = one(resolvedSearchParams?.positions) === "history" ? "history" : "holdings";
  const holdingsPage = Math.max(1, Number(one(resolvedSearchParams?.holdings_page)) || 1);
  const historyPage = Math.max(1, Number(one(resolvedSearchParams?.history_page)) || 1);
  const token = await optionalPageAuthToken();
  const strategy = await getStrategy(slug, {
    period,
    equityLimit: 1500,
    holdingsOffset: (holdingsPage - 1) * 20,
    holdingsLimit: 20,
    historyOffset: (historyPage - 1) * 20,
    historyLimit: 20,
    authToken: token,
  }).catch(() => null);
  if (!strategy) notFound();
  return <StrategyDetail strategy={strategy} period={period} positionsMode={positions} holdingsPage={holdingsPage} historyPage={historyPage} />;
}
