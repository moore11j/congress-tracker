import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { StrategyDetail } from "@/components/strategies/StrategyDetail";
import { getStrategy } from "@/lib/api";
import { optionalPageAuthToken } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";
type Props = { params: Promise<{ slug: string }>; searchParams?: Promise<Record<string, string | string[] | undefined>> };
const one = (value: string | string[] | undefined) => typeof value === "string" ? value : "";
const holdingsPageSize = 20;
type PositionView = "current" | "history";

function pageNumber(value: string) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : 1;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params;
  return { title: `${slug.replaceAll("-", " ")} | Walnut Strategies` };
}

export default async function StrategyPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const resolvedSearchParams = await searchParams;
  const period = one(resolvedSearchParams?.period) || "max";
  const holdingsPage = pageNumber(one(resolvedSearchParams?.holdings_page));
  const positionsView: PositionView = one(resolvedSearchParams?.positions) === "history" ? "history" : "current";
  const token = await optionalPageAuthToken();
  const strategy = await getStrategy(slug, {
    period,
    equityLimit: 1500,
    holdingsOffset: (holdingsPage - 1) * holdingsPageSize,
    holdingsLimit: holdingsPageSize,
    authToken: token,
  }).catch(() => null);
  if (!strategy) notFound();
  return <StrategyDetail strategy={strategy} period={period} holdingsPage={holdingsPage} holdingsPageSize={holdingsPageSize} positionsView={positionsView} isAuthenticated={Boolean(token)} />;
}
