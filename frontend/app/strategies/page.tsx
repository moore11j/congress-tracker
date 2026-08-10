import type { Metadata } from "next";
import { StrategiesDirectory } from "@/components/strategies/StrategiesDirectory";
import { getStrategies, getStrategy } from "@/lib/api";
import { optionalPageAuthToken } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "Strategies | Walnut Markets", description: "Published Walnut model strategies with transparent methodology and performance." };

type Props = { searchParams?: Promise<Record<string, string | string[] | undefined>> };
const one = (value: string | string[] | undefined) => typeof value === "string" ? value : "";

export default async function StrategiesPage({ searchParams }: Props) {
  const params = (await searchParams) ?? {};
  const category = one(params.category) || "all";
  const period = one(params.period) || "max";
  const sort = one(params.sort) || "cagr";
  const selectedSlug = one(params.selected);
  const token = await optionalPageAuthToken();
  const data = await getStrategies({ category: category === "all" ? undefined : category, period, sort, authToken: token }).catch(() => null);
  const selected = data?.items.find((strategy) => strategy.slug === selectedSlug)
    ?? data?.items.find((strategy) => strategy.isFeatured)
    ?? data?.items[0];
  const featured = selected ? await getStrategy(selected.slug, { period, equityLimit: 1500, authToken: token }).catch(() => null) : null;
  return <StrategiesDirectory data={data} featured={featured} category={category} period={period} sort={sort} />;
}
