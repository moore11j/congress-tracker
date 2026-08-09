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
  const period = one((await searchParams)?.period) || "max";
  const token = await optionalPageAuthToken();
  const strategy = await getStrategy(slug, { period, equityLimit: 1500, authToken: token }).catch(() => null);
  if (!strategy) notFound();
  return <StrategyDetail strategy={strategy} period={period} />;
}
