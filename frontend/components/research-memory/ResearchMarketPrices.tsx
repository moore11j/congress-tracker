"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { getInsightsOverview } from "@/lib/api";
import type { InsightsQuoteItem } from "@/lib/types";
import { researchCheckTime } from "@/lib/researchEvidence";
import { researchQuoteStatus, type ResearchMarketAsset } from "@/lib/researchMarketPrices";

export function ResearchMarketPrices({ assets }: { assets: ResearchMarketAsset[] }) {
  const [quotes, setQuotes] = useState<InsightsQuoteItem[] | null>(null);
  const [failed, setFailed] = useState(false);
  const [retry, setRetry] = useState(0);
  const assetKey = assets.map((asset) => asset.symbol).join(",");
  useEffect(() => {
    let active = true;
    setQuotes(null); setFailed(false);
    getInsightsOverview().then((overview) => {
      if (active) setQuotes([...overview.commodities, ...overview.crypto]);
    }).catch(() => { if (active) { setQuotes([]); setFailed(true); } });
    return () => { active = false; };
  }, [assetKey, retry]);

  return <>{assets.map((asset) => {
    const quote = quotes?.find((item) => item.symbol === asset.symbol && item.group === asset.group);
    const status = researchQuoteStatus(quote);
    const change = status === "available" && typeof quote?.change_percent === "number" && Number.isFinite(quote.change_percent) ? quote.change_percent : null;
    const price = status !== "unavailable" && quote?.price != null ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: quote.price < 1 ? 4 : 2 }).format(quote.price) : null;
    return <article key={asset.symbol} className="min-w-0 rounded-lg border border-emerald-300/20 bg-slate-950/30 px-3 py-2.5" aria-label={`${asset.name} price context`}>
      <div className="flex flex-wrap items-center justify-between gap-2"><h3 className="text-xs font-semibold text-slate-200">{asset.name} price</h3><span className={`text-[11px] ${status === "available" ? "text-emerald-300" : "text-amber-200"}`}>{quotes === null ? "Loading…" : status === "stale" ? "Update overdue" : status === "available" ? "Latest cached quote" : "Price unavailable"}</span></div>
      <div className="mt-2 flex flex-wrap items-baseline gap-x-3 gap-y-1"><p className="text-xl font-semibold tabular-nums text-white">{quotes === null ? "—" : price ?? "—"}{price ? <span className="ml-1 text-[10px] font-normal text-slate-400">USD</span> : null}</p>{change !== null ? <p className={`text-xs tabular-nums ${change >= 0 ? "text-emerald-300" : "text-rose-300"}`}>{change > 0 ? "+" : ""}{change.toFixed(2)}% <span className="text-slate-500">24h</span></p> : null}</div>
      <p className="mt-1 text-[11px] text-slate-500">{status === "stale" ? "Last available · " : ""}{quote?.as_of && researchCheckTime(quote.as_of) ? researchCheckTime(quote.as_of) : "Quote time unavailable"}</p>
      {asset.expectations.map((expectation) => <p key={expectation} className="mt-2 break-words text-xs leading-5 text-slate-300">Your assumption: {expectation}</p>)}
      <p className="mt-2 text-[11px] leading-4 text-slate-500">Price context · Trend assessment requires manual review.</p>
      <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1"><Link href={`/insights/${asset.group}`} className="text-[11px] font-semibold text-emerald-200 hover:text-emerald-100">View in Insights →</Link>{failed || (quotes !== null && status !== "available") ? <button type="button" onClick={() => setRetry((value) => value + 1)} className="text-[11px] font-semibold text-emerald-200">Retry prices</button> : null}</div>
    </article>;
  })}</>;
}
