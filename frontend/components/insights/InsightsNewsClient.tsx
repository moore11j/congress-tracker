"use client";

import { useEffect, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { formatDateShort } from "@/lib/format";
import { getInsightsNews } from "@/lib/api";
import type { InsightsNewsResponse, NewsItem } from "@/lib/types";

type Props = {
  page: number;
  limit: number;
};

function NewsSkeleton() {
  return (
    <div className="space-y-0 divide-y divide-white/10">
      {Array.from({ length: 5 }).map((_, index) => (
        <div key={index} className="grid gap-4 py-4 md:grid-cols-[7rem_minmax(0,1fr)_8rem_7rem_minmax(10rem,0.7fr)]">
          <div className="h-16 w-full animate-pulse rounded-lg bg-white/10" />
          <div className="space-y-2">
            <div className="h-4 w-4/5 animate-pulse rounded bg-white/10" />
            <div className="h-3 w-full animate-pulse rounded bg-white/10" />
          </div>
          <div className="h-7 animate-pulse rounded bg-white/10" />
          <div className="h-7 animate-pulse rounded bg-white/10" />
          <div className="h-12 animate-pulse rounded bg-white/10" />
        </div>
      ))}
    </div>
  );
}

const categoryFilters = ["all", "macro", "earnings", "policy", "sector", "commodities"] as const;
const impactFilters = ["all", "bullish", "bearish", "neutral"] as const;

type CategoryFilter = (typeof categoryFilters)[number];
type ImpactFilter = (typeof impactFilters)[number];

function categoryLabel(value: CategoryFilter): string {
  if (value === "all") return "All";
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function itemText(item: NewsItem): string {
  return `${item.title} ${item.summary ?? ""} ${item.symbol ?? ""}`.toLowerCase();
}

function itemCategory(item: NewsItem): Exclude<CategoryFilter, "all"> {
  const text = itemText(item);
  if (/(oil|crude|gold|silver|copper|commodity|commodities|wti|brent)/.test(text)) return "commodities";
  if (/(senate|house|congress|tariff|policy|regulation|antitrust|fed|treasury|white house)/.test(text)) return "policy";
  if (/(earnings|revenue|eps|margin|guidance|profit|quarter)/.test(text)) return "earnings";
  if (/(inflation|cpi|rates|yield|dollar|jobs|gdp|macro|unemployment)/.test(text)) return "macro";
  return "sector";
}

function marketReadLabel(value?: string | null): string {
  if (value === "bullish") return "Bullish";
  if (value === "bearish") return "Bearish";
  if (value === "neutral") return "Neutral";
  return "Mixed";
}

function marketReadClassName(value?: string | null): string {
  if (value === "bullish") return "text-emerald-300";
  if (value === "bearish") return "text-rose-300";
  return "text-amber-300";
}

function tickerChips(item: NewsItem): string[] {
  return item.symbol ? [item.symbol] : [];
}

function freshnessText(value?: string | null): string {
  if (!value) return "Recent";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return formatDateShort(value);
  const hours = Math.max(0, Math.round((Date.now() - date.getTime()) / 36e5));
  if (hours < 1) return "Just now";
  if (hours < 24) return `${hours}h ago`;
  return formatDateShort(value);
}

function walnutTake(item: NewsItem): string {
  const category = itemCategory(item);
  const read = item.market_read;
  if (read === "bullish") {
    if (category === "earnings") return "Positive operating signal. Check whether guidance and margins support follow-through.";
    if (category === "commodities") return "Supportive for exposed producers, but demand and inventory data still matter.";
    return "Positive read for affected tickers. Confirm breadth and valuation before treating it as durable.";
  }
  if (read === "bearish") {
    if (category === "macro") return "Negative macro impulse. Watch rates, inflation, and earnings sensitivity.";
    if (category === "commodities") return "Pressure signal for commodity exposure. Demand confirmation remains the key check.";
    return "Negative read for affected tickers. Watch whether the impact is isolated or spreading.";
  }
  if (item.summary) return item.summary;
  return "Current impact is unclear from available article data.";
}

function EmptyState({ text }: { text: string }) {
  return <div className="rounded-lg border border-white/10 bg-slate-950/45 px-4 py-5 text-sm text-slate-400">{text}</div>;
}

function HeadlineRow({ item }: { item: NewsItem }) {
  const chips = tickerChips(item);
  const [imageVisible, setImageVisible] = useState(Boolean(item.image_url));

  return (
    <article className="grid gap-3 border-b border-white/10 py-3 last:border-b-0 md:grid-cols-[7.5rem_minmax(0,1fr)_9rem_7rem_minmax(12rem,0.75fr)] md:items-center">
      <a href={item.url} target="_blank" rel="noreferrer" className="block overflow-hidden rounded-lg border border-white/10 bg-slate-950/70">
        {imageVisible && item.image_url ? (
          <img src={item.image_url} alt="" className="h-20 w-full object-cover md:h-16" onError={() => setImageVisible(false)} />
        ) : (
          <div className="grid h-20 w-full place-items-center bg-[radial-gradient(circle_at_30%_20%,rgba(45,212,191,0.22),transparent_28%),linear-gradient(135deg,rgba(15,23,42,1),rgba(8,47,73,0.64))] text-xs font-semibold uppercase tracking-[0.18em] text-slate-500 md:h-16">
            News
          </div>
        )}
      </a>

      <div className="min-w-0">
        <a href={item.url} target="_blank" rel="noreferrer" className="text-sm font-semibold leading-5 text-white transition hover:text-emerald-100">
          {item.title}
        </a>
        <p className="mt-1 text-sm leading-5 text-slate-400 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden">
          {item.summary || item.site || "Summary unavailable."}
        </p>
      </div>

      <div className="flex min-w-0 flex-wrap gap-2">
        {chips.length > 0 ? (
          chips.map((chip) => (
            <span key={chip} className="rounded-md border border-white/10 bg-white/[0.04] px-2.5 py-1 text-xs font-semibold text-slate-200">
              {chip}
            </span>
          ))
        ) : (
          <span className="text-xs text-slate-500">No ticker</span>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-2 md:block md:text-right">
        <span className={`text-xs font-semibold ${marketReadClassName(item.market_read)}`}>
          {marketReadLabel(item.market_read)}
        </span>
        <p className="text-xs text-slate-500 md:mt-2">{freshnessText(item.published_at)}</p>
      </div>

      <div className="min-w-0 md:border-l md:border-white/10 md:pl-4">
        <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Walnut take</p>
        <p className="mt-1 text-xs leading-5 text-slate-400 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:3] overflow-hidden">{walnutTake(item)}</p>
      </div>
    </article>
  );
}

export function InsightsNewsClient({ page, limit }: Props) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const [response, setResponse] = useState<InsightsNewsResponse | null>(null);
  const [category, setCategory] = useState<CategoryFilter>("all");
  const [impact, setImpact] = useState<ImpactFilter>("all");
  const pendingScrollPageRef = useRef<number | null>(null);

  useEffect(() => {
    if (pendingScrollPageRef.current !== page) return;
    pendingScrollPageRef.current = null;

    const firstFrame = window.requestAnimationFrame(() => {
      document.getElementById("market-headlines")?.scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    });

    return () => window.cancelAnimationFrame(firstFrame);
  }, [page]);

  useEffect(() => {
    const controller = new AbortController();
    getInsightsNews({ page, limit, signal: controller.signal })
      .then((payload) => {
        if (controller.signal.aborted) return;
        const hasItems = payload.items.length > 0;
        const unavailable = payload.status === "unavailable" || payload.message?.toLowerCase().includes("market data is temporarily unavailable");
        setResponse(
          !hasItems && unavailable
            ? {
                ...payload,
                status: "warming",
                message: "Market headlines are warming. Check back shortly.",
                has_next: false,
              }
            : payload,
        );
      })
      .catch(() => {
        if (!controller.signal.aborted) {
          setResponse({
            items: [],
            status: "warming",
            message: "Market headlines are warming. Check back shortly.",
            page,
            limit,
            has_next: false,
          });
        }
      });
    return () => controller.abort();
  }, [limit, page]);

  function goToPage(nextPage: number) {
    const params = new URLSearchParams(searchParams.toString());
    if (nextPage <= 0) {
      params.delete("page");
    } else {
      params.set("page", String(nextPage));
    }
    const query = params.toString();
    pendingScrollPageRef.current = nextPage;
    router.push(`${pathname}${query ? `?${query}` : ""}`, { scroll: false });
  }

  const visibleItems = (response?.items ?? []).filter((item) => {
    const categoryMatch = category === "all" || itemCategory(item) === category;
    const impactMatch = impact === "all" || item.market_read === impact;
    return categoryMatch && impactMatch;
  });

  return (
    <section id="market-headlines" className="scroll-mt-6 rounded-lg border border-white/10 bg-slate-950/55 p-4 shadow-[0_18px_60px_-42px_rgba(16,185,129,0.55)]">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 pb-3">
        <div className="flex min-w-0 items-center gap-2">
          <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-100">Market Headlines</h2>
          <span className="text-xs text-slate-500">Page {(response?.page ?? page) + 1}</span>
        </div>
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <div className="flex flex-wrap gap-1">
            {categoryFilters.map((value) => (
              <button
                key={value}
                type="button"
                onClick={() => setCategory(value)}
                className={`rounded-md px-2.5 py-1 text-xs font-semibold transition ${category === value ? "bg-emerald-300/15 text-emerald-100" : "text-slate-400 hover:text-white"}`}
              >
                {categoryLabel(value)}
              </button>
            ))}
          </div>
          <select
            aria-label="Impact"
            value={impact}
            onChange={(event) => setImpact(event.target.value as ImpactFilter)}
            className="h-8 rounded-md border border-white/10 bg-slate-950/70 px-2 text-xs font-semibold text-slate-200 outline-none transition focus:border-emerald-300/50"
          >
            <option value="all">Impact: All</option>
            <option value="bullish">Bullish</option>
            <option value="bearish">Bearish</option>
            <option value="neutral">Neutral</option>
          </select>
        </div>
      </div>

      {response ? (
        response.status === "warming" ? (
          <div className="mt-4"><EmptyState text={response.message ?? "Market headlines are warming. Check back shortly."} /></div>
        ) : response.status === "unavailable" ? (
          <div className="mt-4"><EmptyState text={response.message ?? "Market headlines are temporarily unavailable."} /></div>
        ) : visibleItems.length > 0 ? (
          <div className="divide-y-0">{visibleItems.slice(0, 6).map((item) => <HeadlineRow key={`${item.url ?? item.title}-${item.published_at ?? ""}`} item={item} />)}</div>
        ) : (
          <div className="mt-4"><EmptyState text="No recent market news matched these filters." /></div>
        )
      ) : (
        <div className="mt-1"><NewsSkeleton /></div>
      )}

      <div className="mt-4 flex items-center justify-center gap-3 border-t border-white/10 pt-3">
        <button
          type="button"
          onClick={() => goToPage(Math.max(page - 1, 0))}
          disabled={page === 0}
          className={`rounded-lg border px-3 py-2 text-sm font-semibold ${
            page === 0
              ? "cursor-not-allowed border-white/10 bg-slate-950/40 text-slate-600"
              : "border-white/10 bg-slate-950/60 text-slate-200 hover:text-white"
          }`}
        >
          Previous
        </button>
        <button
          type="button"
          onClick={() => goToPage(page + 1)}
          disabled={!response?.has_next}
          className={`rounded-lg border px-3 py-2 text-sm font-semibold ${
            response?.has_next
              ? "border-emerald-300/30 bg-emerald-400/10 text-emerald-100 hover:bg-emerald-400/15"
              : "cursor-not-allowed border-white/10 bg-slate-950/40 text-slate-600"
          }`}
        >
          Next
        </button>
      </div>
    </section>
  );
}
