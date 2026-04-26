"use client";

import { formatDateShort } from "@/lib/format";
import type { NewsItem } from "@/lib/types";

type Props = {
  items: NewsItem[];
  emptyMessage: string;
  status?: string | null;
  message?: string | null;
  showSymbol?: boolean;
  showImage?: boolean;
  compact?: boolean;
};

function metadataLine(item: NewsItem, showSymbol: boolean): string {
  const parts = [item.site ?? "Unknown", formatDateShort(item.published_at ?? null)];
  if (showSymbol && item.symbol) parts.push(item.symbol);
  return parts.filter(Boolean).join(" | ");
}

function marketReadText(marketRead?: string | null): string | null {
  if (marketRead === "bullish") return "Bullish";
  if (marketRead === "bearish") return "Bearish";
  if (marketRead === "neutral") return "Neutral";
  return null;
}

function marketReadClassName(marketRead?: string | null): string {
  if (marketRead === "bullish") return "text-emerald-300/80";
  if (marketRead === "bearish") return "text-rose-300/80";
  return "text-slate-500";
}

function EmptyState({ text }: { text: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-5 text-sm text-slate-400">
      {text}
    </div>
  );
}

export function NewsArticleList({
  items,
  emptyMessage,
  status,
  message,
  showSymbol = true,
  showImage = false,
  compact = false,
}: Props) {
  if (status === "unavailable") return <EmptyState text={message || emptyMessage} />;
  if (items.length === 0) return <EmptyState text={message || emptyMessage} />;

  return (
    <div className="space-y-3">
      {items.map((item) => {
        const marketReadLabel = marketReadText(item.market_read);
        return (
          <article
            key={`${item.url ?? item.title}-${item.published_at ?? ""}`}
            className={`rounded-2xl border border-white/10 bg-slate-950/55 ${compact ? "px-4 py-3" : "px-4 py-4"}`}
          >
            <div className={`grid gap-4 ${showImage && item.image_url ? "md:grid-cols-[120px_minmax(0,1fr)]" : ""}`}>
              {showImage && item.image_url ? (
                <div className="overflow-hidden rounded-xl border border-white/10 bg-slate-900/70">
                  <img src={item.image_url} alt="" className="h-24 w-full object-cover" />
                </div>
              ) : null}
              <div className="min-w-0">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
                    {metadataLine(item, showSymbol)}
                  </p>
                  {marketReadLabel ? (
                    <span className={`text-[11px] font-medium ${marketReadClassName(item.market_read)}`}>
                      Market Read: {marketReadLabel}
                    </span>
                  ) : null}
                </div>
                {item.url ? (
                  <a
                    href={item.url}
                    target="_blank"
                    rel="noreferrer"
                    className={`mt-2 block font-semibold leading-snug text-slate-100 transition hover:text-emerald-200 ${
                      compact ? "text-sm" : "text-base"
                    }`}
                  >
                    {item.title}
                  </a>
                ) : (
                  <p className={`mt-2 font-semibold leading-snug text-slate-100 ${compact ? "text-sm" : "text-base"}`}>
                    {item.title}
                  </p>
                )}
                {item.summary ? (
                  <p className="mt-2 text-sm leading-6 text-slate-400">{item.summary}</p>
                ) : null}
                <div className="mt-3 flex items-center justify-end gap-3">
                  {item.url ? (
                    <a
                      href={item.url}
                      target="_blank"
                      rel="noreferrer"
                      className="text-sm font-semibold text-emerald-200 transition hover:text-emerald-100"
                    >
                      Open article
                    </a>
                  ) : null}
                </div>
              </div>
            </div>
          </article>
        );
      })}
    </div>
  );
}
