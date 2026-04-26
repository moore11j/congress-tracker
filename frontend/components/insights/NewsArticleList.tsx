"use client";

import { useState } from "react";
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
  return parts.filter(Boolean).join(" / ");
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

function NewsThumbnail({ src, onError }: { src: string; onError: () => void }) {
  return (
    <div className="overflow-hidden rounded-md border border-white/10 bg-slate-900/70">
      <img
        src={src}
        alt=""
        className="h-11 w-16 object-cover sm:h-12 sm:w-[4.5rem] md:h-14 md:w-20"
        onError={onError}
      />
    </div>
  );
}

function NewsArticleRow({
  item,
  showSymbol,
  showImage,
  compact,
}: {
  item: NewsItem;
  showSymbol: boolean;
  showImage: boolean;
  compact: boolean;
}) {
  const [showThumbnail, setShowThumbnail] = useState(Boolean(showImage && item.image_url));
  const marketReadLabel = marketReadText(item.market_read);
  const compactMediaLayout = compact && showThumbnail;
  const standardMediaLayout = !compact && showThumbnail;

  return (
    <article
      key={`${item.url ?? item.title}-${item.published_at ?? ""}`}
      className={`rounded-2xl border border-white/10 bg-slate-950/55 ${compact ? "px-4 py-3" : "px-4 py-4"}`}
    >
      <div className={`grid gap-3 ${compactMediaLayout ? "grid-cols-[auto_minmax(0,1fr)] items-start" : standardMediaLayout ? "md:grid-cols-[120px_minmax(0,1fr)]" : ""}`}>
        {showThumbnail && item.image_url ? (
          compact ? (
            <NewsThumbnail src={item.image_url} onError={() => setShowThumbnail(false)} />
          ) : (
            <div className="overflow-hidden rounded-xl border border-white/10 bg-slate-900/70">
              <img src={item.image_url} alt="" className="h-24 w-full object-cover" onError={() => setShowThumbnail(false)} />
            </div>
          )
        ) : null}
        <div className="min-w-0">
          {item.url ? (
            <a
              href={item.url}
              target="_blank"
              rel="noreferrer"
              className={`block font-semibold leading-snug text-slate-100 transition hover:text-emerald-200 ${
                compact
                  ? "text-sm [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden"
                  : "text-base"
              }`}
            >
              {item.title}
            </a>
          ) : (
            <p
              className={`font-semibold leading-snug text-slate-100 ${
                compact
                  ? "text-sm [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden"
                  : "text-base"
              }`}
            >
              {item.title}
            </p>
          )}
          <div className={`flex flex-wrap items-center justify-between gap-2 ${compact ? "mt-1.5" : "mt-2"}`}>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
              {metadataLine(item, showSymbol)}
            </p>
            {marketReadLabel ? (
              <span className={`text-[11px] font-medium ${marketReadClassName(item.market_read)}`}>
                Market Read: {marketReadLabel}
              </span>
            ) : null}
          </div>
          {item.summary ? (
            <p
              className={`mt-2 text-sm text-slate-400 ${
                compact
                  ? "leading-5 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden"
                  : "leading-6"
              }`}
            >
              {item.summary}
            </p>
          ) : null}
          <div className={`flex items-center justify-end gap-3 ${compact ? "mt-2" : "mt-3"}`}>
            {item.url ? (
              <a
                href={item.url}
                target="_blank"
                rel="noreferrer"
                className={`font-semibold text-emerald-200 transition hover:text-emerald-100 ${compact ? "text-xs" : "text-sm"}`}
              >
                {compact ? "Open" : "Open article"}
              </a>
            ) : null}
          </div>
        </div>
      </div>
    </article>
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
      {items.map((item) => (
        <NewsArticleRow
          key={`${item.url ?? item.title}-${item.published_at ?? ""}`}
          item={item}
          showSymbol={showSymbol}
          showImage={showImage}
          compact={compact}
        />
      ))}
    </div>
  );
}
