"use client";

import { formatDateShort } from "@/lib/format";
import type { NewsItem } from "@/lib/types";

type Props = {
  items: NewsItem[];
  emptyMessage: string;
  status?: string | null;
  message?: string | null;
  showSymbol?: boolean;
};

function metadataLine(item: NewsItem, showSymbol: boolean): string {
  const parts = [item.site, formatDateShort(item.published_at ?? null)];
  if (showSymbol && item.symbol) parts.push(item.symbol);
  return parts.filter(Boolean).join(" · ");
}

export function NewsArticleList({
  items,
  emptyMessage,
  status,
  message,
  showSymbol = true,
}: Props) {
  if (status === "unavailable" || status === "disabled") {
    return (
      <div className="rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-5 text-sm text-slate-400">
        {message || emptyMessage}
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-5 text-sm text-slate-400">
        {message || emptyMessage}
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {items.map((item) => (
        <article key={item.url} className="rounded-2xl border border-white/10 bg-slate-950/55 px-4 py-4">
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
            {metadataLine(item, showSymbol)}
          </p>
          <a
            href={item.url}
            target="_blank"
            rel="noreferrer"
            className="mt-2 block text-base font-semibold leading-snug text-slate-100 transition hover:text-emerald-200"
          >
            {item.title}
          </a>
          {item.summary ? (
            <p className="mt-2 text-sm leading-6 text-slate-400">{item.summary}</p>
          ) : null}
          <div className="mt-3 flex items-center justify-between gap-3">
            <span className="text-xs uppercase tracking-[0.14em] text-slate-600">{item.source_type?.replaceAll("_", " ") ?? "news"}</span>
            <a
              href={item.url}
              target="_blank"
              rel="noreferrer"
              className="text-sm font-semibold text-emerald-200 transition hover:text-emerald-100"
            >
              Open article
            </a>
          </div>
        </article>
      ))}
    </div>
  );
}
