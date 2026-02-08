"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Badge } from "@/components/Badge";
import { chamberBadge } from "@/lib/format";
import { selectClassName } from "@/lib/styles";

type SignalPreset = "discovery" | "balanced" | "strict";
type SignalSort = "multiple" | "recent" | "amount";
type SignalLimit = 25 | 50 | 100;

type SignalItem = {
  event_id: number;
  ts: string;
  symbol: string;
  member_name?: string;
  member_bioguide_id?: string;
  party?: string;
  chamber?: string;
  trade_type?: string;
  amount_min?: number;
  amount_max?: number;
  baseline_median_amount_max?: number;
  baseline_count?: number;
  unusual_multiple?: number;
  source?: string;
};

type SignalDebug = {
  total_hits?: number;
  final_hits_count?: number;
  sort?: string;
  offset?: number;
};

type SignalsWrappedResponse = {
  items?: SignalItem[];
  debug?: SignalDebug;
};

const PRESETS: SignalPreset[] = ["discovery", "balanced", "strict"];
const LIMITS: SignalLimit[] = [25, 50, 100];
const SORTS: SignalSort[] = ["multiple", "recent", "amount"];

function sortDefaultForPreset(preset: SignalPreset): SignalSort {
  return preset === "discovery" ? "recent" : "multiple";
}

function parsePreset(v: string | null): SignalPreset {
  return PRESETS.includes(v as SignalPreset) ? (v as SignalPreset) : "balanced";
}

function parseSort(v: string | null, preset: SignalPreset): SignalSort {
  if (SORTS.includes(v as SignalSort)) return v as SignalSort;
  return sortDefaultForPreset(preset);
}

function parseOffset(v: string | null): number {
  const n = Number(v);
  return Number.isInteger(n) && n >= 0 ? n : 0;
}

function parseLimit(v: string | null): SignalLimit {
  const n = Number(v);
  if (n === 25 || n === 50 || n === 100) return n;
  return 100;
}

function formatUSD(n?: number): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

function formatMultiple(n?: number): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)}×`;
}

function sideLabel(tradeType?: string): { label: string; klass: string } {
  const t = (tradeType ?? "").toLowerCase();
  if (t === "purchase" || t === "buy") {
    return { label: "Buy", klass: "border-emerald-500/30 text-emerald-200 bg-emerald-500/10" };
  }
  if (t === "sale" || t === "sell") {
    return { label: "Sell", klass: "border-red-500/30 text-red-200 bg-red-500/10" };
  }
  return { label: tradeType ? tradeType : "—", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
}

function strengthLabel(m?: number): { label: string; klass: string } {
  const x = typeof m === "number" ? m : 0;
  if (x >= 8) return { label: "Whale", klass: "border-rose-500/30 text-rose-200 bg-rose-500/10" };
  if (x >= 4) return { label: "Extreme", klass: "border-orange-500/30 text-orange-200 bg-orange-500/10" };
  if (x >= 2) return { label: "Abnormal", klass: "border-amber-500/30 text-amber-200 bg-amber-500/10" };
  if (x >= 1.5) return { label: "Elevated", klass: "border-sky-500/30 text-sky-200 bg-sky-500/10" };
  return { label: "Normal", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
}

function sourceBadge(item: SignalItem): { label: string; tone: Parameters<typeof Badge>[0]["tone"] } {
  const chamber = (item.chamber ?? "").toLowerCase();
  if (chamber.includes("house")) return chamberBadge("house");
  if (chamber.includes("senate")) return chamberBadge("senate");

  const source = (item.source ?? "").toLowerCase();
  if (source.includes("house")) return chamberBadge("house");
  if (source.includes("senate")) return chamberBadge("senate");
  return chamberBadge();
}

function sortLabel(sort: SignalSort): string {
  if (sort === "recent") return "Recent";
  if (sort === "amount") return "Amount";
  return "Multiple";
}

type FetchResult = {
  items: SignalItem[];
  debug: SignalDebug | null;
};

export function SignalsClient({ apiBase }: { apiBase: string }) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();

  const preset = useMemo(() => parsePreset(searchParams.get("preset")), [searchParams]);
  const limit = useMemo(() => parseLimit(searchParams.get("limit")), [searchParams]);
  const offset = useMemo(() => parseOffset(searchParams.get("offset")), [searchParams]);
  const hasSortParam = searchParams.has("sort");
  const sort = useMemo(() => parseSort(searchParams.get("sort"), preset), [searchParams, preset]);

  const [items, setItems] = useState<SignalItem[]>([]);
  const [debug, setDebug] = useState<SignalDebug | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);

  const updateParams = useCallback(
    (updates: Record<string, string | null>, method: "push" | "replace" = "push") => {
      const params = new URLSearchParams(searchParams.toString());
      Object.entries(updates).forEach(([key, value]) => {
        if (!value) {
          params.delete(key);
        } else {
          params.set(key, value);
        }
      });
      const query = params.toString();
      const href = query ? `${pathname}?${query}` : pathname;
      if (method === "replace") {
        router.replace(href);
      } else {
        router.push(href);
      }
    },
    [pathname, router, searchParams],
  );

  const fetchBatch = useCallback(
    async (requestedOffset: number): Promise<FetchResult> => {
      const url = new URL("/api/signals/unusual", apiBase);
      url.searchParams.set("preset", preset);
      url.searchParams.set("sort", sort);
      url.searchParams.set("offset", String(requestedOffset));
      url.searchParams.set("limit", String(limit));
      url.searchParams.set("debug", "true");

      const res = await fetch(url.toString(), { cache: "no-store" });
      if (!res.ok) {
        throw new Error(`Request failed with ${res.status}`);
      }

      const json: unknown = await res.json();
      if (Array.isArray(json)) {
        return { items: json as SignalItem[], debug: null };
      }

      const wrapped = json as SignalsWrappedResponse;
      return {
        items: Array.isArray(wrapped.items) ? wrapped.items : [],
        debug: wrapped.debug ?? null,
      };
    },
    [apiBase, limit, preset, sort],
  );

  useEffect(() => {
    if (!hasSortParam) {
      updateParams({ sort, offset: String(offset) }, "replace");
    }
  }, [hasSortParam, offset, sort, updateParams]);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      setLoading(true);
      setErrorMessage(null);
      try {
        const chunkCount = Math.floor(offset / limit) + 1;
        const chunks = await Promise.all(
          Array.from({ length: chunkCount }, (_, index) => fetchBatch(index * limit)),
        );
        if (cancelled) return;

        setItems(chunks.flatMap((chunk) => chunk.items));
        const lastDebug = chunks[chunks.length - 1]?.debug ?? null;
        setDebug(lastDebug);
      } catch (e) {
        if (cancelled) return;
        setItems([]);
        setDebug(null);
        setErrorMessage(e instanceof Error ? e.message : "Unable to load signals.");
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    void load();

    return () => {
      cancelled = true;
    };
  }, [offset, limit, fetchBatch]);

  const totalHits = typeof debug?.total_hits === "number" ? debug.total_hits : null;
  const shownCount = typeof debug?.final_hits_count === "number" ? debug.final_hits_count : null;
  const canLoadMore = totalHits !== null && items.length < totalHits;

  const handlePresetChange = (nextPreset: SignalPreset) => {
    const nextSort = sortDefaultForPreset(nextPreset);
    updateParams({ preset: nextPreset, sort: nextSort, offset: "0" });
  };

  const handleSortChange = (nextSort: SignalSort) => {
    updateParams({ sort: nextSort, offset: "0" });
  };

  const handleLimitChange = (nextLimit: string) => {
    updateParams({ limit: nextLimit, offset: "0" });
  };

  const handleLoadMore = async () => {
    setLoadingMore(true);
    try {
      const nextOffset = offset + limit;
      const nextBatch = await fetchBatch(nextOffset);
      setItems((prev) => [...prev, ...nextBatch.items]);
      setDebug(nextBatch.debug ?? debug);
      updateParams({ offset: String(nextOffset) });
    } catch (e) {
      setErrorMessage(e instanceof Error ? e.message : "Unable to load signals.");
    } finally {
      setLoadingMore(false);
    }
  };

  const card = "rounded-2xl border border-slate-800 bg-slate-950/40 shadow-sm";
  const pill = "inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium";
  const btn =
    "inline-flex items-center justify-center rounded-full border px-3 py-1 text-xs font-medium transition hover:bg-slate-900/60";
  const btnActive = "border-emerald-500/40 text-emerald-200 bg-emerald-500/10";
  const btnIdle = "border-slate-800 text-slate-200 bg-slate-950/30";

  return (
    <>
      <div className={`mt-6 p-4 ${card}`}>
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-wrap items-center gap-3">
            <div className="text-xs text-slate-400">Preset</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {PRESETS.map((p) => (
                <button key={p} type="button" onClick={() => handlePresetChange(p)} className={`${btn} ${preset === p ? btnActive : btnIdle}`}>
                  {p.toUpperCase()}
                </button>
              ))}
            </div>

            <div className="ml-2 text-xs text-slate-400">Limit</div>
            <div className="inline-flex items-center gap-2">
              {LIMITS.map((l) => (
                <button key={l} type="button" onClick={() => handleLimitChange(String(l))} className={`${btn} ${limit === l ? btnActive : btnIdle}`}>
                  {l}
                </button>
              ))}
            </div>

            <div className="ml-2 flex items-center gap-2">
              <label htmlFor="signals-sort" className="text-xs text-slate-400">
                Sort
              </label>
              <select
                id="signals-sort"
                value={sort}
                onChange={(event) => handleSortChange(event.target.value as SignalSort)}
                className={`h-8 min-w-[120px] text-xs ${selectClassName}`}
              >
                <option value="multiple">Multiple</option>
                <option value="recent">Recent</option>
                <option value="amount">Amount</option>
              </select>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <span className={`${pill} border-slate-800 text-slate-200 bg-slate-950/30`}>
              Showing <span className="text-white">{items.length}</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              preset <span className="text-white">{preset}</span>
            </span>
          </div>
        </div>
      </div>

      <div className="mt-6">
        <div className="mb-3">
          <h2 className="text-xl font-semibold text-white">Signals table</h2>
          <p className="text-sm text-slate-400">Abnormal trades vs per-symbol historical median.</p>
          {debug ? (
            <p className="mt-1 text-xs text-slate-500">
              Matches: {totalHits ?? "—"} (showing {shownCount ?? items.length}) • Sort: {debug.sort ?? sortLabel(sort)}
            </p>
          ) : null}
        </div>

        <div className={`${card} overflow-hidden`}>
          <div className="overflow-x-auto">
            <table className="min-w-full border-collapse text-sm">
              <thead className="bg-slate-950/50 text-xs uppercase tracking-wider text-slate-400">
                <tr>
                  <th className="px-4 py-3 text-left">Time</th>
                  <th className="px-4 py-3 text-left">Ticker</th>
                  <th className="px-4 py-3 text-left">Member</th>
                  <th className="px-4 py-3 text-left">Side</th>
                  <th className="px-4 py-3 text-left">Amount</th>
                  <th className="px-4 py-3 text-left">Baseline</th>
                  <th className="px-4 py-3 text-left">Multiple</th>
                  <th className="px-4 py-3 text-left">Strength</th>
                  <th className="px-4 py-3 text-left">Source</th>
                </tr>
              </thead>

              <tbody className="divide-y divide-slate-800">
                {items.length === 0 ? (
                  <tr>
                    <td className="px-4 py-10 text-center text-slate-400" colSpan={9}>
                      {loading ? "Loading signals..." : errorMessage ? "Unable to load signals." : "No unusual signals returned."}
                    </td>
                  </tr>
                ) : (
                  items.map((it) => {
                    const side = sideLabel(it.trade_type);
                    const strength = strengthLabel(it.unusual_multiple);
                    const source = sourceBadge(it);

                    return (
                      <tr key={it.event_id} className="hover:bg-slate-900/20">
                        <td className="px-4 py-3 text-slate-300">
                          <span title={it.ts}>{it.ts}</span>
                        </td>
                        <td className="px-4 py-3">
                          <Link href={`/ticker/${it.symbol}`} className="font-mono text-emerald-200 hover:underline">
                            {it.symbol}
                          </Link>
                        </td>
                        <td className="px-4 py-3 text-slate-200">
                          {it.member_bioguide_id ? (
                            <Link href={`/member/${it.member_bioguide_id}`} className="hover:underline">
                              {it.member_name ?? "—"}
                            </Link>
                          ) : (
                            it.member_name ?? "—"
                          )}
                        </td>
                        <td className="px-4 py-3">
                          <span className={`${pill} ${side.klass}`}>{side.label}</span>
                        </td>
                        <td className="px-4 py-3 text-slate-200" title={`${formatUSD(it.amount_min)} – ${formatUSD(it.amount_max)}`}>
                          {formatUSD(it.amount_max)}
                        </td>
                        <td className="px-4 py-3 text-slate-200">{formatUSD(it.baseline_median_amount_max)}</td>
                        <td className="px-4 py-3 text-slate-200">{formatMultiple(it.unusual_multiple)}</td>
                        <td className="px-4 py-3">
                          <span className={`${pill} ${strength.klass}`}>{strength.label}</span>
                        </td>
                        <td className="px-4 py-3">
                          <Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">
                            {source.label}
                          </Badge>
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="mt-4 flex items-center gap-3">
          {canLoadMore ? (
            <button
              type="button"
              onClick={handleLoadMore}
              disabled={loadingMore}
              className={`${btn} ${btnIdle} disabled:cursor-not-allowed disabled:opacity-60`}
            >
              {loadingMore ? "Loading..." : "Load more"}
            </button>
          ) : null}
          {loadingMore ? <span className="text-xs text-slate-400">Fetching next page…</span> : null}
        </div>
      </div>
    </>
  );
}
