"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import type { ProfileActivityItem } from "@/lib/api";

const FILTERS = ["All", "Congress", "Insider", "Institution", "Department"] as const;

export function LatestProfileActivity({ items }: { items: ProfileActivityItem[] }) {
  const [filter, setFilter] = useState<(typeof FILTERS)[number]>("All");
  const rows = useMemo(() => filter === "All" ? items : items.filter((item) => item.type === filter), [filter, items]);

  return <div>
    <div className="flex flex-wrap gap-2" role="tablist" aria-label="Profile activity type">
      {FILTERS.map((option) => <button key={option} type="button" role="tab" aria-selected={filter === option} onClick={() => setFilter(option)} className={`rounded-full border px-3 py-1 text-xs font-semibold transition ${filter === option ? "border-emerald-300/50 bg-emerald-300/15 text-emerald-100" : "border-slate-700 bg-slate-950/60 text-slate-300 hover:border-slate-500 hover:text-white"}`}>{option === "Institution" ? "Institutions" : option === "Department" ? "Departments" : option}</button>)}
    </div>
    {!rows.length ? <p className="py-8 text-sm text-slate-400">No recent {filter === "All" ? "profile" : filter.toLowerCase()} activity is available.</p> : <div className="mt-3 overflow-x-auto"><table className="min-w-[46rem] w-full text-left text-xs"><thead className="border-y border-white/10 text-[10px] font-semibold uppercase tracking-[.13em] text-slate-500"><tr><th className="px-2 py-2">Time</th><th className="px-2 py-2">Profile</th><th className="px-2 py-2">Type</th><th className="px-2 py-2">Ticker</th><th className="px-2 py-2">Activity</th><th className="px-2 py-2 text-right">Value</th><th className="px-2 py-2 text-right">Score</th></tr></thead><tbody className="divide-y divide-white/5">{rows.slice(0, 8).map((item) => <tr key={String(item.id)}><td className="whitespace-nowrap px-2 py-2.5 text-slate-400">{formatTime(item.time)}</td><td className="max-w-36 truncate px-2 py-2.5 font-semibold"><ProfileLink item={item} /></td><td className="px-2 py-2.5"><span className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold ${typeClasses(item.type)}`}>{item.type}</span></td><td className="px-2 py-2.5 font-semibold"><TickerLink item={item} /></td><td className="max-w-28 truncate px-2 py-2.5 text-slate-300">{item.activity ?? "-"}</td><td className="px-2 py-2.5 text-right font-semibold tabular-nums text-white">{formatValue(item.value)}</td><td className="px-2 py-2.5 text-right"><span className="inline-flex min-w-7 justify-center rounded-full border border-emerald-300/40 bg-emerald-300/10 px-1.5 py-0.5 font-semibold tabular-nums text-emerald-200">{formatScore(item.metric)}</span></td></tr>)}</tbody></table></div>}
  </div>;
}

function ProfileLink({ item }: { item: ProfileActivityItem }) { return item.profile_href ? <Link href={item.profile_href} className="text-emerald-200 hover:text-emerald-100">{item.profile}</Link> : <span className="text-slate-200">{item.profile}</span>; }
function TickerLink({ item }: { item: ProfileActivityItem }) { return item.symbol ? item.ticker_href ? <Link href={item.ticker_href} className="text-emerald-200 hover:text-emerald-100">{item.symbol}</Link> : <span className="text-emerald-200">{item.symbol}</span> : <span className="text-slate-500">-</span>; }
function formatValue(value: number | null | undefined) { return typeof value === "number" ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(value) : "-"; }
function formatScore(value: number | null | undefined) { return typeof value === "number" ? Math.round(value).toLocaleString() : "-"; }
function formatTime(value: string | null | undefined) { if (!value) return "-"; const parsed = new Date(value); return Number.isNaN(parsed.valueOf()) ? value : new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(parsed); }
function typeClasses(type: string) { return type === "Congress" ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-200" : type === "Insider" ? "border-blue-300/40 bg-blue-300/10 text-blue-200" : type === "Institution" ? "border-violet-300/40 bg-violet-300/10 text-violet-200" : "border-amber-300/40 bg-amber-300/10 text-amber-200"; }
