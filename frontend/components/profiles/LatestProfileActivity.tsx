"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import type { ProfileActivityItem } from "@/lib/api";

const FILTERS = ["All", "Congress", "Insider", "Institution", "Department"] as const;

export function LatestProfileActivity({ items }: { items: ProfileActivityItem[] }) {
  const [filter, setFilter] = useState<(typeof FILTERS)[number]>("All");
  const rows = useMemo(() => filter === "All" ? items : items.filter((item) => item.type === filter), [filter, items]);

  return <div>
    <div className="flex flex-wrap gap-1.5" role="tablist" aria-label="Profile activity type">
      {FILTERS.map((option) => <button key={option} type="button" role="tab" aria-selected={filter === option} onClick={() => setFilter(option)} className={`rounded-full border px-2.5 py-0.5 text-[10px] font-semibold transition ${filter === option ? "border-emerald-300/50 bg-emerald-300/15 text-emerald-100" : "border-slate-700 bg-slate-950/60 text-slate-300 hover:border-slate-500 hover:text-white"}`}>{option === "Institution" ? "Institutions" : option === "Department" ? "Departments" : option}</button>)}
    </div>
    {!rows.length ? <p className="py-8 text-sm text-slate-400">No recent {filter === "All" ? "profile" : filter.toLowerCase()} activity is available.</p> : <div className="mt-2 overflow-hidden"><table className="w-full table-fixed text-left text-[11px]"><thead className="border-y border-white/10 text-[9px] font-semibold uppercase tracking-[.12em] text-slate-500"><tr><th className="w-[4.4rem] px-1.5 py-1.5">Time</th><th className="px-1.5 py-1.5">Profile</th><th className="hidden w-[4.9rem] px-1.5 py-1.5 sm:table-cell">Type</th><th className="w-[3.5rem] px-1.5 py-1.5">Ticker</th><th className="w-[5.9rem] px-1.5 py-1.5">Activity</th><th className="w-[4.4rem] px-1.5 py-1.5 text-right">Value</th></tr></thead><tbody className="divide-y divide-white/5">{rows.slice(0, 5).map((item) => <tr key={String(item.id)}><td className="truncate px-1.5 py-1.5 text-slate-400">{formatRelativeTime(item.time)}</td><td className="truncate px-1.5 py-1.5 font-semibold"><ProfileLink item={item} /></td><td className="hidden px-1.5 py-1.5 sm:table-cell"><span className={`inline-flex max-w-full rounded-full border px-1.5 py-0.5 text-[9px] font-semibold ${typeClasses(item.type)}`}>{item.type}</span></td><td className="truncate px-1.5 py-1.5 font-semibold"><TickerLink item={item} /></td><td className="truncate px-1.5 py-1.5 text-slate-300" title={activityLabel(item)}>{activityLabel(item)}</td><td className="truncate px-1.5 py-1.5 text-right font-semibold tabular-nums text-white">{formatValue(item.value)}</td></tr>)}</tbody></table></div>}
  </div>;
}

function ProfileLink({ item }: { item: ProfileActivityItem }) { return item.profile_href ? <Link href={item.profile_href} prefetch={false} className="text-emerald-200 hover:text-emerald-100">{item.profile}</Link> : <span className="text-slate-200">{item.profile}</span>; }
function TickerLink({ item }: { item: ProfileActivityItem }) { return item.symbol ? item.ticker_href ? <Link href={item.ticker_href} className="text-emerald-200 hover:text-emerald-100">{item.symbol}</Link> : <span className="text-emerald-200">{item.symbol}</span> : <span className="text-slate-500">-</span>; }
function formatValue(value: number | null | undefined) { return typeof value === "number" ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(value) : "-"; }
function formatRelativeTime(value: string | null | undefined) { if (!value) return "-"; const dateOnly = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value); const parsed = dateOnly ? new Date(Number(dateOnly[1]), Number(dateOnly[2]) - 1, Number(dateOnly[3])) : new Date(value); if (Number.isNaN(parsed.valueOf())) return value; const now = new Date(); const today = new Date(now.getFullYear(), now.getMonth(), now.getDate()); const activityDay = new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate()); const daysAgo = Math.round((today.valueOf() - activityDay.valueOf()) / 86_400_000); return daysAgo <= 0 ? "Today" : `${daysAgo}D ago`; }
function activityLabel(item: ProfileActivityItem) { const activity = (item.activity ?? "").toLowerCase(); if (item.type === "Department") return "Contract Award"; if (item.type === "Institution") { if (activity.includes("new")) return "New Position"; if (/(decreas|reduc|exit|distribut)/.test(activity)) return "Decreased"; return "Increased"; } return /(sale|sell|dispos)/.test(activity) ? "Sale" : "Purchase"; }
function typeClasses(type: string) { return type === "Congress" ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-200" : type === "Insider" ? "border-blue-300/40 bg-blue-300/10 text-blue-200" : type === "Institution" ? "border-violet-300/40 bg-violet-300/10 text-violet-200" : "border-amber-300/40 bg-amber-300/10 text-amber-200"; }
