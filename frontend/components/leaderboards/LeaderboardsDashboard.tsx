"use client";

import Link from "next/link";
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { recordProductEvent } from "@/lib/api";
import { recordGoogleAnalyticsEvent } from "@/lib/googleAnalytics";
import { hasPrivacyConsent } from "@/lib/privacyConsent";

type Snapshot = { items: Record<string, unknown>[]; generated_at: string | null; timeframe_label?: string; methodology?: string; empty_message?: string };
type Props = { topStocks: Snapshot; congress: Snapshot | null; insiders: Snapshot | null; institutions: Snapshot | null; canViewPerformance: boolean; canViewInstitutions: boolean };

const stockFilters = ["All Stocks", "US", "Large Cap", "Mid Cap", "Small Cap", "Tech", "Healthcare", "Financials"];
const rankingFactors = ["Fundamentals", "Technicals", "Insiders", "Congress", "Institutions", "Contracts", "Analysts", "Macro"];

function text(row: Record<string, unknown>, key: string) { return typeof row[key] === "string" ? row[key] : ""; }
function number(row: Record<string, unknown>, key: string) { const value = row[key]; return typeof value === "number" && Number.isFinite(value) ? value : null; }
function percent(value: number | null, digits = 1) { return value === null ? "—" : `${value.toFixed(digits)}%`; }
function price(value: number | null) { return value === null ? "—" : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value); }
function fresh(value: string | null) {
  if (!value) return "Snapshot unavailable";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "Stored snapshot" : new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/Los_Angeles", timeZoneName: "short" }).format(date);
}
function track(eventName: string, properties: Record<string, string | number | boolean | null> = {}) {
  recordProductEvent({ event_name: eventName, path: "/leaderboards", properties });
  if (hasPrivacyConsent("analytics")) recordGoogleAnalyticsEvent(eventName, properties);
}

function Panel({ id, trail, title, subtitle, snapshot, action, children, className = "" }: { id: string; trail: string; title: string; subtitle: string; snapshot: Snapshot | null; action?: ReactNode; children: ReactNode; className?: string }) {
  return <section id={id} className={`scroll-mt-24 rounded-lg border border-cyan-100/10 bg-[#06121d]/95 p-4 shadow-[0_14px_36px_rgba(0,0,0,0.2)] sm:p-5 ${className}`}>
    <div className="flex items-start justify-between gap-4 border-b border-white/10 pb-4">
      <div><p className="text-[10px] text-slate-500">Leaderboards <span className="mx-1 text-slate-700">›</span> {trail}</p><h2 className="mt-2 text-xl font-semibold tracking-tight text-white sm:text-2xl">{title}</h2><p className="mt-1 text-xs leading-5 text-slate-400">{subtitle}</p></div>
      <div className="shrink-0 text-right">{action}<p className="mt-2 text-[10px] text-slate-500">Updated {fresh(snapshot?.generated_at ?? null)}</p></div>
    </div>
    {children}
    {snapshot?.methodology ? <p className="mt-4 text-[11px] leading-5 text-slate-500">{snapshot.methodology}</p> : null}
  </section>;
}

function Empty({ snapshot }: { snapshot: Snapshot | null }) { return <div className="mt-4 rounded-md border border-white/10 bg-slate-950/60 px-4 py-5 text-sm leading-6 text-slate-400">{snapshot?.empty_message ?? "Historical performance rankings are still being built as more qualifying records mature."}</div>; }
function Locked({ title, detail }: { title: string; detail: string }) { return <div className="mt-4 rounded-md border border-white/10 bg-slate-950/60 px-4 py-5"><p className="font-semibold text-white">{title}</p><p className="mt-1 text-sm leading-6 text-slate-400">{detail}</p><Link href="/pricing" className="mt-3 inline-flex text-sm font-semibold text-emerald-200 hover:text-emerald-100">View plans →</Link></div>; }

function Direction({ row }: { row: Record<string, unknown> }) {
  const band = text(row, "confirmation_band").replace(/^./, (letter) => letter.toUpperCase());
  const direction = text(row, "confirmation_direction").replace(/^./, (letter) => letter.toUpperCase());
  return <span className="inline-flex rounded bg-emerald-400/10 px-2 py-1 text-[11px] font-medium text-emerald-200">{[band, direction].filter(Boolean).join(" ") || "Bullish"}</span>;
}

function TopStocks({ snapshot }: { snapshot: Snapshot }) {
  return <Panel id="top-stocks" trail="Top Stocks" title="Top Stocks" subtitle="Stocks ranked by Walnut Confirmation Score" snapshot={snapshot} className="min-w-0">
    <div className="mt-4 flex gap-2 overflow-x-auto pb-1 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">{stockFilters.map((filter, index) => <button key={filter} type="button" className={`shrink-0 rounded-md border px-2.5 py-1.5 text-[11px] ${index === 0 ? "border-emerald-300/45 bg-emerald-300/10 text-emerald-100" : "border-white/10 bg-slate-950/50 text-slate-400"}`}>{filter}</button>)}</div>
    <div className="mt-3 overflow-x-auto"><table className="min-w-[730px] w-full text-left text-xs"><thead className="border-y border-white/10 bg-white/[0.025] text-[10px] font-medium text-slate-400"><tr><th className="px-3 py-3">Rank</th><th className="px-3 py-3">Ticker</th><th className="px-3 py-3">Company</th><th className="px-3 py-3">Confirmation score</th><th className="px-3 py-3">Direction</th><th className="px-3 py-3">Price</th><th className="px-3 py-3">Action</th></tr></thead><tbody className="divide-y divide-white/10">{snapshot.items.map((row) => {
      const symbol = text(row, "symbol"); const href = text(row, "ticker_url") || `/ticker/${symbol}`; const score = number(row, "confirmation_score");
      return <tr key={symbol} className="hover:bg-white/[0.025]"><td className="px-3 py-3 font-mono text-sm font-semibold text-white">{number(row, "rank") ?? "—"}</td><td className="px-3 py-3"><Link href={href} onClick={() => track("leaderboard_stock_click", { symbol })} className="font-mono text-sm font-semibold text-white hover:text-emerald-200">{symbol}</Link></td><td className="max-w-[9rem] truncate px-3 py-3 text-slate-300">{text(row, "company_name")}</td><td className="px-3 py-3"><div className="flex items-center gap-2"><span className="h-1.5 w-16 overflow-hidden rounded-full bg-slate-700"><span className="block h-full rounded-full bg-gradient-to-r from-emerald-300 to-emerald-100" style={{ width: `${Math.min(Math.max(score ?? 0, 0), 100)}%` }} /></span><span className="font-mono text-emerald-100">{score ?? "—"}</span></div></td><td className="px-3 py-3"><Direction row={row} /></td><td className="px-3 py-3 font-mono text-slate-200">{price(number(row, "price"))}</td><td className="px-3 py-3"><Link href={href} onClick={() => track("leaderboard_stock_click", { symbol })} className="whitespace-nowrap rounded-md border border-white/15 px-2 py-1.5 font-semibold text-slate-100 hover:border-emerald-300/50 hover:text-emerald-100">View analysis</Link></td></tr>;
    })}</tbody></table></div>
    {!snapshot.items.length ? <Empty snapshot={snapshot} /> : <p className="mt-3 text-[11px] text-slate-500">Scores range from 0–100. Higher scores indicate stronger cross-source evidence alignment.</p>}
  </Panel>;
}

function Congress({ snapshot, allowed }: { snapshot: Snapshot | null; allowed: boolean }) {
  const [sort, setSort] = useState<"alpha_pct" | "cagr_pct">("alpha_pct");
  const rows = useMemo(() => [...(snapshot?.items ?? [])].sort((a, b) => (number(b, sort) ?? -Infinity) - (number(a, sort) ?? -Infinity)), [snapshot, sort]);
  const select = <label className="block rounded-md border border-white/10 bg-slate-950/70 px-2 py-1.5 text-[10px] text-slate-400">Based on <select aria-label="Congress sort" value={sort} onChange={(event) => { const next = event.target.value as typeof sort; setSort(next); track("leaderboard_sort_change", { section: "congress", sort: next }); }} className="bg-transparent text-slate-200 outline-none"><option value="alpha_pct">3Y alpha</option><option value="cagr_pct">3Y CAGR</option></select></label>;
  return <Panel id="congress" trail="Congress" title="Top Congress Investors" subtitle="Congress members ranked by historical portfolio performance" snapshot={snapshot} action={select} className="min-w-0">
    {!allowed ? <Locked title="Premium leaderboard" detail="Congress portfolio rankings are available with Premium." /> : !rows.length ? <Empty snapshot={snapshot} /> : <div className="mt-3 overflow-x-auto"><table className="min-w-[680px] w-full text-left text-xs"><thead className="border-y border-white/10 bg-white/[0.025] text-[10px] text-slate-400"><tr><th className="px-3 py-3">Rank</th><th className="px-3 py-3">Member</th><th className="px-3 py-3">Party</th><th className="px-3 py-3">Total return</th><th className="px-3 py-3">CAGR</th><th className="px-3 py-3">Alpha</th><th className="px-3 py-3">Max drawdown</th></tr></thead><tbody className="divide-y divide-white/10">{rows.map((row, index) => <tr key={text(row, "href") || text(row, "name")} className="hover:bg-white/[0.025]"><td className="px-3 py-3 font-mono text-white">{index + 1}</td><td className="px-3 py-3"><Link href={text(row, "href")} onClick={() => track("leaderboard_member_click", { name: text(row, "name") })} className="font-medium text-white hover:text-emerald-200">{text(row, "name")}</Link><p className="text-[10px] text-slate-500">{text(row, "chamber")}</p></td><td className="px-3 py-3 text-slate-300">{text(row, "party") || "—"}</td><td className="px-3 py-3 font-mono text-emerald-200">{percent(number(row, "total_return_pct"))}</td><td className="px-3 py-3 font-mono text-emerald-200">{percent(number(row, "cagr_pct"))}</td><td className="px-3 py-3 font-mono text-emerald-200">{percent(number(row, "alpha_pct"))}</td><td className="px-3 py-3 font-mono text-slate-300">{percent(number(row, "max_drawdown_pct"))}</td></tr>)}</tbody></table></div>}
  </Panel>;
}

function Insiders({ snapshot, allowed }: { snapshot: Snapshot | null; allowed: boolean }) {
  return <Panel id="insiders" trail="Insiders" title="Top Insiders" subtitle="Corporate insiders ranked by historical portfolio performance" snapshot={snapshot} action={<span className="rounded-md border border-white/10 px-2 py-1.5 text-[10px] text-slate-400">Based on 1Y performance</span>} className="min-w-0">
    {!allowed ? <Locked title="Premium leaderboard" detail="Insider portfolio rankings are available with Premium when sufficient qualifying history is available." /> : <Empty snapshot={snapshot} />}
  </Panel>;
}

function Institutions({ snapshot, allowed }: { snapshot: Snapshot | null; allowed: boolean }) {
  return <Panel id="institutions" trail="Institutions" title="Top Institutions" subtitle="Institutions ranked by reported-holdings performance" snapshot={snapshot} action={<span className="rounded-md border border-white/10 px-2 py-1.5 text-[10px] text-slate-400">Based on 3Y return</span>} className="min-w-0">
    {!allowed ? <Locked title="Pro leaderboard" detail="Institution performance rankings are available with Pro." /> : !snapshot?.items.length ? <Empty snapshot={snapshot} /> : <div className="mt-3 overflow-x-auto"><table className="min-w-[560px] w-full text-left text-xs"><thead className="border-y border-white/10 bg-white/[0.025] text-[10px] text-slate-400"><tr><th className="px-3 py-3">Rank</th><th className="px-3 py-3">Institution</th><th className="px-3 py-3">3Y return</th><th className="px-3 py-3">Positions</th><th className="px-3 py-3">Coverage</th></tr></thead><tbody className="divide-y divide-white/10">{snapshot.items.map((row) => <tr key={text(row, "cik")} className="hover:bg-white/[0.025]"><td className="px-3 py-3 font-mono text-white">{number(row, "rank") ?? "—"}</td><td className="px-3 py-3"><Link href={text(row, "href")} onClick={() => track("leaderboard_institution_click", { cik: text(row, "cik") })} className="font-medium text-white hover:text-emerald-200">{text(row, "name")}</Link></td><td className="px-3 py-3 font-mono text-emerald-200">{percent(number(row, "total_return_pct"))}</td><td className="px-3 py-3 font-mono text-slate-200">{number(row, "positions_count") ?? "—"}</td><td className="px-3 py-3 font-mono text-slate-300">{percent(number(row, "coverage_pct"))}</td></tr>)}</tbody></table></div>}
  </Panel>;
}

function HowStocksAreRanked() {
  return <section className="rounded-lg border border-cyan-100/10 bg-[#06121d]/95 p-5"><h2 className="text-lg font-semibold text-white">How Stocks Are Ranked</h2><p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">Walnut&apos;s proprietary Confirmation Score measures the alignment and strength of evidence across multiple independent data sources.</p><div className="mt-5 grid grid-cols-2 divide-x divide-y divide-white/10 border border-white/10 sm:grid-cols-4 lg:grid-cols-8">{rankingFactors.map((factor, index) => <div key={factor} className="min-h-24 p-3"><span className="font-mono text-emerald-300">0{index + 1}</span><p className="mt-3 text-xs font-semibold text-white">{factor}</p><p className="mt-1 text-[10px] leading-4 text-slate-500">Independent signal context</p></div>)}</div><p className="mt-5 text-xs text-slate-500">The score is not a prediction of future returns and does not guarantee outperformance.</p></section>;
}

export function LeaderboardsDashboard(props: Props) {
  useEffect(() => { track("leaderboards_view"); }, []);
  useEffect(() => { const seen = new Set<string>(); const observer = new IntersectionObserver((entries) => entries.forEach((entry) => { if (entry.isIntersecting && !seen.has(entry.target.id)) { seen.add(entry.target.id); track("leaderboard_section_view", { section: entry.target.id }); } }), { threshold: 0.35 }); document.querySelectorAll("[data-leaderboard-section]").forEach((element) => observer.observe(element)); return () => observer.disconnect(); }, []);
  return <div className="space-y-4"><div className="grid grid-cols-1 gap-4 2xl:grid-cols-2">{[["top-stocks", <TopStocks key="stocks" snapshot={props.topStocks} />], ["congress", <Congress key="congress" snapshot={props.congress} allowed={props.canViewPerformance} />], ["insiders", <Insiders key="insiders" snapshot={props.insiders} allowed={props.canViewPerformance} />], ["institutions", <Institutions key="institutions" snapshot={props.institutions} allowed={props.canViewInstitutions} />]].map(([section, content]) => <div data-leaderboard-section={section as string} key={section as string}>{content as ReactNode}</div>)}</div><HowStocksAreRanked /></div>;
}
