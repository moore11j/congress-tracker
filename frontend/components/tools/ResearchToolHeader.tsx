import Link from "next/link";
import { ArrowLeftRight, ChartNoAxesCombined, FlaskConical, SlidersHorizontal } from "lucide-react";
import "./research-tools.css";

export function ResearchToolHeader({ title, description, active }: { title: string; description: string; active: "mixer" | "backtesting" | "compare" | "screener" }) {
  const icons = { mixer: FlaskConical, backtesting: ChartNoAxesCombined, compare: ArrowLeftRight, screener: SlidersHorizontal };
  const Icon = icons[active];
  return <header className="research-tool-header">
    <div className="flex items-center gap-3"><span className="rounded-xl border border-emerald-300/20 bg-emerald-300/10 p-2.5 text-emerald-300"><Icon size={22} aria-hidden="true" /></span><p className="text-xs font-semibold uppercase tracking-[.22em] text-emerald-300">Walnut tools · research with context</p></div>
    <h1 className="mt-4 text-3xl font-semibold tracking-tight text-white sm:text-4xl">{title}</h1>
    <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-400 sm:text-base">{description}</p>
    <nav aria-label="Research tools" className="mt-5 flex flex-wrap gap-2">{[
      ["screener", "/screener", "Screen stocks"], ["compare", "/compare/_/_", "Compare stocks"], ["mixer", "/signal-mixer", "Signal Mixer"], ["backtesting", "/backtesting", "Portfolio backtesting"],
    ].map(([key, href, label]) => <Link key={key} href={href} aria-current={active === key ? "page" : undefined} className={`rounded-lg border px-3 py-2 text-xs font-medium transition ${active === key ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-200" : "border-white/10 text-slate-400 hover:border-white/20 hover:text-white"}`}>{label}</Link>)}</nav>
  </header>;
}
