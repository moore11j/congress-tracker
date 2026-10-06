import Link from "next/link";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { directoryCategories, directoryPath, type DirectoryCategory } from "@/lib/publicDirectory";

export const metadata = appPageMetadata("/explore", {
  title: "Browse Stocks, Insiders & Public Filings | Walnut Markets",
  description: "Explore Walnut's stock, Congress member, corporate insider, institutional investor, and government agency profiles from one research directory.",
});

export default function ExplorePage() {
  return <section className="mx-auto max-w-6xl py-8">
    <h1 className="text-3xl font-semibold text-white">Explore public market research</h1>
    <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-300">Start with a company, a person, an investment manager, or a government agency. Each directory links to public profiles where you can review the available evidence and its reporting dates.</p>
    <div className="mt-8 grid gap-4 md:grid-cols-2">
      {(Object.keys(directoryCategories) as DirectoryCategory[]).map(category => <Link prefetch={false} key={category} href={directoryPath(category)} className="rounded-xl border border-white/10 bg-slate-900/50 p-6 hover:border-emerald-300/40">
        <h2 className="text-xl font-semibold text-emerald-200">{directoryCategories[category].title}</h2>
        <p className="mt-3 text-sm leading-6 text-slate-300">{directoryCategories[category].description}</p>
      </Link>)}
    </div>
    <p className="mt-8 text-sm text-slate-300">Looking for an explanation of a market theme? <a className="text-emerald-200 underline" href="https://walnutmarkets.com/research">Read Walnut research briefs</a>.</p>
    <nav aria-label="Research starting points" className="mt-6 rounded-xl border border-white/10 p-5">
      <h2 className="text-lg font-semibold text-white">Research starting points</h2>
      <p className="mt-2 text-sm leading-6 text-slate-300">Compare reported evidence and dates, then follow a company you want to revisit.</p>
      <ul className="mt-4 grid gap-3 text-sm text-emerald-200 sm:grid-cols-2">
        {[
          ["https://walnutmarkets.com/insider-trading-tracker", "Track reported corporate-insider activity"],
          ["https://walnutmarkets.com/stock-analysis-platform", "How to analyze a stock with Walnut"],
          ["/ticker/NVDA", "NVIDIA stock research"], ["/ticker/ANET", "Arista Networks stock research"],
          ["/ticker/NBIS", "Nebius stock research"], ["/ticker/ALV", "Autoliv stock research"],
          ["/departments/nasa", "NASA contracts and public recipients"],
          ["/departments/department-of-energy", "Department of Energy contracts and public recipients"],
          ["https://walnutmarkets.com/research/who-is-buying-nvidia-stock-in-the-latest-13f-filings", "NVIDIA: four managers’ reported share changes"],
          ["https://walnutmarkets.com/research/nbis-vs-crwv-ai-neoclouds", "Nebius and CoreWeave: a dated financial comparison"],
        ].map(([href, label]) => <li key={href}><Link prefetch={false} href={href} className="hover:underline">{label}</Link></li>)}
      </ul>
    </nav>
  </section>;
}
