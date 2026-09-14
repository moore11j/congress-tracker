import { HomepageCtaLink } from "@/components/landing/HomepageCtaLink";
import type { HomepageEvidence, HomepageResearch } from "@/lib/homepagePreview";

export function homepageDate(value: string | null): string {
  return value ? new Intl.DateTimeFormat("en-US", {month: "short", day: "numeric", year: "numeric", timeZone: "UTC"}).format(new Date(value)) : "Date unavailable";
}

function Evidence({items, tickerUrl}: {items: HomepageEvidence[]; tickerUrl: string}) {
  return <ul className="mt-4 space-y-4">{items.map(item => <li key={item.category}>
    <h4 className="text-sm font-semibold text-white">{item.title}</h4>
    <p className="mt-1 text-sm leading-6 text-slate-300">{item.description}</p>
    {item.details.map(detail => <p key={detail} className="mt-1 text-sm text-slate-300">{detail}</p>)}
    <a href={`${tickerUrl}${item.anchor}`} className="mt-2 block text-xs leading-5 text-emerald-200 underline underline-offset-4">{item.source}</a>
    <p className="mt-1 text-xs text-slate-500">Underlying data: {homepageDate(item.dataAsOf)}</p>
  </li>)}</ul>;
}

export function HomepageResearchExample({example, appUrl, rankingAt}: {example: HomepageResearch | null; appUrl: string; rankingAt: string | null}) {
  const tickerUrl = example ? `${appUrl}/ticker/${encodeURIComponent(example.stock.symbol)}` : `${appUrl}/screener`;
  return <section id="research-example" className="border-b border-white/10 px-4 py-12 sm:px-6 lg:px-8">
    <div className="mx-auto max-w-7xl">
      <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">From ranking to research</p>
      <h2 className="mt-3 max-w-4xl text-3xl font-semibold text-white sm:text-4xl">{example ? `Why is ${example.stock.symbol} near the top—and what could challenge it?` : "What supports a ranking—and what could challenge it?"}</h2>
      {example ? <>
        <p className="mt-4 max-w-3xl text-base leading-7 text-slate-400">{example.stock.companyName} is #{example.stock.rank} in the ranking shown above. Here is what the available public evidence supports, and what to investigate before adding it to a portfolio.</p>
        <p className="mt-3 text-xs leading-5 text-slate-500">Ranking snapshot: {homepageDate(rankingAt)} · Research assembled: {homepageDate(example.generatedAt)} (UTC). Sources update on different schedules; newer evidence can differ from the ranking snapshot.</p>
        <div className="mt-6 grid gap-4 md:grid-cols-2">
          <article className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.035] p-5 sm:p-6">
            <h3 className="text-base font-semibold text-emerald-200">Supporting evidence</h3><Evidence items={example.supporting} tickerUrl={tickerUrl}/>
          </article>
          <article className="rounded-lg border border-white/15 bg-slate-950/85 p-5 sm:p-6">
            <h3 className="text-base font-semibold text-slate-100">Risks and conflicting evidence</h3>
            {example.risks.length ? <Evidence items={example.risks} tickerUrl={tickerUrl}/> : <p className="mt-4 text-sm leading-6 text-slate-400">This public snapshot does not identify a conflicting signal. That does not mean the stock is risk-free. Review the full research and source coverage.</p>}
          </article>
        </div>
        <div className="mt-4 grid gap-4 sm:grid-cols-2">
          <div className="rounded-lg border border-white/10 p-5"><h3 className="text-sm font-semibold text-white">Check the historical context</h3><p className="mt-2 text-sm leading-6 text-slate-400">Compare historical results with their benchmark, measurement dates and methodology in the full research. A past result is context for your decision, not a forecast.</p><a href={`${appUrl}/outcomes`} className="mt-3 inline-flex text-sm text-emerald-200 underline underline-offset-4">Explore historical outcomes</a></div>
          <div className="rounded-lg border border-white/10 p-5"><h3 className="text-sm font-semibold text-white">Decide what to monitor</h3><ul className="mt-2 space-y-1 text-sm leading-6 text-slate-400">{(example.watch.length ? example.watch : ["Review new disclosures and price changes as they become available."]).map(item => <li key={item}>{item}</li>)}</ul><a href={`${appUrl}/watchlists`} className="mt-3 inline-flex text-sm text-emerald-200 underline underline-offset-4">Save and monitor in a watchlist</a></div>
        </div>
      </> : <p className="mt-4 max-w-3xl text-base leading-7 text-slate-400">A source-backed example is not available right now. Open a ranked stock to inspect its evidence, risks and historical context, then decide what you want to monitor.</p>}
      <div className="mt-6 flex flex-col gap-3 sm:flex-row sm:items-center">
        <HomepageCtaLink href={tickerUrl} eventName="analyze_stock_click" className="inline-flex items-center justify-center rounded-lg border border-emerald-300/30 px-5 py-3 text-sm font-semibold text-emerald-200 hover:bg-emerald-300/10">{example ? `Open ${example.stock.symbol} research` : "Open Screener"}</HomepageCtaLink>
        <p className="max-w-xl text-xs leading-5 text-slate-500">Public evidence preview. Confirmation Scores and deeper datasets remain available with the appropriate paid plan.</p>
      </div>
    </div>
  </section>;
}
