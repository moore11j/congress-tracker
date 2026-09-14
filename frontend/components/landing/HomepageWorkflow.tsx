import { HomepageCtaLink } from "@/components/landing/HomepageCtaLink";

const section = "scroll-mt-24 border-b border-white/10 px-4 py-12 sm:px-6 lg:px-8";
const link = "mt-4 inline-flex text-sm font-semibold text-emerald-200 underline underline-offset-4";

export function PortfolioBlueprint({appUrl}: {appUrl: string}) {
  return <section id="whats-working" className={section}><div className="mx-auto max-w-7xl">
    <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Your portfolio blueprint</p>
    <h2 className="mt-3 max-w-3xl text-3xl font-semibold text-white sm:text-4xl">Bring the ideas, the people and the strategies together.</h2>
    <p className="mt-4 max-w-3xl text-base leading-7 text-slate-400">Combine ranked stocks, historically strong Congress members and market participants, and backtested strategies. Use the evidence to choose what belongs in your portfolio, then monitor what changes.</p>
    <div className="mt-6 grid gap-4 md:grid-cols-3">
      <article className="rounded-lg border border-white/10 bg-slate-950/85 p-5"><h3 className="text-lg font-semibold text-white">Start with ranked stocks</h3><p className="mt-3 text-sm leading-6 text-slate-400">Use the screener to find ideas worth investigating. Open the ticker to review supporting evidence and risks.</p><HomepageCtaLink href={`${appUrl}/screener`} eventName="open_screener_click" className={link}>Open Screener →</HomepageCtaLink></article>
      <article className="rounded-lg border border-white/10 bg-slate-950/85 p-5"><h3 className="text-lg font-semibold text-white">Learn from historical performers</h3><p className="mt-3 text-sm leading-6 text-slate-400">Compare Congress members, insiders and institutions where data and your plan allow. Inspect the dates and disclosed activity behind each track record.</p><HomepageCtaLink href={`${appUrl}/leaderboards#top-stocks`} eventName="see_top_performers_click" className={link}>View Leaderboards →</HomepageCtaLink><HomepageCtaLink href={`${appUrl}/profiles`} eventName="insider_profile_click" className="mt-2 block text-xs text-slate-400 underline underline-offset-4">Explore participant profiles</HomepageCtaLink></article>
      <article className="rounded-lg border border-white/10 bg-slate-950/85 p-5"><h3 className="text-lg font-semibold text-white">Study backtested strategies</h3><p className="mt-3 text-sm leading-6 text-slate-400">Examine the rules, benchmark, measurement period and historical results before deciding whether an approach fits your research.</p><HomepageCtaLink href={`${appUrl}/strategies`} eventName="strategy_click" className={link}>Explore Strategies →</HomepageCtaLink></article>
    </div>
    <p className="mt-4 max-w-4xl text-xs leading-5 text-slate-500">Walnut supplies research tools, not a personalized investment portfolio. You decide what to buy or sell. Historical and backtested performance does not guarantee future results.</p>
  </div></section>;
}

export function HomepageMethodology({appUrl}: {appUrl: string}) {
  return <section id="confirmation-score" className={section}><div className="mx-auto max-w-7xl">
    <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Sources and methodology</p>
    <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">See where the evidence comes from.</h2>
    <div className="mt-6 grid gap-5 md:grid-cols-3">
      <div><h3 className="font-semibold text-white">Follow the source</h3><p className="mt-2 text-sm leading-6 text-slate-400">Inspect <a href="/congress-trades" className="underline underline-offset-4">Congress disclosures</a>, <a href="/insider-trading-tracker" className="underline underline-offset-4">SEC Form 4 activity</a>, <a href="/institutional-filings" className="underline underline-offset-4">13F holdings</a> and <a href="/government-contracts" className="underline underline-offset-4">government awards</a> in their research context. Coverage varies by source and plan.</p></div>
      <div><h3 className="font-semibold text-white">Keep the dates in view</h3><p className="mt-2 text-sm leading-6 text-slate-400">A trade date, disclosure date, reporting period and refresh timestamp can describe different moments. Check the underlying date before interpreting a change as new activity.</p></div>
      <div><h3 className="font-semibold text-white">Understand the calculation</h3><p className="mt-2 text-sm leading-6 text-slate-400">The Confirmation Score summarizes available evidence and its alignment. It is not a probability of a positive return. Historical results describe the past.</p></div>
    </div>
    <div className="mt-5 flex flex-wrap gap-x-6 gap-y-3 text-sm font-semibold text-emerald-200"><a href="/stock-confirmation-score" className="underline underline-offset-4">Confirmation Score methodology</a><a href={`${appUrl}/strategies/methodology`} className="underline underline-offset-4">Strategy methodology</a><a href={`${appUrl}/insights`} className="whitespace-nowrap underline underline-offset-4">Explore research briefs →</a></div>
  </div></section>;
}

export function HomepageFaq() {
  return <section id="homepage-faq" className={section}><div className="mx-auto max-w-4xl">
    <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Questions before you start</p>
    <h2 className="mt-3 text-3xl font-semibold text-white">Frequently asked questions</h2>
    <div className="mt-6 divide-y divide-white/10">{[
      ["Does Walnut build or manage my portfolio?", "Walnut helps you find ideas, inspect evidence, compare historical participants and strategies, and monitor changes. You decide what to buy or sell; Walnut does not construct or manage a personalized investment portfolio for you."],
      ["What can I explore for free?", "Start with the three-stock ranking preview and public ticker research. Confirmation Scores, full rankings, deeper datasets and higher limits remain subject to the existing Premium and Pro plans."],
      ["Does a high ranking guarantee a winning investment?", "No. Rankings summarize available evidence; they do not guarantee returns. Historical and hypothetical backtested results are research context, not predictions."],
      ["Where should I start if I already have a stock in mind?", "Search for its ticker, inspect the underlying evidence and risks, then save it to a watchlist to follow relevant changes. Research articles and social posts can link you directly to the relevant ticker or research page."],
    ].map(([question, answer]) => <details key={question} className="py-4"><summary className="cursor-pointer text-sm font-semibold text-white">{question}</summary><p className="mt-3 text-sm leading-6 text-slate-400">{answer}</p></details>)}</div>
    <a href="https://walnutmarkets.com/faq" className={link}>Read all FAQs</a>
  </div></section>;
}
