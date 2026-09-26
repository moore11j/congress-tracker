import { HomepageCtaLink } from "@/components/landing/HomepageCtaLink";

const section = "scroll-mt-24 border-b border-white/10 px-4 py-12 sm:px-6 lg:px-8";
const link = "mt-4 inline-flex text-sm font-semibold text-emerald-200 underline underline-offset-4";

export function PortfolioBlueprint({appUrl}: {appUrl: string}) {
  return <section id="whats-working" className={section}><div className="mx-auto max-w-7xl">
    <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Research filtered for you</p>
    <h2 className="mt-3 max-w-3xl text-3xl font-semibold text-white sm:text-4xl">Get the ideas. Understand why. Keep following.</h2>
    <p className="mt-4 max-w-3xl text-base leading-7 text-slate-400">Walnut scans the evidence, ranks the strongest stock ideas and brings them to you. Inspect the supporting research when you want to go deeper, then follow meaningful changes.</p>
    <div className="mt-6 grid gap-4 md:grid-cols-3">
      <article className="rounded-lg border border-white/10 bg-slate-950/85 p-5"><h3 className="text-lg font-semibold text-white">Receive the strongest ideas</h3><p className="mt-3 text-sm leading-6 text-slate-400">Start with your Top 5 and choose a weekly email. Premium adds more ideas and daily delivery.</p><HomepageCtaLink href={`${appUrl}/leaderboards#top-stocks`} eventName="top_stocks_click" className={link}>See Top Stock Ideas →</HomepageCtaLink></article>
      <article className="rounded-lg border border-white/10 bg-slate-950/85 p-5"><h3 className="text-lg font-semibold text-white">See why each idea ranked</h3><p className="mt-3 text-sm leading-6 text-slate-400">Every idea has a short reason and source labels. Premium opens the detailed evidence, conflicts and similar historical setups, subject to source access.</p><HomepageCtaLink href={`${appUrl}/leaderboards#top-stocks`} eventName="see_top_performers_click" className={link}>Explore the research →</HomepageCtaLink></article>
      <article className="rounded-lg border border-white/10 bg-slate-950/85 p-5"><h3 className="text-lg font-semibold text-white">Follow what changes</h3><p className="mt-3 text-sm leading-6 text-slate-400">Save the stocks you care about. Walnut monitors disclosures and source changes; alerts and strategy following depend on your plan.</p><a href={`${appUrl}/watchlists`} className={link}>Save and monitor ideas →</a></article>
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
      ["What can I explore for free?", "Guests preview true ranks #3–#5. A free account unlocks #1–#5 with short reasons and source labels, plus optional weekly Top 5 emails. Premium adds more ideas, daily or weekly delivery and detailed evidence. Pro adds the highest limits and advanced datasets."],
      ["Does a high ranking guarantee a winning investment?", "No. Rankings summarize available evidence; they do not guarantee returns. Historical and hypothetical backtested results are research context, not predictions."],
      ["Where should I start if I already have a stock in mind?", "Search for its ticker, inspect the underlying evidence and risks, then save it to a watchlist to follow relevant changes. Research articles and social posts can link you directly to the relevant ticker or research page."],
    ].map(([question, answer]) => <details key={question} className="py-4"><summary className="cursor-pointer text-sm font-semibold text-white">{question}</summary><p className="mt-3 text-sm leading-6 text-slate-400">{answer}</p></details>)}</div>
    <a href="https://walnutmarkets.com/faq" className={link}>Read all FAQs</a>
  </div></section>;
}
