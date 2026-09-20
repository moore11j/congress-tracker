import { headers } from "next/headers";
import { MarketingHeader } from "@/components/landing/MarketingHeader";
import { OptionsCalculator } from "@/components/tools/OptionsCalculator";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";

const description = "Free options profit calculator: build calls, puts, spreads, covered calls and iron condors. Explore payoff charts, break-even prices, Greeks and price-time scenarios.";
export const metadata = marketingSeoPageMetadata("/options-calculator", { title: "Options Profit Calculator & Strategy Builder | Walnut Markets", description });

export default async function OptionsCalculatorPage() {
  const marketing = (await headers()).get("x-walnut-public-landing") === "1";
  const now = new Date(), expiry = new Date(now);
  expiry.setUTCDate(expiry.getUTCDate() + 30);
  expiry.setUTCDate(expiry.getUTCDate() + (5 - expiry.getUTCDay() + 7) % 7);
  return <>{marketing ? <MarketingHeader /> : null}<div className="mx-auto max-w-[1600px] px-1 py-6 sm:px-4 sm:py-8">
    <div className="mb-7 max-w-3xl"><p className="text-xs font-semibold uppercase tracking-[.22em] text-emerald-300">Walnut tools · explore the possibilities</p><h1 className="mt-3 text-3xl font-semibold tracking-tight text-white sm:text-4xl">Options Profit Calculator</h1><p className="mt-3 text-sm leading-6 text-slate-400 sm:text-base">Turn a market outlook into a strategy you can understand. Build your position, see the trade-offs, and explore how price, time, and volatility change the outcome.</p></div>
    <OptionsCalculator today={now.toISOString().slice(0, 10)} initialExpiry={expiry.toISOString().slice(0, 10)} />
    <section className="mt-8 max-w-3xl space-y-4 text-sm leading-6 text-slate-400"><h2 className="text-xl font-semibold text-white">Options calculator questions</h2><details><summary className="cursor-pointer font-medium text-slate-200">Does this use live options prices?</summary><p className="mt-2">No. Listed dates and strikes come from Massive. Historical closing trades load from the configured provider: Alpaca in batches, or Massive individually. Each actual price shows its source and date, and may be older than the previous session. Missing prices remain labeled estimates. Greeks use your assumptions.</p></details><details><summary className="cursor-pointer font-medium text-slate-200">Can I calculate multi-leg options strategies?</summary><p className="mt-2">Yes. Build up to six standard option legs with one underlying and expiration, plus a stock holding. Start from spreads, covered calls, long calls and puts, straddles, strangles, or an iron condor.</p></details><details><summary className="cursor-pointer font-medium text-slate-200">Why does the scenario differ from the expiration payoff?</summary><p className="mt-2">Before expiration, options may retain time value. The scenario uses a European pricing model with your volatility, rate and dividend assumptions. At expiration, profit or loss uses intrinsic value and your actual entered premiums.</p></details><a href="/stock-analysis-tools" className="inline-block text-emerald-200">Explore more Walnut tools →</a></section>
    <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify({ "@context": "https://schema.org", "@type": "WebApplication", name: "Walnut Options Profit Calculator", url: "https://walnutmarkets.com/options-calculator", applicationCategory: "FinanceApplication", operatingSystem: "Any", description, offers: { "@type": "Offer", price: "0", priceCurrency: "USD" } }).replace(/</g, "\\u003c") }} />
  </div></>;
}
