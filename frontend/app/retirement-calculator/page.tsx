import { headers } from "next/headers";
import { MarketingHeader } from "@/components/landing/MarketingHeader";
import { RetirementCalculator } from "@/components/tools/RetirementCalculator";
import { marketingSeoPageMetadata } from "@/lib/marketingMetadata";

const title = "Retirement Calculator & Investment Growth Planner | Walnut Markets";
const description = "Free retirement calculator for individuals and couples. Project monthly investment growth, spouse savings, retirement withdrawals, and inflation with charts and yearly tables.";
export const metadata = marketingSeoPageMetadata("/retirement-calculator", { title, description });

const faqs = [
  { question: "Can I calculate retirement savings with a spouse?", answer: "Yes. Enter separate opening balances, monthly savings, current ages, retirement ages, and retirement income targets. The calculator shows each person's balance at their retirement and the combined balance when both have retired." },
  { question: "How are investment growth and retirement withdrawals calculated?", answer: "The calculator compounds an effective annual return monthly and adds savings at month end. Each person's contributions stop at retirement. Withdrawals then begin, adjusted for inflation and a simplified withdrawal tax rate, using your separate retirement return assumption." },
  { question: "Can I use a historical portfolio return?", answer: "You can search Walnut strategies, Congress members, insiders, or institutions and import an available annualized historical return. Some data requires an eligible account. The source and period are shown before applying it. Historical returns are illustrative assumptions, not predictions." },
  { question: "Can I see a yearly retirement table and export it?", answer: "Switch between Chart and Table to review balances, contributions, investment growth, withdrawals, and income shortfalls. Download the selected period as CSV, in future dollars or inflation-adjusted today's dollars." },
];

export default async function RetirementCalculatorPage() {
  const isMarketing = (await headers()).get("x-walnut-public-landing") === "1";
  return <>{isMarketing ? <MarketingHeader /> : null}<div className="mx-auto max-w-[1600px] px-1 py-6 sm:px-4 sm:py-8">
    <div className="mb-7 max-w-3xl"><p className="text-xs font-semibold uppercase tracking-[.22em] text-emerald-300">Walnut tools · plan your future</p><h1 className="mt-3 text-3xl font-semibold tracking-tight text-white sm:text-4xl">Retirement & Investment Growth Calculator</h1><p className="mt-3 text-sm leading-6 text-slate-400 sm:text-base">See what your savings could become. Build a plan for you and your partner, explore portfolio-inspired returns, and follow your investments from the first contribution through retirement.</p></div>
    <RetirementCalculator startYear={new Date().getFullYear()} />
    <section className="mt-8 max-w-4xl"><h2 className="text-xl font-semibold text-white">Retirement calculator questions</h2><div className="mt-4 divide-y divide-white/10">{faqs.map((faq) => <details key={faq.question} className="py-4"><summary className="cursor-pointer text-sm font-medium text-slate-200">{faq.question}</summary><p className="mt-3 text-sm leading-6 text-slate-400">{faq.answer}</p></details>)}</div><a href="/stock-analysis-tools" className="mt-4 inline-block text-sm text-emerald-200">Explore more Walnut tools →</a></section>
    <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify({ "@context": "https://schema.org", "@graph": [{ "@type": "WebApplication", name: "Walnut Retirement & Investment Growth Calculator", url: "https://walnutmarkets.com/retirement-calculator", applicationCategory: "FinanceApplication", operatingSystem: "Any", description, offers: { "@type": "Offer", price: "0", priceCurrency: "USD" } }, { "@type": "FAQPage", mainEntity: faqs.map((faq) => ({ "@type": "Question", name: faq.question, acceptedAnswer: { "@type": "Answer", text: faq.answer } })) }] }).replace(/</g, "\\u003c") }} />
  </div></>;
}
