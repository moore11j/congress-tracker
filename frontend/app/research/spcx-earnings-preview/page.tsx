import type { Metadata } from "next";
import Link from "next/link";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { CampaignCtaLink } from "@/components/research/CampaignCtaLink";
import { ResearchBriefContextualCta } from "@/components/research/ResearchBriefContextualCta";
import { getResearchBriefBySlug } from "@/lib/researchBriefs";

export const dynamic = "force-static";

const brief = getResearchBriefBySlug("spcx-earnings-preview");
const canonicalUrl = "https://walnutmarkets.com/research/spcx-earnings-preview";
const pageTitle = "SPCX earnings preview: can growth justify its valuation?";
const pageDescription =
  "SpaceX's first public-company earnings report tests Starlink growth, AI capex, Starship progress, launch economics, guidance, and the upcoming share unlock.";

const spcxTerminalHref = "https://app.walnutmarkets.com/ticker/SPCX";
const signupHref = "/login?mode=register&return_to=%2Fresearch%2Fspcx-earnings-preview";
const redditSourceHref = "https://www.reddit.com/r/walnutmarkets/comments/1veiog2/spcx_earnings_preview_can_growth_justify_its/";
const businessInsiderSourceHref = "https://www.businessinsider.com/spacex-stock-spcx-q2-earnings-preview-insider-lockup-expiration-2026-7";
const axiosSourceHref = "https://www.axios.com/2026/08/03/spacex-stock-lockup-earnings";
const investorsSourceHref = "https://www.investors.com/news/spacex-earnings-key-themes-q2-2026-elon-musk-starlink-starship-xai-nasa-falcon9/";

export const metadata: Metadata = {
  title: `${brief?.title ?? pageTitle} | Walnut Markets Research`,
  description: brief?.description ?? pageDescription,
  alternates: {
    canonical: "/research/spcx-earnings-preview",
  },
  openGraph: {
    title: `${pageTitle} | Walnut Markets Research`,
    description: pageDescription,
    url: canonicalUrl,
    siteName: "Walnut Markets",
    type: "article",
  },
  twitter: {
    card: "summary",
    title: `${pageTitle} | Walnut Markets Research`,
    description: pageDescription,
  },
};

const articleSchema = {
  "@context": "https://schema.org",
  "@type": "Article",
  headline: pageTitle,
  description: pageDescription,
  datePublished: "2026-08-03",
  dateModified: "2026-08-03",
  author: {
    "@type": "Organization",
    name: "Walnut Markets",
  },
  publisher: {
    "@type": "Organization",
    name: "Walnut Markets",
  },
  mainEntityOfPage: canonicalUrl,
  about: ["SPCX", "SpaceX", "Starlink", "Starship", "AI infrastructure"],
};

const headlineMetrics = [
  {
    label: "Consensus revenue",
    value: "$6.9B",
    detail: "The setup is less about a clean revenue beat and more about whether guidance supports the growth thesis.",
  },
  {
    label: "Consensus EPS",
    value: "-$0.23",
    detail: "Investors are not expecting surprise profitability, so losses may be tolerated if the outlook improves.",
  },
  {
    label: "Lockup timing",
    value: "Aug. 6",
    detail: "The first post-earnings share unlock is a technical risk that could affect trading even if the quarter is solid.",
  },
  {
    label: "Main debate",
    value: "Growth vs spend",
    detail: "The bull case depends on Starlink, AI, launch, and Starship scaling faster than cash usage worries.",
  },
] as const;

const thesisPillars = [
  "Starlink subscriber growth and margin durability",
  "AI infrastructure revenue, capex, and monetization path",
  "Starship development milestones and payload timeline",
  "Launch cadence, reliability, and unit economics",
  "Management confidence around future free cash flow",
] as const;

const bullCase = [
  "Revenue is expected to grow sharply from the prior quarter, giving management a chance to show scale across multiple business lines.",
  "Starlink remains the most visible earnings engine and the cleanest bridge from space infrastructure to recurring revenue.",
  "AI-related revenue could accelerate meaningfully if customers and compute projects are converting from concept into contracted demand.",
  "The stock's pullback from post-IPO highs has cooled expectations, which may lower the bar for a positive reaction.",
  "Wall Street commentary remains broadly constructive on the long-term opportunity despite near-term volatility.",
] as const;

const bearCase = [
  "AI capex may be rising faster than monetization, increasing pressure on free cash flow and valuation support.",
  "The upcoming lockup expiration could add selling pressure independent of the operating update.",
  "Starlink growth needs to remain strong enough to fund heavier infrastructure and development spending.",
  "Launch dominance is already partly priced in, so investors may focus on incremental margin improvement rather than cadence alone.",
  "A weak guide or vague commentary could keep the stock under pressure after a large post-IPO decline.",
] as const;

const watchRows = [
  ["Revenue vs consensus", "Can SpaceX clear the roughly $6.9B bar and frame growth as durable rather than one-quarter volatility?"],
  ["Starlink momentum", "Subscriber growth, churn, pricing, satellite capacity, and operating profit commentary are the most important recurring-revenue indicators."],
  ["AI economics", "Investors need evidence that AI is becoming a business line, not simply a larger capex line."],
  ["Launch economics", "Cadence matters, but the stronger signal is whether launch margins and reuse economics keep improving."],
  ["Starship roadmap", "The market will listen for payload timing, reliability progress, and cost discipline."],
  ["Lockup response", "Management does not control insider selling, but it can address liquidity, float expansion, and expected volatility clearly."],
] as const;

const scenarioRows = [
  {
    scenario: "Positive",
    signal: "Revenue meets or beats, Starlink remains strong, AI spend is paired with credible monetization, and guidance improves.",
    marketRead: "Could support a confidence reset after the selloff.",
  },
  {
    scenario: "Neutral",
    signal: "Headline numbers are close to consensus, but management gives limited incremental detail on AI, capex, Starlink, or Starship.",
    marketRead: "The stock may remain driven by unlock mechanics and positioning.",
  },
  {
    scenario: "Negative",
    signal: "Revenue misses, capex guidance rises, Starlink growth slows, or management cannot explain the path from spend to cash flow.",
    marketRead: "Could extend the downtrend, especially with the lockup event days away.",
  },
] as const;

const buttonClassName =
  "inline-flex min-h-11 items-center justify-center rounded-lg px-5 py-2.5 text-sm font-semibold transition";
const primaryButtonClassName = `${buttonClassName} bg-emerald-300 text-slate-950 hover:bg-emerald-200`;
const secondaryButtonClassName = `${buttonClassName} border border-white/15 text-slate-100 hover:border-emerald-300/50 hover:text-emerald-100`;

function MetricCard({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <article className="rounded-lg border border-white/10 bg-slate-950/65 p-4">
      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</p>
      <p className="mt-2 text-2xl font-semibold text-white">{value}</p>
      <p className="mt-2 text-sm leading-6 text-slate-400">{detail}</p>
    </article>
  );
}

function BulletPanel({ title, items, tone = "emerald" }: { title: string; items: readonly string[]; tone?: "emerald" | "rose" | "cyan" | "amber" }) {
  const toneClass = {
    emerald: "text-emerald-300",
    rose: "text-rose-300",
    cyan: "text-cyan-300",
    amber: "text-amber-300",
  }[tone];

  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
      <h2 className={`text-xs font-semibold uppercase tracking-[0.2em] ${toneClass}`}>{title}</h2>
      <ul className="mt-4 grid gap-3 text-sm leading-6 text-slate-300">
        {items.map((item) => (
          <li key={item} className="rounded-lg border border-white/10 bg-white/[0.035] px-4 py-3">
            {item}
          </li>
        ))}
      </ul>
    </section>
  );
}

export default function SpcxEarningsPreviewPage() {
  return (
    <main className="-mx-4 -my-1.5 min-h-screen bg-[#06111f] text-slate-100 sm:-mx-6 lg:-mx-8 2xl:-mx-10">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(articleSchema).replace(/</g, "\\u003c") }} />

      <section className="border-b border-white/10 bg-[linear-gradient(180deg,rgba(8,20,35,0.98),rgba(6,17,31,0.94))]">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.95fr] lg:px-8 lg:py-14">
          <div className="flex min-w-0 flex-col justify-center">
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">
              <WalnutBrandMark className="flex h-7 w-7 items-center justify-center rounded-lg border border-emerald-300/30 bg-slate-950" svgClassName="h-5 w-5 overflow-visible" />
              Walnut Earnings Brief
            </div>
            <h1 className="mt-5 max-w-4xl text-4xl font-semibold leading-tight text-white sm:text-5xl">
              SPCX earnings preview: can growth justify its valuation?
            </h1>
            <p className="mt-5 max-w-3xl text-lg leading-8 text-slate-300">
              SpaceX reports its first public-company earnings on August 4, 2026. The market will likely care less about one quarter of losses and more about whether Starlink, AI infrastructure, Starship, and launch economics can support the premium valuation.
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <CampaignCtaLink href={spcxTerminalHref} eventName="view_ticker_spcx_click" className={primaryButtonClassName} properties={{ campaign: "spcx_earnings_preview" }}>
                View SPCX data
              </CampaignCtaLink>
              <CampaignCtaLink href={signupHref} eventName="start_free_click" className={secondaryButtonClassName} properties={{ campaign: "spcx_earnings_preview" }}>
                Start free
              </CampaignCtaLink>
            </div>
            <p className="mt-4 text-xs leading-5 text-slate-500">Research only. Not investment advice. No buy or sell recommendation.</p>
          </div>

          <div className="grid content-start gap-3 sm:grid-cols-2">
            {headlineMetrics.map((metric) => (
              <MetricCard key={metric.label} {...metric} />
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.9fr_1.1fr] lg:px-8">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Why This Quarter Matters</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">SpaceX is not being valued like a traditional aerospace company.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            The stock is being priced as a multi-engine infrastructure platform. That makes the first public earnings report a credibility test: management needs to show that the growth story is scaling and that spending is connected to future cash generation.
          </p>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            If the outlook improves, investors may look through another quarterly loss. If growth slows or capex moves higher without a clearer monetization path, the recent selloff may remain the dominant signal.
          </p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-cyan-300">The Valuation Pillars</p>
          <div className="mt-4 grid gap-2">
            {thesisPillars.map((pillar) => (
              <div key={pillar} className="rounded-lg border border-white/10 bg-white/[0.035] px-4 py-3 text-sm leading-6 text-slate-300">
                {pillar}
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-5 px-4 pb-10 sm:px-6 lg:grid-cols-2 lg:px-8">
        <BulletPanel title="Bull Case" items={bullCase} />
        <BulletPanel title="Bear Case" items={bearCase} tone="rose" />
      </section>

      <section className="border-y border-white/10 bg-slate-950/40">
        <div className="mx-auto max-w-7xl px-4 py-10 sm:px-6 lg:px-8">
          <div className="max-w-3xl">
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-amber-300">What We Are Watching</p>
            <h2 className="mt-3 text-2xl font-semibold text-white">The market may care more about the outlook than the quarter itself.</h2>
            <p className="mt-4 text-sm leading-7 text-slate-400">
              The useful read is whether multiple data points confirm the long-term thesis at the same time: growth, margins, capex discipline, Starlink momentum, AI monetization, and management confidence.
            </p>
          </div>

          <div className="mt-6 overflow-hidden rounded-lg border border-white/10">
            <div className="hidden grid-cols-[0.8fr_1.4fr] border-b border-white/10 bg-white/[0.04] px-4 py-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:grid">
              <span>Focus area</span>
              <span>Why it matters</span>
            </div>
            <div className="divide-y divide-white/10 bg-slate-950/45">
              {watchRows.map(([focus, detail]) => (
                <article key={focus} className="grid gap-3 px-4 py-4 text-sm leading-6 text-slate-300 md:grid-cols-[0.8fr_1.4fr] md:items-start">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Focus area</p>
                    <p className="font-semibold text-white">{focus}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Why it matters</p>
                    <p className="text-slate-400">{detail}</p>
                  </div>
                </article>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.8fr_1.2fr] lg:px-8">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-cyan-300">Scenario Framework</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">Three ways the print can trade.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            Earnings and the share unlock are separate events, but they land close enough together that the market may treat them as one volatility window.
          </p>
        </div>
        <div className="grid gap-3">
          {scenarioRows.map((row) => (
            <article key={row.scenario} className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <h3 className="text-base font-semibold text-white">{row.scenario}</h3>
                <span className="rounded-md border border-white/10 bg-white/[0.04] px-2.5 py-1 text-xs font-semibold text-slate-300">
                  {row.marketRead}
                </span>
              </div>
              <p className="mt-3 text-sm leading-6 text-slate-400">{row.signal}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="border-y border-white/10 bg-slate-950/40">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.9fr] lg:px-8">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Bottom Line</p>
            <h2 className="mt-3 text-2xl font-semibold text-white">SPCX is a high-conviction story with a high-expectation setup.</h2>
            <p className="mt-4 text-sm leading-7 text-slate-400">
              A solid quarter with improving guidance could help rebuild confidence after the post-IPO decline. A miss, weaker guide, or signs that AI spending is running ahead of monetization could extend pressure, especially with the lockup expiration expected days later.
            </p>
            <p className="mt-4 text-sm leading-7 text-slate-400">
              For Walnut, the earnings print is only one data point. The stronger signal will be whether the release, management commentary, capital allocation, Starlink momentum, and post-release market reaction all point in the same direction.
            </p>
          </div>
          <div className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.06] p-5">
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Continue Research</p>
            <h3 className="mt-3 text-lg font-semibold text-white">Use the full Walnut ticker page for SPCX.</h3>
            <p className="mt-3 text-sm leading-6 text-slate-300">
              Review fundamentals, reported insider activity, Congress trades, institutional ownership, government contracts, technicals, and the latest market context as the earnings window develops.
            </p>
            <CampaignCtaLink href={spcxTerminalHref} eventName="view_ticker_spcx_click" className={`${primaryButtonClassName} mt-5`} properties={{ campaign: "spcx_earnings_preview", placement: "bottom_line" }}>
              Open SPCX ticker page
            </CampaignCtaLink>
          </div>
        </div>
      </section>

      <section className="border-t border-white/10 bg-slate-950/30 px-4 py-10 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <ResearchBriefContextualCta ticker="SPCX" companyName="SpaceX" researchSlug="spcx-earnings-preview" />
        </div>
      </section>

      <section className="border-t border-white/10 px-4 py-8 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl text-xs leading-6 text-slate-500">
          Sources:{" "}
          <Link href={redditSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Walnut Markets Reddit brief
          </Link>
          ,{" "}
          <Link href={businessInsiderSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Business Insider earnings and lockup preview
          </Link>
          ,{" "}
          <Link href={axiosSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Axios lockup analysis
          </Link>
          , and{" "}
          <Link href={investorsSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Investor's Business Daily earnings preview
          </Link>
          . Data and expectations referenced as of August 3, 2026. Research only. Not investment advice.
        </div>
      </section>
    </main>
  );
}
