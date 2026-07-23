import type { Metadata } from "next";
import Link from "next/link";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { CampaignCtaLink } from "@/components/research/CampaignCtaLink";

export const dynamic = "force-static";

const canonicalUrl = "https://walnutmarkets.com/research/nbis-vs-crwv-ai-neoclouds";
const pageTitle = "NBIS vs CRWV: Which AI Neocloud Trade Has Better Risk/Reward?";
const pageDescription =
  "Compare Nebius and CoreWeave across revenue, backlog, margins, debt, capex, Nvidia alignment, and Walnut market data. Research only, not investment advice.";

const nbisTerminalHref = "https://app.walnutmarkets.com/ticker/NBIS";
const crwvTerminalHref = "https://app.walnutmarkets.com/ticker/CRWV";
const signupHref = "/login?mode=register&return_to=%2Fresearch%2Fnbis-vs-crwv-ai-neoclouds";
const nebiusResultsHref = "https://www.sec.gov/Archives/edgar/data/1513845/000110465926059872/tm2614392d1_ex99-1.htm";
const nebiusLetterHref = "https://www.sec.gov/Archives/edgar/data/1513845/000110465926059872/tm2614392d1_ex99-2.htm";
const nebiusAnnualHref = "https://www.sec.gov/Archives/edgar/data/1513845/000110465926052948/nbis-20251231x20f.htm";
const nebiusNvidiaHref = "https://www.sec.gov/Archives/edgar/data/1513845/000110465926026163/tm268532d1_6k.htm";
const nebiusMetaHref = "https://www.sec.gov/Archives/edgar/data/1513845/000110465926027886/tm268879d1_6k.htm";
const coreweaveResultsHref = "https://investors.coreweave.com/news/news-details/2026/CoreWeave-Reports-Strong-First-Quarter-2026-Results/default.aspx";
const coreweaveAnnualHref = "https://www.sec.gov/Archives/edgar/data/1769628/000176962826000104/crwv-20251231.htm";

export const metadata: Metadata = {
  title: "NBIS vs CRWV: AI Neocloud Stock Comparison | Walnut Markets",
  description: pageDescription,
  alternates: {
    canonical: "/research/nbis-vs-crwv-ai-neoclouds",
  },
  openGraph: {
    title: "NBIS vs CRWV: AI Neocloud Stock Comparison | Walnut Markets",
    description: pageDescription,
    url: canonicalUrl,
    siteName: "Walnut Markets",
    type: "article",
    images: [
      {
        url: "/ad-thumbnails/nbis-crwv-neoclouds.png",
        width: 1200,
        height: 628,
        alt: "NBIS vs CRWV AI neocloud comparison thumbnail",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: "NBIS vs CRWV: AI Neocloud Stock Comparison | Walnut Markets",
    description: pageDescription,
    images: ["/ad-thumbnails/nbis-crwv-neoclouds.png"],
  },
};

const articleSchema = {
  "@context": "https://schema.org",
  "@type": "Article",
  headline: pageTitle,
  description: pageDescription,
  datePublished: "2026-07-23",
  dateModified: "2026-07-23",
  author: {
    "@type": "Organization",
    name: "Walnut Markets",
  },
  publisher: {
    "@type": "Organization",
    name: "Walnut Markets",
  },
  mainEntityOfPage: canonicalUrl,
};

const headlineMetrics = [
  {
    label: "NBIS Q1 revenue",
    value: "$399.0M",
    detail: "Reported revenue grew 684% year over year; Nebius AI cloud revenue was $390M.",
  },
  {
    label: "NBIS adjusted EBITDA",
    value: "$129.5M",
    detail: "Roughly 32.5% of consolidated revenue, with 2026 guidance targeting about 40% adjusted EBITDA margin.",
  },
  {
    label: "CRWV Q1 revenue",
    value: "$2.078B",
    detail: "Reported revenue grew roughly 112% year over year from $982M.",
  },
  {
    label: "CRWV backlog",
    value: "$99.4B",
    detail: "Revenue backlog disclosed as of March 31, 2026, including RPO plus committed-contract estimates.",
  },
] as const;

const comparisonRows = [
  ["Latest quarter revenue", "Q1 2026: $399.0M, +684% YoY", "Q1 2026: $2.078B, +112% YoY"],
  ["Adjusted EBITDA", "Q1 2026: $129.5M", "Q1 2026: $1.157B"],
  ["Adjusted EBITDA margin", "About 32.5% of Q1 revenue", "56% in Q1 2026"],
  ["GAAP net income / loss", "Net income from continuing operations of $621.2M, helped by investment revaluation gains; adjusted net loss was $100.3M", "Net loss of $740M"],
  ["Backlog / RPO", "Specific backlog not disclosed in the Q1 release; disclosed Microsoft and Meta contracts are multi-year and material", "Revenue backlog of $99.4B"],
  ["Cash / debt", "Cash and equivalents of $9.3B; current and non-current debt of about $8.45B", "Cash and equivalents of $2.244B; current and non-current debt of about $24.86B"],
  ["Capex / infrastructure", "Q1 PPE and intangible purchases of $2.473B; secured up to 1.2 GW of power and land in Pennsylvania", "Q1 PPE purchases of $7.695B; active power above 1 GW and contracted power above 3.5 GW"],
  ["Customer concentration", "2025 revenue included customers at 25% and 15%; 2026 disclosed long-term contracts with Microsoft and Meta", "2025 10-K disclosed Microsoft at about 67% of revenue"],
  ["Nvidia relationship", "$2B Nvidia private placement and broader strategic collaboration disclosed in March 2026", "$2B Nvidia Class A investment and expanded relationship to build more than 5 GW of AI factories by 2030"],
  ["Walnut confirmation score", "54, moderate bearish, 3-source bearish confirmation", "57, moderate bearish, 3-source bearish confirmation"],
  ["Walnut price / volume", "Bearish tape confirmation; latest close $218.22, volume 0.81x 30D average, RSI near neutral, MACD bearish crossover", "Bearish tape confirmation; latest close $82.63, volume 0.55x 30D average, RSI below neutral, MACD bearish crossover"],
  ["Reported insider / Congress activity in Walnut", "Reported insider activity active: 0 buys / 14 sells; no qualifying Congress trades in the 30-day context window", "Reported insider activity active: 0 buys / 200 sells; no qualifying Congress trades in the 30-day context window"],
] as const;

const nbisBull = [
  "Nebius is growing from a smaller base, but the latest reported growth rate is much faster: Q1 revenue rose 684% year over year to $399.0M.",
  "Adjusted EBITDA was positive at $129.5M, and management said it remained on track for roughly 40% adjusted EBITDA margin in 2026.",
  "The balance sheet has more visible flexibility than CRWV on the current data: $9.3B of cash and equivalents against about $8.45B of debt at quarter end.",
  "Nvidia alignment is explicit through a $2B private placement and strategic collaboration tied to AI cloud expansion.",
] as const;

const nbisBear = [
  "The scale gap is large. CoreWeave's latest quarterly revenue is more than five times Nebius' consolidated Q1 revenue.",
  "Large hyperscaler contracts can create execution risk, service-level commitments, and customer concentration risk as deployments ramp.",
  "Q1 capex was heavy for the size of the business, and new owned sites increase delivery and financing complexity.",
  "Walnut currently shows bearish price/volume confirmation and reported insider selling activity in the 30-day context window.",
] as const;

const crwvBull = [
  "CoreWeave has the cleaner scale argument: $2.078B of Q1 revenue, $1.157B of adjusted EBITDA, and $99.4B of revenue backlog.",
  "The company disclosed major AI customer wins, including Meta and Anthropic, plus existing enterprise and AI-native customer expansions.",
  "Infrastructure scale is real: CoreWeave reported more than 1 GW of active power and more than 3.5 GW of contracted power.",
  "Nvidia alignment is also explicit, including a $2B Class A investment and expanded relationship to build more than 5 GW of AI factories by 2030.",
] as const;

const crwvBear = [
  "The debt and capex burden is the central risk: about $24.86B of current and non-current debt and $7.695B of Q1 PPE purchases.",
  "Customer concentration remains material. The 2025 10-K disclosed Microsoft at about 67% of revenue.",
  "Adjusted EBITDA is strong, but GAAP losses and interest expense still matter; Q1 net loss was $740M and net interest expense was $536M.",
  "Walnut currently shows stronger bearish tape confirmation than NBIS and reported insider selling activity in the 30-day context window.",
] as const;

const watchItems = [
  "next earnings reports",
  "revenue growth versus guidance",
  "adjusted EBITDA margin",
  "customer concentration",
  "capex and financing needs",
  "Nvidia relationship",
  "reported institutional activity when available",
  "price/volume confirmation",
  "Walnut confirmation score trend",
] as const;

const buttonClassName =
  "inline-flex min-h-11 items-center justify-center rounded-lg px-5 py-2.5 text-sm font-semibold transition";
const primaryButtonClassName = `${buttonClassName} bg-emerald-300 text-slate-950 hover:bg-emerald-200`;
const secondaryButtonClassName = `${buttonClassName} border border-white/15 text-slate-100 hover:border-emerald-300/50 hover:text-emerald-100`;

function BulletSection({ title, items, tone = "emerald" }: { title: string; items: readonly string[]; tone?: "emerald" | "rose" | "cyan" | "amber" }) {
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

export default function NbisCrwvAiNeocloudsPage() {
  return (
    <main className="-mx-4 -my-1.5 min-h-screen bg-[#06111f] text-slate-100 sm:-mx-6 lg:-mx-8 2xl:-mx-10">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(articleSchema).replace(/</g, "\\u003c") }} />

      <section className="border-b border-white/10 bg-[linear-gradient(180deg,rgba(8,20,35,0.98),rgba(6,17,31,0.94))]">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.95fr] lg:px-8 lg:py-14">
          <div className="flex min-w-0 flex-col justify-center">
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">
              <WalnutBrandMark className="flex h-7 w-7 items-center justify-center rounded-lg border border-emerald-300/30 bg-slate-950" svgClassName="h-5 w-5 overflow-visible" />
              Walnut DD Brief
            </div>
            <h1 className="mt-5 max-w-4xl text-4xl font-semibold leading-tight text-white sm:text-5xl">{pageTitle}</h1>
            <p className="mt-5 max-w-3xl text-lg leading-8 text-slate-300">
              The AI infrastructure trade is getting more selective. CRWV has scale and backlog. NBIS may offer a cleaner risk/reward if the market starts rewarding profitability, balance sheet flexibility, and Nvidia alignment.
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <CampaignCtaLink href={nbisTerminalHref} eventName="view_ticker_nbis_click" className={primaryButtonClassName} properties={{ campaign: "nbis_vs_crwv_research" }}>
                View NBIS data
              </CampaignCtaLink>
              <CampaignCtaLink href={crwvTerminalHref} eventName="view_ticker_crwv_click" className={secondaryButtonClassName} properties={{ campaign: "nbis_vs_crwv_research" }}>
                View CRWV data
              </CampaignCtaLink>
              <CampaignCtaLink href={signupHref} eventName="start_free_click" className={secondaryButtonClassName} properties={{ campaign: "nbis_vs_crwv_research" }}>
                Start free
              </CampaignCtaLink>
            </div>
            <p className="mt-4 text-xs leading-5 text-slate-500">Research only. Not investment advice. No buy or sell recommendation.</p>
          </div>

          <div className="grid content-start gap-3 sm:grid-cols-2">
            {headlineMetrics.map((metric) => (
              <article key={metric.label} className="rounded-lg border border-white/10 bg-slate-950/65 p-4">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">{metric.label}</p>
                <p className="mt-2 text-2xl font-semibold text-white">{metric.value}</p>
                <p className="mt-2 text-sm leading-6 text-slate-400">{metric.detail}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.9fr_1.1fr] lg:px-8">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">The Situation</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">AI neocloud names are moving from narrative to proof.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            AI infrastructure stocks have been volatile because investors are trying to separate real demand from hype. The market is no longer giving every AI infrastructure stock a free pass.
          </p>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            The relevant questions are scale, backlog quality, profitability, customer concentration, debt, capex discipline, Nvidia alignment, and whether demand stays durable.
          </p>
        </div>
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-cyan-300">The Issue</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">CRWV is larger. NBIS may be cleaner.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            CRWV has the larger revenue base and the huge backlog. NBIS is smaller, but the latest data shows a profitability inflection, a large cash balance, and explicit Nvidia support.
          </p>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            The risk is not theoretical. Both companies need heavy infrastructure buildouts, large customers, GPU supply, power access, and demand durability to convert AI infrastructure appetite into durable economics.
          </p>
        </div>
      </section>

      <section className="border-y border-white/10 bg-slate-950/40">
        <div className="mx-auto max-w-7xl px-4 py-10 sm:px-6 lg:px-8">
          <div className="max-w-3xl">
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-amber-300">Data Comparison</p>
            <h2 className="mt-3 text-2xl font-semibold text-white">The table favors CRWV on scale and NBIS on balance sheet flexibility.</h2>
          </div>
          <div className="mt-6 overflow-hidden rounded-lg border border-white/10">
            <div className="hidden grid-cols-[0.9fr_1.1fr_1.1fr] border-b border-white/10 bg-white/[0.04] px-4 py-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:grid">
              <span>Metric</span>
              <span>NBIS / Nebius</span>
              <span>CRWV / CoreWeave</span>
            </div>
            <div className="divide-y divide-white/10 bg-slate-950/45">
              {comparisonRows.map(([metric, nbis, crwv]) => (
                <article key={metric} className="grid gap-3 px-4 py-4 text-sm leading-6 text-slate-300 md:grid-cols-[0.9fr_1.1fr_1.1fr] md:items-start">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Metric</p>
                    <p className="font-semibold text-white">{metric}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">NBIS / Nebius</p>
                    <p>{nbis}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">CRWV / CoreWeave</p>
                    <p>{crwv}</p>
                  </div>
                </article>
              ))}
            </div>
          </div>
          <p className="mt-4 text-xs leading-5 text-slate-500">
            Walnut market data reflects the production API response generated July 23, 2026 UTC with underlying quote and price/volume data through July 22, 2026.
          </p>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-5 px-4 py-10 sm:px-6 lg:grid-cols-2 lg:px-8">
        <BulletSection title="NBIS Bull Case" items={nbisBull} />
        <BulletSection title="NBIS Bear Case" items={nbisBear} tone="rose" />
        <BulletSection title="CRWV Bull Case" items={crwvBull} tone="cyan" />
        <BulletSection title="CRWV Bear Case" items={crwvBear} tone="rose" />
      </section>

      <section className="border-y border-white/10 bg-slate-950/40">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.85fr_1.15fr] lg:px-8">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Walnut Data</p>
            <h2 className="mt-3 text-2xl font-semibold text-white">Confirmation score is separate from the underlying data.</h2>
            <p className="mt-4 text-sm leading-7 text-slate-400">
              Walnut's confirmation score is a proprietary summary. It should not be blended with the public company data above. The underlying Walnut data here is price/volume, fundamentals, reported insider activity, and reported Congress activity.
            </p>
            <p className="mt-4 text-sm leading-7 text-slate-400">
              Reported institutional activity, options flow, and broader signal rows were locked or unavailable in the public production response used for this page, so they are not used as support.
            </p>
          </div>
          <div className="grid gap-3">
            <article className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
              <h3 className="text-base font-semibold text-white">NBIS</h3>
              <p className="mt-2 text-sm leading-6 text-slate-400">
                Confirmation score 54, moderate bearish. Underlying data: bearish tape confirmation, fundamental strength, reported insider activity active with 0 buys / 14 sells, and no qualifying Congress trades in the 30-day context window.
              </p>
            </article>
            <article className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
              <h3 className="text-base font-semibold text-white">CRWV</h3>
              <p className="mt-2 text-sm leading-6 text-slate-400">
                Confirmation score 57, moderate bearish. Underlying data: stronger bearish tape confirmation, mixed fundamental profile, reported insider activity active with 0 buys / 200 sells, and no qualifying Congress trades in the 30-day context window.
              </p>
            </article>
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.9fr] lg:px-8">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-amber-300">The Call</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">CRWV for scale. NBIS for cleaner risk/reward.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            CoreWeave has the stronger scale case: larger revenue, massive backlog, large customer wins, and clear Nvidia alignment. That is the cleanest argument for CRWV.
          </p>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            Nebius has the cleaner risk/reward if the market starts rewarding balance sheet flexibility, profitability inflection, and Nvidia alignment. That view depends on execution, continued demand, and whether price/volume and reported activity improve from the current Walnut read.
          </p>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            Our call: CRWV for scale. NBIS for cleaner risk/reward.
          </p>
          <p className="mt-4 text-xs leading-5 text-slate-500">This is research only. It is not a buy, sell, or hold recommendation.</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">What To Watch Next</p>
          <ul className="mt-4 grid gap-2 text-sm leading-6 text-slate-300">
            {watchItems.map((item) => (
              <li key={item} className="rounded-lg border border-white/10 bg-white/[0.035] px-4 py-3">
                {item}
              </li>
            ))}
          </ul>
        </div>
      </section>

      <section className="border-t border-white/10 bg-slate-950/30">
        <div className="mx-auto grid max-w-7xl gap-6 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.8fr] lg:px-8">
          <div>
            <h2 className="text-2xl font-semibold text-white">Keep the comparison current inside Walnut.</h2>
            <p className="mt-3 text-sm leading-7 text-slate-400">
              Re-check the ticker data as earnings, filings, reported activity, price/volume confirmation, and confirmation scores change.
            </p>
          </div>
          <div className="flex flex-wrap items-start gap-3 lg:justify-end">
            <CampaignCtaLink href={nbisTerminalHref} eventName="view_ticker_nbis_click" className={primaryButtonClassName} properties={{ campaign: "nbis_vs_crwv_research", placement: "footer" }}>
              View NBIS data
            </CampaignCtaLink>
            <CampaignCtaLink href={crwvTerminalHref} eventName="view_ticker_crwv_click" className={secondaryButtonClassName} properties={{ campaign: "nbis_vs_crwv_research", placement: "footer" }}>
              View CRWV data
            </CampaignCtaLink>
            <CampaignCtaLink href={signupHref} eventName="start_free_click" className={secondaryButtonClassName} properties={{ campaign: "nbis_vs_crwv_research", placement: "footer" }}>
              Start free
            </CampaignCtaLink>
          </div>
        </div>
      </section>

      <section className="border-t border-white/10 px-4 py-8 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl text-xs leading-6 text-slate-500">
          Sources:{" "}
          <Link href={nebiusResultsHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Nebius Q1 2026 results
          </Link>
          ,{" "}
          <Link href={nebiusLetterHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Nebius Q1 2026 shareholder letter
          </Link>
          ,{" "}
          <Link href={nebiusAnnualHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Nebius 2025 Form 20-F
          </Link>
          ,{" "}
          <Link href={nebiusNvidiaHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Nebius Nvidia 6-K
          </Link>
          ,{" "}
          <Link href={nebiusMetaHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Nebius Meta 6-K
          </Link>
          ,{" "}
          <Link href={coreweaveResultsHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            CoreWeave Q1 2026 results
          </Link>
          , and{" "}
          <Link href={coreweaveAnnualHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            CoreWeave 2025 Form 10-K
          </Link>
          . Walnut market data was queried from the production API on July 23, 2026 UTC. Research only. Not investment advice.
        </div>
      </section>
    </main>
  );
}
