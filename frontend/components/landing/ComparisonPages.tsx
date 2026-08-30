import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import {
  appUrl,
  comparisonCheckedOn,
  comparisonHubJsonLd,
  comparisonPageJsonLd,
  comparisonPageList,
  comparisonPath,
  comparisonScreenshot,
  type CompetitorComparisonPage,
} from "@/lib/comparisonPages";
import { WALNUT_REDDIT_URL, WALNUT_X_HANDLE, WALNUT_X_URL } from "@/lib/marketingMetadata";

const headerLinks = [
  { label: "Comparisons", href: "/compare" },
  { label: "Analysis tools", href: "/stock-analysis-tools" },
  { label: "Pricing", href: `${appUrl}/pricing` },
] as const;

const footerPlatformLinks = [
  { label: "Compare Walnut", href: "/compare" },
  { label: "Stock Analysis Tools", href: "/stock-analysis-tools" },
  { label: "Stock Screener", href: `${appUrl}/screener` },
  { label: "Compare Stocks", href: `${appUrl}/compare/NVDA/MU` },
  { label: "Pricing", href: `${appUrl}/pricing` },
] as const;

const footerResearchLinks = [
  { label: "Congress Trades", href: "/congress-trades" },
  { label: "Insider Trading", href: "/insider-trading-tracker" },
  { label: "Government Contracts", href: "/government-contracts" },
  { label: "Institutional Filings", href: "/institutional-filings" },
  { label: "Confirmation Score", href: "/stock-confirmation-score" },
] as const;

const footerCompanyLinks = [
  { label: "About", href: `${appUrl}/about` },
  { label: "FAQ", href: `${appUrl}/faq` },
  { label: "Contact", href: "mailto:support@walnutmarkets.com" },
  { label: "Terms", href: `${appUrl}/terms` },
  { label: "Privacy", href: `${appUrl}/privacy` },
] as const;

const primaryButtonClassName = "inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200";
const secondaryButtonClassName = "inline-flex items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-5 py-3 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:bg-white/[0.06]";

function JsonLd({ data }: { data: unknown }) {
  return <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(data).replace(/</g, "\\u003c") }} />;
}

function Header() {
  return (
    <header className="border-b border-white/10 bg-slate-950/88">
      <div className="mx-auto flex max-w-6xl items-center justify-between gap-4 px-4 py-4 sm:px-6 lg:px-8">
        <a href="/" className="flex min-w-0 items-center gap-3" aria-label="Walnut home">
          <WalnutBrandMark
            className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-emerald-300/35 bg-slate-950 shadow-[0_0_28px_rgba(16,185,129,0.18)]"
            svgClassName="h-6 w-6 overflow-visible"
          />
          <span className="leading-none">
            <span className="block whitespace-nowrap text-base font-semibold text-white">Walnut</span>
            <span className="mt-1 block whitespace-nowrap text-[11px] font-medium text-slate-400">Market Terminal</span>
          </span>
        </a>
        <nav className="hidden items-center gap-4 text-sm font-medium text-slate-300 md:flex" aria-label="Comparison pages">
          {headerLinks.map((link) => (
            <a key={link.href} href={link.href} className="hover:text-white">
              {link.label}
            </a>
          ))}
        </nav>
        <a href={appUrl} className="rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
          Launch Terminal
        </a>
      </div>
    </header>
  );
}

function Footer() {
  return (
    <footer className="border-t border-white/10 px-4 py-8 sm:px-6 lg:px-8">
      <div className="mx-auto grid max-w-6xl gap-8 text-sm text-slate-400 lg:grid-cols-[1.2fr_2fr]">
        <div>
          <p className="font-semibold text-white">Walnut Markets</p>
          <p className="mt-3 max-w-sm text-xs leading-5">
            Walnut is a stock research and analysis platform operated by Walnut Intelligence Inc. It is provided for research and informational purposes only and does not provide investment advice.
          </p>
          <div className="mt-4 flex flex-wrap gap-4 text-xs">
            <a href={WALNUT_X_URL} target="_blank" rel="noreferrer" className="hover:text-white">
              X / {WALNUT_X_HANDLE}
            </a>
            <a href={WALNUT_REDDIT_URL} target="_blank" rel="noreferrer" className="hover:text-white">
              Reddit / r/walnutmarkets
            </a>
          </div>
        </div>
        <div className="grid gap-6 sm:grid-cols-3">
          <FooterNav title="Platform" links={footerPlatformLinks} />
          <FooterNav title="Research data" links={footerResearchLinks} />
          <FooterNav title="Company" links={footerCompanyLinks} />
        </div>
      </div>
    </footer>
  );
}

function FooterNav({ title, links }: { title: string; links: readonly { label: string; href: string }[] }) {
  return (
    <nav className="grid content-start gap-2" aria-label={`${title} footer`}>
      <p className="font-semibold text-slate-300">{title}</p>
      {links.map((link) => (
        <a key={link.href} href={link.href} className="hover:text-white">
          {link.label}
        </a>
      ))}
    </nav>
  );
}

function Breadcrumb({ current }: { current?: string }) {
  return (
    <nav className="mb-8 text-sm text-slate-400" aria-label="Breadcrumb">
      <a href="/" className="hover:text-white">
        Walnut Markets
      </a>
      <span className="mx-2 text-slate-600">/</span>
      {current ? (
        <>
          <a href="/compare" className="hover:text-white">
            Compare
          </a>
          <span className="mx-2 text-slate-600">/</span>
          <span className="text-slate-300">{current}</span>
        </>
      ) : (
        <span className="text-slate-300">Compare</span>
      )}
    </nav>
  );
}

export function ComparisonHubPage() {
  return (
    <main className="min-h-screen bg-[#030712] text-slate-100">
      <JsonLd data={comparisonHubJsonLd()} />
      <Header />
      <section className="border-b border-white/10 px-4 py-14 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-6xl">
          <Breadcrumb />
          <div className="grid gap-10 lg:grid-cols-[0.95fr_1.05fr] lg:items-start">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Compare Walnut Markets</p>
              <h1 className="mt-4 text-4xl font-semibold leading-tight text-white sm:text-5xl">Compare stock research platforms without the hand-waving.</h1>
              <p className="mt-5 max-w-3xl text-base leading-7 text-slate-300 sm:text-lg">
                Walnut is built for investors who want to turn price, fundamentals, disclosures, ownership, contracts, and research notes into a clearer read on a stock. These comparisons explain where Walnut fits, where another tool may be better, and when using both makes sense.
              </p>
              <div className="mt-7 flex flex-col gap-3 sm:flex-row">
                <a href={appUrl} className={primaryButtonClassName}>
                  Launch Walnut
                </a>
                <a href={`${appUrl}/pricing`} className={secondaryButtonClassName}>
                  View pricing
                </a>
              </div>
            </div>
            <aside className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">How we compare</p>
              <div className="mt-5 grid gap-3">
                {["Fair about specialist strengths", "No fake dashboards or invented customer quotes", "Current public sources checked on " + comparisonCheckedOn].map((item) => (
                  <div key={item} className="rounded-lg border border-white/10 bg-slate-950/70 px-4 py-3 text-sm font-medium leading-6 text-slate-200">
                    {item}
                  </div>
                ))}
              </div>
            </aside>
          </div>
        </div>
      </section>

      <section className="px-4 py-14 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-6xl">
          <div className="grid gap-4 md:grid-cols-2">
            {comparisonPageList.map((page) => (
              <a key={page.slug} href={comparisonPath(page.slug)} className="rounded-lg border border-white/10 bg-white/[0.035] p-5 transition hover:border-emerald-300/35 hover:bg-white/[0.055]">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">{page.eyebrow}</p>
                <h2 className="mt-3 text-xl font-semibold text-white">{page.h1}</h2>
                <p className="mt-3 text-sm leading-6 text-slate-400">{page.hubDescription}</p>
              </a>
            ))}
          </div>
        </div>
      </section>
      <Footer />
    </main>
  );
}

export function CompetitorComparisonPageView({ page }: { page: CompetitorComparisonPage }) {
  return (
    <main className="min-h-screen bg-[#030712] text-slate-100">
      <JsonLd data={comparisonPageJsonLd(page)} />
      <Header />
      <section className="border-b border-white/10 px-4 py-14 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-6xl">
          <Breadcrumb current={page.competitorName} />
          <div className="grid gap-10 lg:grid-cols-[0.95fr_1.05fr] lg:items-start">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{page.eyebrow}</p>
              <h1 className="mt-4 text-4xl font-semibold leading-tight text-white sm:text-5xl">{page.h1}</h1>
              <p className="mt-5 max-w-3xl text-base leading-7 text-slate-300 sm:text-lg">{page.intro}</p>
              <div className="mt-7 flex flex-col gap-3 sm:flex-row">
                <a href={page.primaryCta.href} className={primaryButtonClassName}>
                  {page.primaryCta.label}
                </a>
                <a href={page.secondaryCta.href} className={secondaryButtonClassName}>
                  {page.secondaryCta.label}
                </a>
              </div>
            </div>
            <aside className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Quick verdict</p>
              <div className="mt-5 grid gap-3">
                <VerdictCard label="Walnut is stronger when" body={page.quickVerdict.walnut} tone="emerald" />
                <VerdictCard label={`${page.competitorName} may be stronger when`} body={page.quickVerdict.competitor} tone="cyan" />
              </div>
            </aside>
          </div>
        </div>
      </section>

      <section className="px-4 py-14 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-6xl gap-8 lg:grid-cols-[1fr_280px]">
          <div className="space-y-6">
            <ComparisonTable page={page} />
            <ProductEvidence />
            <FitSections page={page} />
            <WorkflowDifference page={page} />
            <WalnutDifferentiator page={page} />
            <PlanContext page={page} />
            <FAQ page={page} />
            <FinalCta page={page} />
          </div>
          <AsideNav page={page} />
        </div>
      </section>
      <Footer />
    </main>
  );
}

function VerdictCard({ label, body, tone }: { label: string; body: string; tone: "emerald" | "cyan" }) {
  const toneClass = tone === "emerald" ? "border-emerald-300/20 bg-emerald-300/[0.07] text-emerald-100" : "border-cyan-300/20 bg-cyan-300/[0.06] text-cyan-100";
  return (
    <div className={`rounded-lg border p-4 ${toneClass}`}>
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em]">{label}</p>
      <p className="mt-2 text-sm leading-6 text-slate-200">{body}</p>
    </div>
  );
}

function ComparisonTable({ page }: { page: CompetitorComparisonPage }) {
  return (
    <section className="overflow-hidden rounded-lg border border-white/10 bg-white/[0.035]">
      <div className="border-b border-white/10 p-5">
        <h2 className="text-xl font-semibold text-white">Quick comparison summary</h2>
        <p className="mt-2 text-sm leading-6 text-slate-400">Descriptive values are used where a simple yes/no would be misleading.</p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[720px] text-left text-sm">
          <thead className="bg-slate-950/70 text-xs uppercase tracking-[0.16em] text-slate-500">
            <tr>
              <th className="w-[26%] px-4 py-3 font-semibold">Category</th>
              <th className="w-[37%] px-4 py-3 font-semibold text-emerald-200">Walnut Markets</th>
              <th className="w-[37%] px-4 py-3 font-semibold text-cyan-200">{page.competitorName}</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/10">
            {page.rows.map((row) => (
              <tr key={row.label}>
                <td className="px-4 py-3 font-semibold text-white">{row.label}</td>
                <td className="px-4 py-3 leading-6 text-slate-300">{row.walnut}</td>
                <td className="px-4 py-3 leading-6 text-slate-300">{row.competitor}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ProductEvidence() {
  return (
    <section className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
      <div className="grid gap-6 lg:grid-cols-[0.8fr_1.2fr] lg:items-center">
        <div>
          <h2 className="text-xl font-semibold text-white">Real product evidence</h2>
          <p className="mt-3 text-sm leading-7 text-slate-300">
            The screenshot below is a live Walnut comparison workflow. It is included to show the actual product shape, not a fabricated dashboard or invented performance claim.
          </p>
          <a href={`${appUrl}/compare/NVDA/MU`} className="mt-5 inline-flex text-sm font-semibold text-emerald-200 hover:text-emerald-100">
            Open the live stock comparison
          </a>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950 p-2">
          <img src={comparisonScreenshot} alt="Walnut Compare page showing NVDA versus MU in the live product" className="w-full rounded-md border border-white/10" />
          <p className="mt-3 px-1 text-xs leading-5 text-slate-500">Premium comparison shown. Pro adds institutional and options-related context where available.</p>
        </div>
      </div>
    </section>
  );
}

function FitSections({ page }: { page: CompetitorComparisonPage }) {
  return (
    <section className="grid gap-4 md:grid-cols-2">
      <FitCard title="Who Walnut is best for" items={page.walnutBestFor} tone="emerald" />
      <FitCard title={`Who ${page.competitorName} is best for`} items={page.competitorBestFor} tone="cyan" />
    </section>
  );
}

function FitCard({ title, items, tone }: { title: string; items: string[]; tone: "emerald" | "cyan" }) {
  const eyebrowClass = tone === "emerald" ? "text-emerald-300" : "text-cyan-300";
  return (
    <section className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
      <p className={`text-xs font-semibold uppercase tracking-[0.18em] ${eyebrowClass}`}>Best fit</p>
      <h2 className="mt-3 text-xl font-semibold text-white">{title}</h2>
      <ul className="mt-4 space-y-3 text-sm leading-6 text-slate-300">
        {items.map((item) => (
          <li key={item} className="rounded-lg border border-white/10 bg-slate-950/60 p-3">
            {item}
          </li>
        ))}
      </ul>
    </section>
  );
}

function WorkflowDifference({ page }: { page: CompetitorComparisonPage }) {
  return (
    <section className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
      <h2 className="text-xl font-semibold text-white">{page.workflowTitle}</h2>
      <div className="mt-4 space-y-3 text-sm leading-7 text-slate-300">
        {page.workflowBody.map((paragraph) => (
          <p key={paragraph}>{paragraph}</p>
        ))}
      </div>
    </section>
  );
}

function WalnutDifferentiator({ page }: { page: CompetitorComparisonPage }) {
  return (
    <section className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.055] p-5">
      <h2 className="text-xl font-semibold text-white">Walnut's differentiator</h2>
      <div className="mt-4 grid gap-3 sm:grid-cols-4">
        {["Data", "Interpretation", "Judgment", "Action-ready research"].map((item) => (
          <div key={item} className="rounded-lg border border-emerald-300/20 bg-slate-950/70 px-3 py-3 text-center text-sm font-semibold text-emerald-100">
            {item}
          </div>
        ))}
      </div>
      <div className="mt-4 space-y-3 text-sm leading-7 text-slate-300">
        {page.differentiatorBody.map((paragraph) => (
          <p key={paragraph}>{paragraph}</p>
        ))}
      </div>
    </section>
  );
}

function PlanContext({ page }: { page: CompetitorComparisonPage }) {
  return (
    <section className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
      <h2 className="text-xl font-semibold text-white">Pricing and plan context</h2>
      <p className="mt-3 text-sm leading-7 text-slate-300">{page.planContext}</p>
      <div className="mt-5 rounded-lg border border-white/10 bg-slate-950/70 p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Public sources checked</p>
        <div className="mt-3 space-y-3 text-sm leading-6 text-slate-300">
          {page.facts.map((item) => (
            <div key={item.sourceUrl}>
              <a href={item.sourceUrl} target="_blank" rel="noreferrer" className="font-semibold text-emerald-200 hover:text-emerald-100">
                {item.sourceName}
              </a>
              <span className="text-slate-500"> - checked {item.checkedOn}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function FAQ({ page }: { page: CompetitorComparisonPage }) {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/85 p-5">
      <h2 className="text-xl font-semibold text-white">FAQ</h2>
      <div className="mt-4 divide-y divide-white/10">
        {page.faq.map((item) => (
          <details key={item.question} className="group py-4 first:pt-0 last:pb-0">
            <summary className="flex cursor-pointer list-none items-start justify-between gap-4 text-base font-semibold text-white marker:hidden">
              <span>{item.question}</span>
              <span className="mt-0.5 text-lg leading-none text-emerald-300 transition group-open:rotate-45" aria-hidden="true">
                +
              </span>
            </summary>
            <p className="mt-3 text-sm leading-7 text-slate-300">{item.answer}</p>
          </details>
        ))}
      </div>
    </section>
  );
}

function FinalCta({ page }: { page: CompetitorComparisonPage }) {
  return (
    <section className="rounded-lg border border-white/10 bg-white/[0.035] p-6">
      <h2 className="text-2xl font-semibold text-white">Research your next stock with Walnut</h2>
      <p className="mt-3 max-w-2xl text-sm leading-7 text-slate-300">
        Start with a ticker, then examine what multiple data sources say before your next decision.
      </p>
      <div className="mt-6 flex flex-col gap-3 sm:flex-row">
        <a href={page.primaryCta.href} className={primaryButtonClassName}>
          {page.primaryCta.label}
        </a>
        <a href="/compare" className={secondaryButtonClassName}>
          View all comparisons
        </a>
      </div>
    </section>
  );
}

function AsideNav({ page }: { page: CompetitorComparisonPage }) {
  return (
    <aside className="h-fit rounded-lg border border-white/10 bg-white/[0.035] p-5 lg:sticky lg:top-6">
      <p className="text-sm font-semibold text-white">Related research</p>
      <nav className="mt-4 grid gap-2 text-sm" aria-label="Related research links">
        {page.relatedLinks.map((link) => (
          <a key={`${link.href}-${link.label}`} href={link.href} className="rounded-lg border border-white/10 bg-slate-950/70 px-3 py-2 text-slate-300 transition hover:border-emerald-300/35 hover:text-white">
            <span className="block font-semibold text-slate-100">{link.label}</span>
            <span className="mt-1 block text-xs leading-5 text-slate-500">{link.body}</span>
          </a>
        ))}
      </nav>
      {page.claimsForOwnerReview.length ? (
        <div className="mt-5 rounded-lg border border-amber-300/20 bg-amber-300/10 p-3">
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-amber-100">Review notes</p>
          <ul className="mt-2 space-y-2 text-xs leading-5 text-amber-50">
            {page.claimsForOwnerReview.map((claim) => (
              <li key={claim}>{claim}</li>
            ))}
          </ul>
        </div>
      ) : null}
    </aside>
  );
}
