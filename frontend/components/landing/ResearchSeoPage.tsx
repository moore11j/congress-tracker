import { publicResearchTools } from "@/lib/publicResearchTools";
import { seoLandingPageJsonLd, type SeoLandingPage } from "@/lib/seoLandingPages";
import { WALNUT_REDDIT_URL, WALNUT_X_HANDLE, WALNUT_X_URL } from "@/lib/marketingMetadata";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");

const platformFooterLinks = [
  { label: "Compare Walnut", href: "/compare" },
  { label: "Stock Research Software", href: "/stock-research-software" },
  { label: "Stock Analysis Platform", href: "/stock-analysis-platform" },
  { label: "Stock Analysis Tools", href: "/stock-analysis-tools" },
  { label: "Stock Screener", href: `${appUrl}/screener` },
  { label: "Compare Stocks", href: `${appUrl}/compare/NVDA/MU` },
  { label: "Research Briefs", href: `${appUrl}/insights` },
  { label: "Pricing", href: "/pricing" },
] as const;

const researchDataFooterLinks = [
  { label: "Congress Trades", href: "/congress-trades" },
  { label: "Insider Trading", href: "/insider-trading-tracker" },
  { label: "Insider Analysis Software", href: "/insider-trading-analysis-software" },
  { label: "Alternative Data", href: "/alternative-data-stock-analysis" },
  { label: "Government Contracts", href: "/government-contracts" },
  { label: "Institutional Filings", href: "/institutional-filings" },
  { label: "Institutional Activity", href: "/institutional-activity-tracker" },
  { label: "Confirmation Score", href: "/stock-confirmation-score" },
  { label: "Macro Positioning", href: `${appUrl}/insights#macro-positioning` },
] as const;

const companyFooterLinks = [
  { label: "About", href: "/about" },
  { label: "FAQ", href: "/faq" },
  { label: "Contact", href: "mailto:support@walnutmarkets.com" },
  { label: "Terms", href: "/terms" },
  { label: "Privacy", href: "/privacy" },
] as const;

export function ResearchSeoPage({ page }: { page: SeoLandingPage }) {
  const structuredData = seoLandingPageJsonLd(page);

  return (
    <main className="min-h-screen bg-[#030712] text-slate-100">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(structuredData).replace(/</g, "\\u003c") }} />
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
          <nav className="hidden items-center gap-4 text-sm font-medium text-slate-300 md:flex" aria-label="Research pages">
            <a href="/stock-analysis-tools" className="hover:text-white">
              Analysis tools
            </a>
            <a href="/congress-trades" className="hover:text-white">
              Congress
            </a>
            <a href="/insider-trading-tracker" className="hover:text-white">
              Insiders
            </a>
          </nav>
          <a href={appUrl} className="rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
            Launch Terminal
          </a>
        </div>
      </header>

      <section className="border-b border-white/10 px-4 py-14 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-6xl">
          <nav className="mb-8 text-sm text-slate-400" aria-label="Breadcrumb">
            <a href="/" className="hover:text-white">
              Walnut Markets
            </a>
            <span className="mx-2 text-slate-600">/</span>
            <span className="text-slate-300">{page.breadcrumbLabel}</span>
          </nav>

          <div className="grid gap-10 lg:grid-cols-[0.95fr_1.05fr] lg:items-start">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{page.eyebrow}</p>
              <h1 className="mt-4 text-4xl font-semibold leading-tight text-white sm:mt-5 sm:text-5xl">{page.h1}</h1>
              <p className="mt-5 max-w-3xl text-base leading-7 text-slate-300 sm:mt-6 sm:text-lg">{page.intro}</p>
              <div className="mt-6 flex flex-col gap-3 sm:mt-7 sm:flex-row">
                <a href={page.primaryCta.href} className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
                  {page.primaryCta.label}
                </a>
                {page.secondaryCta ? (
                  <a href={page.secondaryCta.href} className="inline-flex items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-5 py-3 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:bg-white/[0.06]">
                    {page.secondaryCta.label}
                  </a>
                ) : null}
              </div>
            </div>

            <aside className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">What Walnut helps you inspect</p>
              <div className="mt-5 grid gap-3">
                {page.highlights.map((item) => (
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
        <div className="mx-auto grid max-w-6xl gap-8 lg:grid-cols-[1fr_280px]">
          <div className="space-y-6">
            {page.sections.map((section) => (
              <section key={section.title} className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
                <h2 className="text-xl font-semibold text-white">{section.title}</h2>
                {section.paragraphs?.length ? (
                  <div className="mt-4 space-y-3 text-sm leading-7 text-slate-300">
                    {section.paragraphs.map((paragraph) => (
                      <p key={paragraph}>{paragraph}</p>
                    ))}
                  </div>
                ) : null}
                {section.cards?.length ? (
                  <div className="mt-5 grid gap-3 sm:grid-cols-2">
                    {section.cards.map((card) => {
                      const content = (
                        <>
                          {card.label ? <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">{card.label}</p> : null}
                          <h3 className={card.label ? "mt-3 text-base font-semibold text-white" : "text-base font-semibold text-white"}>{card.title}</h3>
                          <p className="mt-3 text-sm leading-6 text-slate-400">{card.body}</p>
                        </>
                      );
                      return card.href ? (
                        <a key={`${section.title}-${card.title}`} href={card.href} className="rounded-lg border border-white/10 bg-slate-950/70 p-4 transition hover:border-emerald-300/35 hover:bg-white/[0.045]">
                          {content}
                        </a>
                      ) : (
                        <article key={`${section.title}-${card.title}`} className="rounded-lg border border-white/10 bg-slate-950/70 p-4">
                          {content}
                        </article>
                      );
                    })}
                  </div>
                ) : null}
                {section.title === "Popular ticker examples" && page.popularTickers ? (
                  <div className="mt-5 flex flex-wrap gap-2">
                    {page.popularTickers.map((ticker) => (
                      <a key={ticker} href={`${appUrl}/ticker/${ticker}`} className="rounded-lg border border-emerald-300/25 bg-emerald-300/10 px-3 py-2 font-mono text-sm font-semibold text-emerald-100 hover:bg-emerald-300/15">
                        {ticker}
                      </a>
                    ))}
                  </div>
                ) : null}
              </section>
            ))}

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
          </div>

          <aside className="h-fit rounded-lg border border-white/10 bg-white/[0.035] p-5 lg:sticky lg:top-6">
            <p className="text-sm font-semibold text-white">Research tools</p>
            <nav className="mt-4 grid gap-2 text-sm" aria-label="Research tools">
              {publicResearchTools.map((tool) => (
                <a key={tool.href} href={tool.href} className="rounded-lg border border-white/10 bg-slate-950/70 px-3 py-2 text-slate-300 transition hover:border-emerald-300/35 hover:text-white">
                  {tool.label}
                </a>
              ))}
            </nav>
          </aside>
        </div>
      </section>

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
            <nav className="grid content-start gap-2" aria-label="Platform footer">
              <p className="font-semibold text-slate-300">Platform</p>
              {platformFooterLinks.map((link) => (
                <a key={link.href} href={link.href} className="hover:text-white">
                  {link.label}
                </a>
              ))}
            </nav>
            <nav className="grid content-start gap-2" aria-label="Research data footer">
              <p className="font-semibold text-slate-300">Research data</p>
              {researchDataFooterLinks.map((link) => (
                <a key={link.href} href={link.href} className="hover:text-white">
                  {link.label}
                </a>
              ))}
            </nav>
            <nav className="grid content-start gap-2" aria-label="Company footer">
              <p className="font-semibold text-slate-300">Company</p>
              {companyFooterLinks.map((link) => (
                <a key={link.href} href={link.href} className="hover:text-white">
                  {link.label}
                </a>
              ))}
            </nav>
          </div>
        </div>
      </footer>
    </main>
  );
}
