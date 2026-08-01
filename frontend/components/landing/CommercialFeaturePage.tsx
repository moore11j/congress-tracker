import { CampaignEventOnMount, CampaignTrackedLink } from "@/components/campaign/CampaignAnalytics";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { publicResearchTools } from "@/lib/publicResearchTools";
import { commercialFeaturePageJsonLd, type CommercialFeaturePage as CommercialFeaturePageData } from "@/lib/commercialFeaturePages";
import { WALNUT_REDDIT_URL, WALNUT_X_HANDLE, WALNUT_X_URL } from "@/lib/marketingMetadata";

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");
const productImage = "/landing/compare-nvda-mu-production.png";

const footerPlatformLinks = [
  { label: "Compare Walnut", href: "/compare" },
  { label: "Stock Research Software", href: "/stock-research-software" },
  { label: "Stock Analysis Platform", href: "/stock-analysis-platform" },
  { label: "Stock Screener", href: `${appUrl}/screener` },
  { label: "Pricing", href: "/pricing" },
] as const;

const footerResearchLinks = [
  { label: "Insider Analysis", href: "/insider-trading-analysis-software" },
  { label: "Alternative Data", href: "/alternative-data-stock-analysis" },
  { label: "Institutional Activity", href: "/institutional-activity-tracker" },
  { label: "Congress Trades", href: "/congress-trades" },
  { label: "Government Contracts", href: "/government-contracts" },
] as const;

export function CommercialFeaturePage({ page }: { page: CommercialFeaturePageData }) {
  const structuredData = commercialFeaturePageJsonLd(page);
  const analyticsProperties = { page: page.key, route: page.pathname };

  return (
    <main className="min-h-screen w-screen max-w-[100vw] overflow-x-hidden bg-[#030712] text-slate-100">
      <CampaignEventOnMount eventName="seo_feature_page_view" path={page.pathname} properties={analyticsProperties} />
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
          <nav className="hidden items-center gap-4 text-sm font-medium text-slate-300 md:flex" aria-label="Feature pages">
            <a href="/stock-research-software" className="hover:text-white">
              Research software
            </a>
            <a href="/alternative-data-stock-analysis" className="hover:text-white">
              Alternative data
            </a>
            <a href="/compare" className="hover:text-white">
              Compare
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

          <div className="grid gap-10 lg:grid-cols-[0.9fr_1.1fr] lg:items-center">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{page.eyebrow}</p>
              <h1 className="mt-4 text-3xl font-semibold leading-tight text-white sm:mt-5 sm:text-5xl">{page.h1}</h1>
              <p className="mt-5 max-w-3xl text-base leading-7 text-slate-300 sm:mt-6 sm:text-lg">{page.intro}</p>
              <p className="mt-4 max-w-2xl text-sm leading-6 text-slate-400">{page.targetUser}</p>
              <div className="mt-7 flex flex-col gap-3 sm:flex-row">
                <CampaignTrackedLink
                  href={page.primaryCta.href}
                  className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200"
                  eventName="seo_feature_primary_cta_click"
                  path={page.pathname}
                  properties={analyticsProperties}
                >
                  {page.primaryCta.label}
                </CampaignTrackedLink>
                <CampaignTrackedLink
                  href={page.secondaryCta.href}
                  className="inline-flex items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-5 py-3 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:bg-white/[0.06]"
                  eventName="seo_feature_secondary_cta_click"
                  path={page.pathname}
                  properties={analyticsProperties}
                >
                  {page.secondaryCta.label}
                </CampaignTrackedLink>
              </div>
            </div>

            <figure className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/85 shadow-2xl shadow-black/35">
              <img src={productImage} alt={page.imageAlt} width={1440} height={980} className="h-auto w-full" />
              <figcaption className="border-t border-white/10 px-4 py-3 text-xs leading-5 text-slate-400">
                Real Walnut interface capture. Data visibility depends on plan access, ticker coverage, and source availability.
              </figcaption>
            </figure>
          </div>
        </div>
      </section>

      <section className="px-4 py-14 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-6xl gap-8 lg:grid-cols-[1fr_300px]">
          <div className="space-y-6">
            <section className="grid gap-3 sm:grid-cols-3">
              {page.highlights.map((item) => (
                <article key={item} className="rounded-lg border border-white/10 bg-white/[0.035] p-4">
                  <p className="text-sm leading-6 text-slate-200">{item}</p>
                </article>
              ))}
            </section>

            <TextSection title="The Research Problem" paragraphs={page.problem} />
            <TextSection title="How Walnut Handles It" paragraphs={page.approach} />

            <section className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
              <h2 className="text-xl font-semibold text-white">Workflow</h2>
              <div className="mt-5 grid gap-3 sm:grid-cols-3">
                {page.workflow.map((item) => (
                  <a key={item.title} href={item.href ?? page.primaryCta.href} className="rounded-lg border border-white/10 bg-slate-950/70 p-4 transition hover:border-emerald-300/35 hover:bg-white/[0.045]">
                    <h3 className="text-base font-semibold text-white">{item.title}</h3>
                    <p className="mt-3 text-sm leading-6 text-slate-400">{item.body}</p>
                  </a>
                ))}
              </div>
            </section>

            <section className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
              <h2 className="text-xl font-semibold text-white">Example {page.journeyTicker} Research Journey</h2>
              <ol className="mt-5 grid gap-3">
                {page.journey.map((step, index) => (
                  <li key={step} className="flex gap-3 rounded-lg border border-white/10 bg-slate-950/70 p-4">
                    <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full border border-emerald-300/35 bg-emerald-300/10 text-sm font-semibold text-emerald-100">{index + 1}</span>
                    <p className="text-sm leading-6 text-slate-300">{step}</p>
                  </li>
                ))}
              </ol>
            </section>

            <TextSection title="Plan and Access Context" paragraphs={[page.access]} />

            <section className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
              <h2 className="text-xl font-semibold text-white">Helpful Next Pages</h2>
              <div className="mt-5 grid gap-3 sm:grid-cols-3">
                {page.relatedLinks.map((link) => (
                  <a key={link.href ?? link.title} href={link.href ?? "/"} className="rounded-lg border border-white/10 bg-slate-950/70 p-4 transition hover:border-emerald-300/35 hover:bg-white/[0.045]">
                    <h3 className="text-base font-semibold text-white">{link.title}</h3>
                    <p className="mt-3 text-sm leading-6 text-slate-400">{link.body}</p>
                  </a>
                ))}
              </div>
            </section>

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
          <div className="grid gap-6 sm:grid-cols-2">
            <nav className="grid content-start gap-2" aria-label="Platform footer">
              <p className="font-semibold text-slate-300">Platform</p>
              {footerPlatformLinks.map((link) => (
                <a key={link.href} href={link.href} className="hover:text-white">
                  {link.label}
                </a>
              ))}
            </nav>
            <nav className="grid content-start gap-2" aria-label="Research data footer">
              <p className="font-semibold text-slate-300">Research data</p>
              {footerResearchLinks.map((link) => (
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

function TextSection({ title, paragraphs }: { title: string; paragraphs: string[] }) {
  return (
    <section className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
      <h2 className="text-xl font-semibold text-white">{title}</h2>
      <div className="mt-4 space-y-3 text-sm leading-7 text-slate-300">
        {paragraphs.map((paragraph) => (
          <p key={paragraph}>{paragraph}</p>
        ))}
      </div>
    </section>
  );
}
