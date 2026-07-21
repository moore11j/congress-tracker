import { publicResearchTools } from "@/lib/publicResearchTools";
import { loginUrl, seoLandingPageJsonLd, type SeoLandingPage } from "@/lib/seoLandingPages";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");

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
            <a href="/congress-trades" className="hover:text-white">
              Congress
            </a>
            <a href="/insider-trading-tracker" className="hover:text-white">
              Insiders
            </a>
            <a href="/market-intelligence-terminal" className="hover:text-white">
              Terminal
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
            <span className="text-slate-300">{page.h1}</span>
          </nav>

          <div className="grid gap-10 lg:grid-cols-[0.95fr_1.05fr] lg:items-start">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{page.eyebrow}</p>
              <h1 className="mt-5 text-4xl font-semibold leading-tight text-white sm:text-5xl">{page.h1}</h1>
              <p className="mt-6 max-w-3xl text-base leading-7 text-slate-300 sm:text-lg">{page.intro}</p>
              <div className="mt-7 flex flex-col gap-3 sm:flex-row">
                <a href={page.primaryCta.href} className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
                  {page.primaryCta.label}
                </a>
                <a href={loginUrl} className="inline-flex items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-5 py-3 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:bg-white/[0.06]">
                  Start Free
                </a>
                <a href="/market-intelligence-terminal" className="inline-flex items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-5 py-3 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:bg-white/[0.06]">
                  Explore Walnut
                </a>
              </div>
              <p className="mt-5 text-xs leading-5 text-slate-400">Research only. Not investment advice.</p>
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
                <div className="mt-4 space-y-3 text-sm leading-7 text-slate-300">
                  {section.paragraphs.map((paragraph) => (
                    <p key={paragraph}>{paragraph}</p>
                  ))}
                </div>
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
        <div className="mx-auto grid max-w-6xl gap-6 text-sm text-slate-400 lg:grid-cols-[1fr_1.3fr]">
          <div>
            <p className="font-semibold text-slate-300">Walnut Market Terminal</p>
            <p className="mt-1 text-xs leading-5">Operated by Walnut Intelligence Inc. Built for research and informational purposes only.</p>
          </div>
          <nav className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3" aria-label="Market data footer">
            {publicResearchTools.map((tool) => (
              <a key={tool.href} href={tool.href} className="hover:text-white">
                {tool.label}
              </a>
            ))}
            <a href="/about" className="hover:text-white">
              About
            </a>
            <a href="/faq" className="hover:text-white">
              FAQ
            </a>
            <a href="mailto:support@walnutmarkets.com" className="hover:text-white">
              Contact
            </a>
          </nav>
        </div>
      </footer>
    </main>
  );
}
