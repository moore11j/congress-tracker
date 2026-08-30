import type { Metadata } from "next";
import Link from "next/link";
import { CampaignEventOnMount, CampaignTrackedLink } from "@/components/campaign/CampaignAnalytics";
import { RedditTickerSearchForm } from "@/components/campaign/RedditTickerSearchForm";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { optionalPageAuthState } from "@/lib/serverAuth";
import { getEntitlements } from "@/lib/api";
import { campaignPropertiesFromRecord, pathWithCampaignParams, registerHref, type SearchParamRecord } from "@/lib/campaignAttribution";
import { marketingCanonicalUrl, marketingPageMetadata, WALNUT_APP_URL } from "@/lib/marketingMetadata";

type PageProps = {
  searchParams?: Promise<SearchParamRecord>;
};

export const dynamic = "force-dynamic";

const pagePath = "/reddit/stock-research";
const pageTitle = "Stock Research Without the Tab Overload | Walnut Markets";
const pageDescription = "Before you buy any stock, run it through Walnut. Research fundamentals, price trends, public activity, positioning and market context in one workflow.";

export const metadata: Metadata = marketingPageMetadata(pagePath, {
  title: pageTitle,
  description: pageDescription,
  robots: {
    index: false,
    follow: true,
  },
  openGraph: {
    type: "website",
    title: pageTitle,
    description: pageDescription,
    url: marketingCanonicalUrl(pagePath),
    siteName: "Walnut Markets",
    images: [
      {
        url: "/landing/compare-nvda-mu-production.png",
        width: 1265,
        height: 713,
        alt: "Walnut Markets stock comparison workflow.",
      },
    ],
  },
});

const primaryButtonClassName =
  "inline-flex min-h-11 items-center justify-center rounded-lg bg-emerald-300 px-5 py-2.5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200";
const secondaryButtonClassName =
  "inline-flex min-h-11 items-center justify-center rounded-lg border border-white/15 px-5 py-2.5 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/50 hover:text-emerald-100";

const beforeItems = [
  "Check fundamentals in one place",
  "Open another site for charts",
  "Search filings and insider trades separately",
  "Compare conflicting information manually",
  "Lose track of what actually changed",
] as const;

const afterItems = [
  "Research the stock in one workflow",
  "See what supports the move",
  "See what conflicts",
  "Compare stocks directly",
  "Know what to watch next",
] as const;

const outcomeCards = [
  {
    title: "Is the stock worth a closer look?",
    copy: "Review fundamentals, price trends and relevant activity without piecing everything together manually.",
  },
  {
    title: "Is the setup getting stronger or weaker?",
    copy: "See which data points support the move, which ones conflict, and what recently changed.",
  },
  {
    title: "What should you watch next?",
    copy: "Track the catalysts, risks and conditions that could change the conclusion.",
  },
] as const;

const featureGroups = [
  "Fundamentals",
  "Price and volume",
  "Congress activity",
  "Insider activity",
  "Institutional positioning",
  "Government contracts",
  "Macro context",
  "Stock comparisons",
  "Research Briefs",
] as const;

function SectionEyebrow({ children }: { children: string }) {
  return <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{children}</p>;
}

function CheckList({ title, items, tone }: { title: string; items: readonly string[]; tone: "before" | "after" }) {
  const accentClassName = tone === "before" ? "text-amber-200" : "text-emerald-200";
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
      <h3 className={`text-sm font-semibold ${accentClassName}`}>{title}</h3>
      <ul className="mt-4 grid gap-2 text-sm leading-6 text-slate-300">
        {items.map((item) => (
          <li key={item} className="rounded-md border border-white/10 bg-white/[0.03] px-3 py-2">
            {item}
          </li>
        ))}
      </ul>
    </div>
  );
}

export default async function RedditStockResearchPage({ searchParams }: PageProps) {
  const sp = (await searchParams) ?? {};
  const authState = await optionalPageAuthState();
  const entitlements = authState.token ? await getEntitlements(authState.token, { source: "RedditStockResearchPage" }).catch(() => null) : null;
  const plan = entitlements?.effective_tier ?? entitlements?.tier ?? (authState.token ? "free" : "logged_out");
  const researchEntryPath = pathWithCampaignParams("/search", sp, { referring_landing_page: pagePath });
  const primaryHref = authState.token ? researchEntryPath : registerHref(researchEntryPath);
  const compareHref = pathWithCampaignParams("/compare/_/_", sp, { referring_landing_page: pagePath });
  const insightsHref = pathWithCampaignParams("/insights", sp, { referring_landing_page: pagePath });
  const pricingHref = `${WALNUT_APP_URL}${pathWithCampaignParams("/pricing", sp, { referring_landing_page: pagePath })}`;
  const pageProperties = {
    ...campaignPropertiesFromRecord(sp),
    page_path: pagePath,
    auth_state: authState.token ? "authenticated" : "logged_out",
    plan: String(plan),
  };

  return (
    <main className="min-h-screen bg-[#06111f] text-slate-100">
      <CampaignEventOnMount eventName="reddit_landing_view" path={pagePath} properties={pageProperties} />
      <header className="border-b border-white/10 bg-slate-950/80">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-4 sm:px-6 lg:px-8">
          <Link href="/" className="flex items-center gap-2">
            <WalnutBrandMark className="flex h-8 w-8 items-center justify-center rounded-lg border border-emerald-300/30 bg-slate-950" svgClassName="h-5 w-5 overflow-visible" />
            <span className="font-semibold text-white">Walnut</span>
          </Link>
          <nav className="flex items-center gap-2">
            <Link href="/login" className="hidden rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 hover:border-white/20 hover:text-white sm:inline-flex">
              Sign In
            </Link>
            <CampaignTrackedLink
              href={primaryHref}
              eventName="reddit_landing_primary_cta_click"
              secondaryEventName={authState.token ? undefined : "reddit_signup_start"}
              path={pagePath}
              properties={{ ...pageProperties, cta: "header_research_free" }}
              className="inline-flex min-h-10 items-center justify-center rounded-lg border border-emerald-300/35 bg-emerald-300/15 px-3 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
            >
              Research Free
            </CampaignTrackedLink>
          </nav>
        </div>
      </header>

      <section className="border-b border-white/10 bg-[linear-gradient(180deg,rgba(8,20,35,0.98),rgba(6,17,31,0.94))]">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.9fr_1.1fr] lg:px-8 lg:py-14">
          <div className="flex min-w-0 flex-col justify-center">
            <SectionEyebrow>Stock research without the tab overload</SectionEyebrow>
            <h1 className="mt-4 max-w-3xl text-4xl font-semibold leading-tight text-white sm:text-5xl">
              Before you buy any stock, run it through Walnut.
            </h1>
            <p className="mt-5 max-w-2xl text-lg leading-8 text-slate-300">
              Research fundamentals, price trends, Congress and insider activity, institutional positioning and more in one place. See what supports the stock, what conflicts, and what to watch next.
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <CampaignTrackedLink
                href={primaryHref}
                eventName="reddit_landing_primary_cta_click"
                secondaryEventName={authState.token ? undefined : "reddit_signup_start"}
                path={pagePath}
                properties={{ ...pageProperties, cta: "hero_primary" }}
                className={primaryButtonClassName}
              >
                Research Your First Stock Free
              </CampaignTrackedLink>
              <CampaignTrackedLink
                href="#workflow"
                eventName="reddit_landing_secondary_cta_click"
                path={pagePath}
                properties={{ ...pageProperties, cta: "hero_secondary" }}
                className={secondaryButtonClassName}
              >
                See How Walnut Works
              </CampaignTrackedLink>
            </div>
          </div>
          <div className="min-w-0 rounded-lg border border-white/10 bg-slate-950/65 p-3 shadow-2xl shadow-black/30">
            <img src="/landing/compare-nvda-mu-production.png" alt="Walnut Markets production stock comparison screen." className="h-auto w-full rounded-md border border-white/10" />
            <p className="mt-3 text-xs leading-5 text-slate-500">Production Walnut comparison workflow. Research only, not investment advice.</p>
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 bg-slate-950/35">
        <div className="mx-auto max-w-7xl px-4 py-5 text-center text-sm font-medium text-slate-300 sm:px-6 lg:px-8">
          One research workflow for fundamentals, price action, alternative data and market context.
        </div>
      </section>

      <section className="mx-auto max-w-7xl px-4 py-10 sm:px-6 lg:px-8">
        <div className="max-w-3xl">
          <SectionEyebrow>Before and after</SectionEyebrow>
          <h2 className="mt-3 text-2xl font-semibold text-white">Stop rebuilding the same stock research across five different websites.</h2>
        </div>
        <div className="mt-6 grid gap-4 lg:grid-cols-2">
          <CheckList title="Before Walnut" items={beforeItems} tone="before" />
          <CheckList title="With Walnut" items={afterItems} tone="after" />
        </div>
      </section>

      <section className="border-y border-white/10 bg-slate-950/35">
        <div className="mx-auto max-w-7xl px-4 py-10 sm:px-6 lg:px-8">
          <div className="max-w-3xl">
            <SectionEyebrow>Decision context</SectionEyebrow>
            <h2 className="mt-3 text-2xl font-semibold text-white">Know what the data is actually telling you.</h2>
          </div>
          <div className="mt-6 grid gap-4 md:grid-cols-3">
            {outcomeCards.map((card) => (
              <article key={card.title} className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
                <h3 className="text-base font-semibold text-white">{card.title}</h3>
                <p className="mt-3 text-sm leading-6 text-slate-400">{card.copy}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section id="workflow" className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.85fr_1.15fr] lg:px-8">
        <div>
          <SectionEyebrow>Workflow</SectionEyebrow>
          <h2 className="mt-3 text-2xl font-semibold text-white">From ticker to decision in minutes.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">Start with a symbol, review the Walnut conclusion and supporting data, then save it, compare it or track what changes.</p>
          <CampaignTrackedLink
            href={primaryHref}
            eventName="reddit_landing_primary_cta_click"
            secondaryEventName={authState.token ? undefined : "reddit_signup_start"}
            path={pagePath}
            properties={{ ...pageProperties, cta: "workflow_research_stock" }}
            className={`${primaryButtonClassName} mt-6`}
          >
            Research a Stock Free
          </CampaignTrackedLink>
        </div>
        <div className="grid gap-3 sm:grid-cols-3">
          {["Search a stock", "Review the Walnut conclusion and supporting data", "Save it, compare it or track what changes"].map((step, index) => (
            <article key={step} className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-300">Step {index + 1}</p>
              <h3 className="mt-3 text-base font-semibold text-white">{step}</h3>
            </article>
          ))}
        </div>
      </section>

      <section className="border-y border-white/10 bg-slate-950/35">
        <div className="mx-auto max-w-7xl px-4 py-10 sm:px-6 lg:px-8">
          <div className="max-w-3xl">
            <SectionEyebrow>Production capabilities</SectionEyebrow>
            <h2 className="mt-3 text-2xl font-semibold text-white">The research most investors piece together manually.</h2>
          </div>
          <div className="mt-6 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {featureGroups.map((feature) => (
              <div key={feature} className="rounded-lg border border-white/10 bg-slate-950/55 px-4 py-3 text-sm font-semibold text-slate-200">
                {feature}
              </div>
            ))}
          </div>
          <div className="mt-4 rounded-lg border border-emerald-300/20 bg-emerald-300/[0.06] p-4">
            <h3 className="text-sm font-semibold text-emerald-100">Confirmation score</h3>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              Walnut&apos;s proprietary confirmation score summarizes whether the available data is leaning bullish, bearish or mixed, and shows the data behind the conclusion.
            </p>
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-4 px-4 py-10 sm:px-6 lg:grid-cols-2 lg:px-8">
        <article className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
          <SectionEyebrow>Compare</SectionEyebrow>
          <h2 className="mt-3 text-2xl font-semibold text-white">Compare two stocks. See which one has stronger support.</h2>
          <p className="mt-3 text-sm leading-7 text-slate-400">
            Put fundamentals, price trends, positioning and alternative data side by side. Walnut highlights where one stock is stronger and where the conclusion is less certain.
          </p>
          <CampaignTrackedLink href={compareHref} eventName="reddit_landing_secondary_cta_click" path={pagePath} properties={{ ...pageProperties, cta: "compare_two_stocks" }} className={`${secondaryButtonClassName} mt-5`}>
            Compare Two Stocks
          </CampaignTrackedLink>
        </article>
        <article className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
          <SectionEyebrow>Research Briefs</SectionEyebrow>
          <h2 className="mt-3 text-2xl font-semibold text-white">Go deeper without starting from a blank page.</h2>
          <p className="mt-3 text-sm leading-7 text-slate-400">
            Read structured research covering what changed, key data, catalysts, risks and what to watch next.
          </p>
          <CampaignTrackedLink href={insightsHref} eventName="reddit_landing_secondary_cta_click" path={pagePath} properties={{ ...pageProperties, cta: "explore_research_briefs" }} className={`${secondaryButtonClassName} mt-5`}>
            Explore Research Briefs
          </CampaignTrackedLink>
        </article>
      </section>

      <section className="border-t border-white/10 bg-slate-950/45">
        <div className="mx-auto grid max-w-7xl gap-6 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.85fr] lg:px-8">
          <div>
            <h2 className="text-3xl font-semibold text-white">Before your next buy, check Walnut.</h2>
            <p className="mt-3 text-sm leading-7 text-slate-300">Research your first stock free and see whether the data supports the move.</p>
            <p className="mt-3 text-sm font-medium text-emerald-100">Start free. Upgrade only when you need deeper research and advanced data.</p>
            <div className="mt-6 flex flex-wrap gap-3">
              <CampaignTrackedLink
                href={primaryHref}
                eventName="reddit_landing_primary_cta_click"
                secondaryEventName={authState.token ? undefined : "reddit_signup_start"}
                path={pagePath}
                properties={{ ...pageProperties, cta: "final_primary" }}
                className={primaryButtonClassName}
              >
                Research Your First Stock Free
              </CampaignTrackedLink>
              <CampaignTrackedLink href={pricingHref} eventName="reddit_landing_secondary_cta_click" path={pagePath} properties={{ ...pageProperties, cta: "view_pricing" }} className={secondaryButtonClassName}>
                View Pricing
              </CampaignTrackedLink>
            </div>
          </div>
          <div className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
            <h3 className="text-sm font-semibold text-white">Search a ticker</h3>
            <p className="mt-2 text-sm leading-6 text-slate-400">Go straight into the research workflow.</p>
            <RedditTickerSearchForm className="mt-4 flex flex-col gap-2 sm:flex-row" />
          </div>
        </div>
      </section>

      <footer className="border-t border-white/10 px-4 py-6 sm:px-6 lg:px-8">
        <div className="mx-auto flex max-w-7xl flex-col gap-3 text-xs leading-5 text-slate-500 sm:flex-row sm:items-center sm:justify-between">
          <p>Research and informational purposes only. Not investment advice.</p>
          <div className="flex flex-wrap gap-4">
            <a href={`${WALNUT_APP_URL}/pricing`} className="hover:text-slate-300">Pricing</a>
            <a href={`${WALNUT_APP_URL}/privacy`} className="hover:text-slate-300">Privacy</a>
            <a href={`${WALNUT_APP_URL}/terms`} className="hover:text-slate-300">Terms</a>
            <a href={`${WALNUT_APP_URL}/faq`} className="hover:text-slate-300">Disclosures</a>
          </div>
        </div>
      </footer>
    </main>
  );
}
