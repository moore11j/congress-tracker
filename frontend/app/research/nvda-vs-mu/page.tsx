import type { Metadata } from "next";
import Link from "next/link";
import { CampaignEventOnMount } from "@/components/campaign/CampaignAnalytics";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { CampaignCtaLink } from "@/components/research/CampaignCtaLink";
import { PremiumResearchGate } from "@/components/research/MuPremiumGate";
import { ResearchBriefContextualCta } from "@/components/research/ResearchBriefContextualCta";
import { getEntitlements } from "@/lib/api";
import { isAdminEntitlement, normalizeTier, type Entitlements } from "@/lib/entitlements";
import { getResearchBriefBySlug } from "@/lib/researchBriefs";
import { buildReturnTo, optionalPageAuthToken } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";

const brief = getResearchBriefBySlug("nvda-vs-mu");
const canonicalUrl = "https://walnutmarkets.com/research/nvda-vs-mu";
const pageTitle = "NVDA vs MU: which is the better buy right now?";
const pageDescription =
  "NVIDIA has the cleaner AI demand and margin story, while Micron offers cheaper memory-cycle torque if DRAM and HBM pricing keep recovering.";

const nvdaTerminalHref = "https://app.walnutmarkets.com/ticker/NVDA";
const muTerminalHref = "https://app.walnutmarkets.com/ticker/MU";
const compareHref = "/compare/NVDA/MU?utm_source=reddit&utm_medium=organic_social&utm_campaign=nvda_vs_mu_research&utm_content=research_page_compare";
const micronSourceHref = "https://micron.gcs-web.com/news-releases/news-release-details/micron-technology-inc-reports-record-results-third-quarter";
const nvidiaSourceHref = "https://investor.nvidia.com/news/press-release-details/2026/NVIDIA-Announces-Financial-Results-for-First-Quarter-Fiscal-2027/default.aspx";

export const metadata: Metadata = {
  title: `${brief?.title ?? pageTitle} | Walnut Markets Research`,
  description: brief?.description ?? pageDescription,
  alternates: {
    canonical: "/research/nvda-vs-mu",
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
  about: ["NVDA", "MU", "NVIDIA", "Micron", "AI semiconductors", "HBM", "DRAM"],
};

const headlineMetrics = [
  {
    label: "Call",
    value: "NVDA",
    detail: "NVIDIA has the better business quality and cleaner data, even though MU has more recovery torque.",
  },
  {
    label: "NVDA Q1 FY2027 revenue",
    value: "$81.6B",
    detail: "Revenue rose 85% year over year, with Data Center revenue at $75.2B.",
  },
  {
    label: "MU Q3 FY2026 revenue",
    value: "$41.46B",
    detail: "Micron reported record results as AI memory demand and Strategic Customer Agreements strengthened the cycle.",
  },
  {
    label: "Key split",
    value: "Quality vs torque",
    detail: "NVDA is the cleaner compounder. MU has more cyclical upside if memory pricing keeps improving.",
  },
] as const;

const comparisonRows = [
  ["Latest reported period", "Q1 FY2027, reported May 20, 2026", "Q3 FY2026, reported June 24, 2026"],
  ["Revenue", "$81.6B, up 85% year over year", "$41.46B, up from $23.86B in the prior quarter and $9.30B a year ago"],
  ["GAAP EPS", "$2.39 diluted EPS", "$24.67 diluted EPS"],
  ["Non-GAAP EPS", "$1.87 diluted EPS", "$25.11 diluted EPS"],
  ["Gross margin", "74.9% GAAP / 75.0% non-GAAP", "84.6% GAAP / 84.9% non-GAAP"],
  ["Core read", "AI accelerator demand is translating into revenue scale, Data Center growth, and high margins.", "AI memory demand is driving a powerful cyclical recovery with stronger pricing and customer commitments."],
  ["Main risk", "Expectations are high and any slowdown in AI infrastructure spending would pressure the premium multiple.", "Memory remains cyclical; DRAM/HBM pricing or demand rollover would hit the setup faster."],
] as const;

const nvdaCase = [
  "NVDA is converting AI infrastructure demand into very large revenue growth and margin power.",
  "The Data Center segment gives investors a direct read on AI factory buildout demand.",
  "The latest quarter showed scale plus profitability, which makes the story cleaner than a pure recovery trade.",
  "The risk is valuation and expectations: NVDA has less room for vague guidance or slower demand signals.",
] as const;

const muCase = [
  "MU has the cheaper and more explosive recovery setup if the memory cycle keeps tightening.",
  "HBM, DRAM, and strategic customer agreements can create sharp upside when pricing and supply discipline line up.",
  "Micron's reported gross margin was stronger than NVIDIA's in the latest cited quarters, showing how powerful the cycle has become.",
  "The risk is cyclicality: if memory pricing rolls over, the earnings torque can reverse quickly.",
] as const;

const watchItems = [
  "NVDA Data Center revenue growth and forward demand commentary",
  "NVDA gross margin stability as new architecture cycles ramp",
  "MU DRAM and HBM pricing commentary",
  "MU customer agreement durability and supply discipline",
  "Free cash flow conversion at both companies",
  "Relative valuation after earnings revisions",
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

function BulletPanel({ title, items, tone = "emerald" }: { title: string; items: readonly string[]; tone?: "emerald" | "cyan" | "rose" | "amber" }) {
  const toneClass = {
    emerald: "text-emerald-300",
    cyan: "text-cyan-300",
    rose: "text-rose-300",
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

function canReadFullArticle(entitlements: Entitlements | null) {
  if (!entitlements) return false;
  if (isAdminEntitlement(entitlements)) return true;
  const tier = normalizeTier(entitlements.effective_tier ?? entitlements.tier);
  return tier === "premium" || tier === "pro" || tier === "admin";
}

function entitlementLabel(entitlements: Entitlements | null) {
  if (!entitlements) return "logged_out";
  if (isAdminEntitlement(entitlements)) return "admin";
  return normalizeTier(entitlements.effective_tier ?? entitlements.tier);
}

async function loadEntitlements(): Promise<{ entitlements: Entitlements | null; authenticated: boolean }> {
  const authToken = await optionalPageAuthToken();
  if (!authToken) return { entitlements: null, authenticated: false };
  try {
    return {
      entitlements: await getEntitlements(authToken, { source: "NvdaMuResearchPremiumGate" }),
      authenticated: true,
    };
  } catch {
    return { entitlements: null, authenticated: true };
  }
}

function returnToWithResearchContext(searchParams: Record<string, string | string[] | undefined>) {
  const params: Record<string, string | string[] | undefined> = {
    ...searchParams,
    cta_ticker: "NVDA",
    research_slug: "nvda-vs-mu",
  };
  return buildReturnTo("/research/nvda-vs-mu", params);
}

export default async function NvdaVsMuResearchPage({ searchParams }: { searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  const sp = await searchParams;
  const returnTo = returnToWithResearchContext(sp);
  const { entitlements, authenticated } = await loadEntitlements();
  const canReadFull = canReadFullArticle(entitlements);
  const userEntitlement = entitlementLabel(entitlements);

  return (
    <main className="-mx-4 -my-1.5 min-h-screen bg-[#06111f] text-slate-100 sm:-mx-6 lg:-mx-8 2xl:-mx-10">
      {canReadFull ? (
        <>
          <CampaignEventOnMount eventName="research_preview_viewed" path={returnTo} properties={{ article_slug: "nvda-vs-mu", ticker: "NVDA", tickers: "NVDA,MU", user_entitlement: userEntitlement }} />
          <CampaignEventOnMount eventName="research_full_article_viewed" path={returnTo} properties={{ article_slug: "nvda-vs-mu", ticker: "NVDA", tickers: "NVDA,MU", user_entitlement: userEntitlement }} />
        </>
      ) : null}
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(articleSchema).replace(/</g, "\\u003c") }} />

      <section className="border-b border-white/10 bg-[linear-gradient(180deg,rgba(8,20,35,0.98),rgba(6,17,31,0.94))]">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.95fr] lg:px-8 lg:py-14">
          <div className="flex min-w-0 flex-col justify-center">
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">
              <WalnutBrandMark className="flex h-7 w-7 items-center justify-center rounded-lg border border-emerald-300/30 bg-slate-950" svgClassName="h-5 w-5 overflow-visible" />
              Walnut Comparison Brief
            </div>
            <h1 className="mt-5 max-w-4xl text-4xl font-semibold leading-tight text-white sm:text-5xl">
              NVDA vs MU: which is the better buy right now?
            </h1>
            <p className="mt-5 max-w-3xl text-lg leading-8 text-slate-300">
              {canReadFull
                ? "NVDA is still the cleaner setup. MU has the cheaper recovery profile and more upside torque if memory keeps recovering, but the current data favors NVIDIA's AI demand, revenue scale, and margin power."
                : "The comparison turns on quality versus torque: NVIDIA's direct AI infrastructure demand against Micron's memory-cycle recovery."}
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <CampaignCtaLink href={nvdaTerminalHref} eventName="view_ticker_nvda_click" className={primaryButtonClassName} properties={{ campaign: "nvda_vs_mu_research" }}>
                View NVDA data
              </CampaignCtaLink>
              <CampaignCtaLink href={muTerminalHref} eventName="view_ticker_mu_click" className={secondaryButtonClassName} properties={{ campaign: "nvda_vs_mu_research" }}>
                View MU data
              </CampaignCtaLink>
              {canReadFull ? (
                <Link href={compareHref} className={secondaryButtonClassName}>
                  Compare NVDA vs MU
                </Link>
              ) : (
                <Link href={`/login?mode=register&return_to=${encodeURIComponent(returnTo)}`} className={secondaryButtonClassName}>
                  Create free account
                </Link>
              )}
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
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{canReadFull ? "The Call" : "The Setup"}</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">
            {canReadFull ? "More upside torque: MU. Better business plus cleaner data: NVDA." : "The NVDA versus MU debate turns on quality versus cycle torque."}
          </h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            Micron's latest report shows how powerful the AI memory cycle can be. The company posted record revenue, very high gross margin, and large EPS as demand accelerated. That is the torque.
          </p>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            {canReadFull
              ? "NVIDIA still gets the edge because its AI demand is showing up as massive revenue scale, Data Center concentration, high margins, and a more direct role in the AI infrastructure buildout."
              : "The full brief weighs whether NVIDIA's AI revenue quality is enough to beat Micron's cheaper recovery setup."}
          </p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-5">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-cyan-300">Decision Frame</p>
          <div className="mt-4 grid gap-3 text-sm leading-6 text-slate-300">
            <div className="rounded-lg border border-white/10 bg-white/[0.035] px-4 py-3">
              Choose NVDA if the priority is business quality, AI revenue visibility, and cleaner execution data.
            </div>
            <div className="rounded-lg border border-white/10 bg-white/[0.035] px-4 py-3">
              Choose MU if the priority is cheaper cyclical torque and the memory recovery keeps extending.
            </div>
          </div>
        </div>
      </section>

      {canReadFull ? (
        <>
          <section className="border-y border-white/10 bg-slate-950/40">
            <div className="mx-auto max-w-7xl px-4 py-10 sm:px-6 lg:px-8">
              <div className="max-w-3xl">
                <p className="text-xs font-semibold uppercase tracking-[0.22em] text-amber-300">Reported Data</p>
                <h2 className="mt-3 text-2xl font-semibold text-white">The latest cited quarters show two strong stories, but different risk profiles.</h2>
              </div>
              <div className="mt-6 overflow-hidden rounded-lg border border-white/10">
                <div className="hidden grid-cols-[0.9fr_1.1fr_1.1fr] border-b border-white/10 bg-white/[0.04] px-4 py-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:grid">
                  <span>Metric</span>
                  <span>NVDA</span>
                  <span>MU</span>
                </div>
                <div className="divide-y divide-white/10 bg-slate-950/45">
                  {comparisonRows.map(([metric, nvda, mu]) => (
                    <article key={metric} className="grid gap-3 px-4 py-4 text-sm leading-6 text-slate-300 md:grid-cols-[0.9fr_1.1fr_1.1fr] md:items-start">
                      <div>
                        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Metric</p>
                        <p className="font-semibold text-white">{metric}</p>
                      </div>
                      <div>
                        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">NVDA</p>
                        <p>{nvda}</p>
                      </div>
                      <div>
                        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">MU</p>
                        <p>{mu}</p>
                      </div>
                    </article>
                  ))}
                </div>
              </div>
              <p className="mt-4 text-xs leading-5 text-slate-500">
                Source periods: NVIDIA Q1 FY2027, reported May 20, 2026; Micron Q3 FY2026, reported June 24, 2026.
              </p>
            </div>
          </section>

          <section className="mx-auto grid max-w-7xl gap-5 px-4 py-10 sm:px-6 lg:grid-cols-2 lg:px-8">
            <BulletPanel title="Why NVDA Gets The Edge" items={nvdaCase} />
            <BulletPanel title="Why MU Still Has Torque" items={muCase} tone="cyan" />
          </section>

          <section className="border-y border-white/10 bg-slate-950/40">
            <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.85fr_1.15fr] lg:px-8">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.22em] text-rose-300">What Could Change The Call</p>
                <h2 className="mt-3 text-2xl font-semibold text-white">The NVDA edge weakens if AI demand slows; the MU setup weakens if memory pricing rolls over.</h2>
                <p className="mt-4 text-sm leading-7 text-slate-400">
                  This is not a permanent ranking. It is a current-data view. NVDA wins on cleaner business quality and direct AI infrastructure exposure, while MU can outperform when the memory cycle is tightening faster than investors expect.
                </p>
              </div>
              <div className="grid gap-2">
                {watchItems.map((item) => (
                  <div key={item} className="rounded-lg border border-white/10 bg-slate-950/55 px-4 py-3 text-sm leading-6 text-slate-300">
                    {item}
                  </div>
                ))}
              </div>
            </div>
          </section>

          <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.9fr] lg:px-8">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Bottom Line</p>
              <h2 className="mt-3 text-2xl font-semibold text-white">NVDA is the better business. MU is the higher-torque recovery trade.</h2>
              <p className="mt-4 text-sm leading-7 text-slate-400">
                The simplest framing: MU has more upside torque if memory demand, HBM pricing, and supply discipline keep improving. NVDA has the cleaner data because AI infrastructure demand is already flowing through revenue, EPS, and margins at enormous scale.
              </p>
              <p className="mt-4 text-sm leading-7 text-slate-400">
                On the current evidence, the better buy is NVDA. The better recovery torque is MU.
              </p>
            </div>
            <div className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.06] p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Track Both</p>
              <h3 className="mt-3 text-lg font-semibold text-white">Keep the comparison current inside Walnut.</h3>
              <p className="mt-3 text-sm leading-6 text-slate-300">
                Re-check the ticker pages as estimates, filings, price/volume confirmation, and market context change.
              </p>
              <div className="mt-5 flex flex-wrap gap-3">
                <CampaignCtaLink href={nvdaTerminalHref} eventName="view_ticker_nvda_click" className={primaryButtonClassName} properties={{ campaign: "nvda_vs_mu_research", placement: "footer" }}>
                  View NVDA
                </CampaignCtaLink>
                <CampaignCtaLink href={muTerminalHref} eventName="view_ticker_mu_click" className={secondaryButtonClassName} properties={{ campaign: "nvda_vs_mu_research", placement: "footer" }}>
                  View MU
                </CampaignCtaLink>
              </div>
            </div>
          </section>

          <section className="border-t border-white/10 bg-slate-950/30 px-4 py-10 sm:px-6 lg:px-8">
            <div className="mx-auto max-w-7xl">
              <ResearchBriefContextualCta ticker="NVDA" companyName="NVIDIA" researchSlug="nvda-vs-mu" />
            </div>
          </section>

          <section className="border-t border-white/10 px-4 py-8 sm:px-6 lg:px-8">
            <div className="mx-auto max-w-7xl text-xs leading-6 text-slate-500">
              Sources:{" "}
              <Link href={nvidiaSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
                NVIDIA Q1 FY2027 results
              </Link>
              {" "}and{" "}
              <Link href={micronSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
                Micron Q3 FY2026 results
              </Link>
              . Data and expectations referenced as of August 3, 2026. Research only. Not investment advice.
            </div>
          </section>
        </>
      ) : (
        <section className="border-t border-white/10 bg-slate-950/35 px-4 py-10 sm:px-6 lg:px-8">
          <div className="mx-auto max-w-4xl">
            <PremiumResearchGate
              authState={authenticated ? "free" : "logged_out"}
              entitlement={userEntitlement}
              returnTo={returnTo}
              articleSlug="nvda-vs-mu"
              tickers={["NVDA", "MU"]}
              requiredPlan="premium"
              heading="Unlock Walnut's Full NVDA vs MU Conclusion"
              description="See the full comparison table, directional judgment, supporting evidence, catalysts, risks, and what could change the call."
            />
          </div>
        </section>
      )}
    </main>
  );
}
