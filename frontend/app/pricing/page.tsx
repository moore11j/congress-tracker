import type { Metadata } from "next";
import { PricingPlannerDeferred } from "@/components/billing/PricingPlannerDeferred";
import { defaultPlanConfig } from "@/lib/defaultPlanConfig";
import { WALNUT_MARKETING_URL, WALNUT_SOCIAL_IMAGE_URL, WALNUT_X_HANDLE, marketingCanonicalUrl, marketingPageMetadata } from "@/lib/marketingMetadata";

export const dynamic = "force-static";
export const revalidate = false;

export const metadata: Metadata = marketingPageMetadata("/pricing", {
  title: "Walnut Markets Pricing | Stock Research Software Plans",
  description:
    "Compare Walnut Markets Free, Premium, and Pro plans for stock research, confirmation scoring, disclosures, watchlists, monitoring, and Pro data layers.",
  openGraph: {
    title: "Walnut Markets Pricing",
    description:
      "Compare plans for stock research, disclosure context, monitoring, confirmation scoring, and Pro data layers.",
    type: "website",
    siteName: "Walnut Markets",
    images: [
      {
        url: WALNUT_SOCIAL_IMAGE_URL,
        width: 1200,
        height: 630,
        alt: "Walnut Markets stock research and analysis platform.",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    site: WALNUT_X_HANDLE,
    title: "Walnut Markets Pricing",
    description:
      "Compare Free, Premium, and Pro plans for Walnut's stock research workflow.",
    images: [WALNUT_SOCIAL_IMAGE_URL],
  },
});

const pricingFaq = [
  {
    question: "Is Walnut Markets free?",
    answer: "Yes. The Free plan supports starter research, a small watchlist, core feeds, and limited screening.",
  },
  {
    question: "Which plan includes the confirmation score?",
    answer: "Premium includes ticker confirmation, peer comparison, deeper monitoring, and higher workflow limits.",
  },
  {
    question: "Which plan includes institutional activity?",
    answer: "Institutional activity, institutional filters, market pressure, and macro positioning are Pro-level features.",
  },
  {
    question: "Can I cancel a paid plan?",
    answer: "Yes. Billing is managed from your account, and access remains available through the paid period.",
  },
] as const;

function planOffer(tier: "free" | "premium" | "pro", name: string, interval: "monthly" | "annual") {
  const price = defaultPlanConfig.plan_prices.find((item) => item.tier === tier && item.billing_interval === interval);
  if (!price) return null;
  return {
    "@type": "Offer",
    name: `${name} ${interval}`,
    url: marketingCanonicalUrl("/pricing"),
    price: Number((price.amount_cents / 100).toFixed(2)),
    priceCurrency: price.currency || "USD",
    availability: "https://schema.org/InStock",
  };
}

function pricingJsonLd() {
  const offers = [
    planOffer("free", "Free plan", "monthly"),
    planOffer("premium", "Premium plan", "monthly"),
    planOffer("premium", "Premium plan", "annual"),
    planOffer("pro", "Pro plan", "monthly"),
    planOffer("pro", "Pro plan", "annual"),
  ].filter(Boolean);

  return [
    {
      "@context": "https://schema.org",
      "@type": "SoftwareApplication",
      name: "Walnut Market Terminal",
      applicationCategory: "FinanceApplication",
      operatingSystem: "Web",
      url: WALNUT_MARKETING_URL,
      image: WALNUT_SOCIAL_IMAGE_URL,
      offers,
    },
    {
      "@context": "https://schema.org",
      "@type": "BreadcrumbList",
      itemListElement: [
        { "@type": "ListItem", position: 1, name: "Walnut Markets", item: WALNUT_MARKETING_URL },
        { "@type": "ListItem", position: 2, name: "Pricing", item: marketingCanonicalUrl("/pricing") },
      ],
    },
    {
      "@context": "https://schema.org",
      "@type": "FAQPage",
      mainEntity: pricingFaq.map((item) => ({
        "@type": "Question",
        name: item.question,
        acceptedAnswer: { "@type": "Answer", text: item.answer },
      })),
    },
  ];
}

export default async function PricingPage() {
  return (
    <div className="mx-auto max-w-6xl space-y-8">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(pricingJsonLd()).replace(/</g, "\\u003c") }} />
      <PricingPlannerDeferred />
      <section className="rounded-lg border border-white/10 bg-slate-950/60 p-5">
        <h2 className="text-xl font-semibold text-white">Pricing FAQ</h2>
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {pricingFaq.map((item) => (
            <div key={item.question} className="rounded-lg border border-white/10 bg-white/[0.03] p-4">
              <h3 className="text-sm font-semibold text-white">{item.question}</h3>
              <p className="mt-2 text-sm leading-6 text-slate-400">{item.answer}</p>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
