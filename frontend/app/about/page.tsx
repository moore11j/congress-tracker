import type { Metadata } from "next";
import { LegalPageShell, LegalSection } from "@/components/landing/LegalPageShell";
import { legalPageChrome } from "@/lib/legalPageChrome";
import { WALNUT_MARKETING_DESCRIPTION, WALNUT_MARKETING_URL, WALNUT_REDDIT_URL, WALNUT_SOCIAL_URLS, WALNUT_X_HANDLE, WALNUT_X_URL } from "@/lib/marketingMetadata";

const lastUpdated = "July 9, 2026";
const ABOUT_DESCRIPTION =
  "Learn who operates Walnut Market Terminal, why Walnut focuses on disclosure intelligence, and how its research tools are built.";

export const metadata: Metadata = {
  title: "About Walnut Markets | Walnut Market Terminal",
  description: ABOUT_DESCRIPTION,
  alternates: {
    canonical: "/about",
  },
};

const aboutJsonLd = [
  {
    "@context": "https://schema.org",
    "@type": "AboutPage",
    name: "About Walnut Markets",
    url: `${WALNUT_MARKETING_URL}/about`,
    description: ABOUT_DESCRIPTION,
    isPartOf: {
      "@type": "WebSite",
      name: "Walnut Markets",
      url: WALNUT_MARKETING_URL,
    },
    about: {
      "@type": "Organization",
      name: "Walnut Intelligence Inc.",
      url: WALNUT_MARKETING_URL,
      sameAs: WALNUT_SOCIAL_URLS,
    },
  },
  {
    "@context": "https://schema.org",
    "@type": "Organization",
    name: "Walnut Intelligence Inc.",
    url: WALNUT_MARKETING_URL,
    description: WALNUT_MARKETING_DESCRIPTION,
    sameAs: WALNUT_SOCIAL_URLS,
  },
];

export default async function AboutPage() {
  const chrome = await legalPageChrome();

  return (
    <LegalPageShell
      eyebrow="Company"
      title="About Walnut Markets"
      description="Walnut Market Terminal is operated by Walnut Intelligence Inc. and built for investors who want public disclosure data organized into a practical research workflow."
      lastUpdated={lastUpdated}
      chrome={chrome}
    >
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(aboutJsonLd).replace(/</g, "\\u003c") }} />

      <LegalSection title="What Walnut Does">
        <p>
          Walnut brings Congress trading disclosures, SEC insider activity, government contract data, market context, ticker pages, signal scores, screeners, and watchlists into one terminal-style research product.
        </p>
        <p>
          The goal is to make scattered public information easier to monitor, compare, and verify. Walnut is built for research and informational purposes only, and it does not provide investment advice.
        </p>
      </LegalSection>

      <LegalSection title="Operator Background">
        <p>
          Walnut Market Terminal is operated by Walnut Intelligence Inc. and built by a CPA with years of experience working with investment-company reporting, controls, public-market data workflows, and investor-facing financial information.
        </p>
        <p>
          That background shapes the product: traceable source context, clear dates, visible limitations, and practical workflows matter more than black-box claims.
        </p>
      </LegalSection>

      <LegalSection title="Trust and Support">
        <p>
          Walnut uses public filings, government records, market data providers, and secure third-party services to operate the product. Payment information is processed by Stripe, and Walnut does not store full card numbers.
        </p>
        <p>
          Questions, support requests, or data issues can be sent to{" "}
          <a className="text-emerald-200 hover:text-emerald-100" href="mailto:support@walnutmarkets.com">
            support@walnutmarkets.com
          </a>
          .
        </p>
      </LegalSection>

      <LegalSection title="Social Channels">
        <p>
          Follow Walnut on{" "}
          <a className="text-emerald-200 hover:text-emerald-100" href={WALNUT_X_URL} target="_blank" rel="noreferrer">
            X at {WALNUT_X_HANDLE}
          </a>{" "}
          or join{" "}
          <a className="text-emerald-200 hover:text-emerald-100" href={WALNUT_REDDIT_URL} target="_blank" rel="noreferrer">
            r/walnutmarkets
          </a>{" "}
          on Reddit.
        </p>
      </LegalSection>
    </LegalPageShell>
  );
}
