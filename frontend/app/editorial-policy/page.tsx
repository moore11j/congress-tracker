import type { Metadata } from "next";
import { LegalPageShell, LegalSection } from "@/components/landing/LegalPageShell";
import { appCanonicalUrl, appPageMetadata, marketingCanonicalUrl } from "@/lib/marketingMetadata";

export const metadata: Metadata = appPageMetadata("/editorial-policy", {
  title: "Editorial Policy & Research Standards | Walnut Markets",
  description: "How Walnut Markets uses sources and AI in research, attributes authorship, explains data limitations, and accepts correction requests.",
});

const linkStyle = "text-emerald-200 hover:underline";

export default function EditorialPolicyPage() {
  return (
    <LegalPageShell
      eyebrow="Research standards"
      title="Editorial Policy"
      description="How Walnut research is prepared, who publishes it, and how to raise a concern about a finding."
      lastUpdated="September 22, 2026"
    >
      <LegalSection title="Publisher and authorship">
        <p>
          Walnut Markets is published by Walnut Intelligence Inc. Research credited to Walnut Markets is published under the organization’s name, including AI-assisted briefs. That byline identifies the publisher responsible for the content; it does not identify an individual analyst or imply independent human review of every brief.
        </p>
        <p>
          Contributors may use a consistent pen name for privacy. Any such byline must represent a real contributor’s work, with an accurate description of their role. Walnut does not use fictional credentials or claim a reviewer checked an article unless that review took place.
        </p>
        <p><a href={appCanonicalUrl("/about")} className={linkStyle}>About Walnut Markets</a></p>
      </LegalSection>

      <LegalSection title="AI assistance and editorial responsibility">
        <p>
          Walnut uses AI to help organize source material, summarize data, and draft research briefs. AI-assisted briefs are labeled as such. Automated checks and AI-generated summaries can miss context or produce errors; publication and a Walnut byline are not guarantees of accuracy or a statement that every claim has been manually verified.
        </p>
        <p>
          Walnut Intelligence Inc. remains responsible for the research it publishes. Readers should compare a material finding with its linked source and the dates shown before relying on it.
        </p>
      </LegalSection>

      <LegalSection title="Sources, dates, and data limitations">
        <p>
          Research draws on public filings, company disclosures, government records, market data providers, and Walnut’s derived analysis. Source links let readers distinguish the underlying record from Walnut’s interpretation. Missing or incomplete data should not be treated as evidence that no activity occurred.
        </p>
        <p>
          A transaction date, filing date, reporting period, and article publication date can describe different points in time. Reported insider trades and institutional holdings are historical disclosures, not a live view of anyone’s portfolio. An article’s updated date does not mean every underlying dataset was refreshed at that moment.
        </p>
      </LegalSection>

      <LegalSection title="Scores and historical performance">
        <p>
          Walnut’s Confirmation Score summarizes the evidence used by its methodology. It is not a probability of profit or a promise of future returns. Historical results and backtests depend on their stated periods, assumptions, and available data.
        </p>
        <p>
          Read the <a href={marketingCanonicalUrl("/stock-confirmation-score")} className={linkStyle}>Confirmation Score methodology</a> and the explanations accompanying the <a href={appCanonicalUrl("/outcomes")} className={linkStyle}>Outcomes ledger</a> before comparing results.
        </p>
      </LegalSection>

      <LegalSection title="Report an error or request a correction">
        <p>
          Email <a href="mailto:support@walnutmarkets.com?subject=Research%20correction" className={linkStyle}>support@walnutmarkets.com</a> with the article URL, the statement or figure in question, and a supporting source where available. Use the same address to report a broken source link, an attribution issue, or an omitted limitation.
        </p>
        <p>
          Correction requests are assessed against the underlying evidence. Our standard is to identify substantive corrections in the affected article, explaining what changed and when, rather than silently replacing a material finding. A new market development is distinct from an error in an earlier report.
        </p>
      </LegalSection>

      <LegalSection title="Purpose of the research">
        <p>
          Walnut provides research and information, not personalized investment advice. A stock’s inclusion, a reported purchase, or a high score is not a recommendation to trade. Historical outcomes do not guarantee future results.
        </p>
      </LegalSection>
    </LegalPageShell>
  );
}
