import type { Metadata } from "next";
import { LegalPageShell, LegalSection } from "@/components/landing/LegalPageShell";

const lastUpdated = "June 4, 2026";

export const metadata: Metadata = {
  title: "Terms of Use | Walnut Intelligence",
  description: "Terms of Use for Walnut Intelligence Inc. and Walnut Market Terminal.",
};

export default function TermsPage() {
  return (
    <LegalPageShell
      eyebrow="Legal"
      title="Terms of Use"
      description="These Terms of Use govern access to Walnut Intelligence Inc., Walnut Intel, and Walnut Market Terminal. They are MVP legal terms intended for later review by counsel."
      lastUpdated={lastUpdated}
    >
      <LegalSection title="1. Acceptance of Terms">
        <p>By accessing or using Walnut Intel, Walnut Market Terminal, or related services, you agree to these Terms of Use. If you do not agree, do not use the service.</p>
      </LegalSection>

      <LegalSection title="2. Informational Purposes Only">
        <p>The service is provided for informational and research purposes only. Content, data, screens, alerts, rankings, and signals are not personalized recommendations.</p>
      </LegalSection>

      <LegalSection title="3. No Investment, Financial, Legal, Tax, or Accounting Advice">
        <p>Walnut Intelligence Inc. does not provide financial, investment, tax, accounting, or legal advice. Nothing in the service is investment advice, and you are responsible for your own investment decisions.</p>
      </LegalSection>

      <LegalSection title="4. No Broker-Dealer or Adviser Relationship">
        <p>Use of the service does not create a broker-dealer, investment adviser, fiduciary, attorney-client, accountant-client, or similar professional relationship with Walnut Intelligence Inc.</p>
      </LegalSection>

      <LegalSection title="5. Market Data, Congressional Disclosures, Insider Activity, Government Contract Data, and Third-Party Sources">
        <p>The service may display market data, congressional disclosures, insider activity, government contract information, news, issuer data, analytics, and other information from public records and third-party providers.</p>
        <p>Third-party data may be delayed, incomplete, corrected after publication, restricted by provider terms, or unavailable at times.</p>
      </LegalSection>

      <LegalSection title="6. Reporting Delays, Data Limitations, and No Warranty">
        <p>Congressional disclosures, SEC filings, market data, news, government records, and provider feeds can include delays, errors, omissions, restatements, and formatting differences. The service is provided on an as-is and as-available basis without warranties of accuracy, availability, completeness, or fitness for a particular purpose.</p>
      </LegalSection>

      <LegalSection title="7. User Accounts and Security">
        <p>You are responsible for maintaining the confidentiality of your account credentials and for activity under your account. Notify us promptly if you believe your account has been accessed without authorization.</p>
      </LegalSection>

      <LegalSection title="8. Subscriptions, Billing, and Refunds">
        <p>Paid plans, billing intervals, limits, and features are shown at checkout or in account billing screens. Subscription payments may be processed by third-party payment providers. Unless otherwise stated at purchase, fees are non-refundable except where required by law.</p>
      </LegalSection>

      <LegalSection title="9. Acceptable Use">
        <p>You may not misuse the service, interfere with its operation, attempt unauthorized access, scrape or resell data where prohibited, violate provider terms, infringe intellectual property rights, or use the service for unlawful activity.</p>
      </LegalSection>

      <LegalSection title="10. Intellectual Property">
        <p>Walnut Intelligence Inc. owns or licenses the service, software, design, branding, compilation, analytics, and related materials. You may use the service only as permitted by these Terms and applicable law.</p>
      </LegalSection>

      <LegalSection title="11. Limitation of Liability">
        <p>To the maximum extent permitted by law, Walnut Intelligence Inc. will not be liable for indirect, incidental, consequential, special, exemplary, or punitive damages, or for trading losses, lost profits, lost data, or business interruption arising from use of the service.</p>
      </LegalSection>

      <LegalSection title="12. Changes to the Service">
        <p>We may modify, suspend, or discontinue features, data sources, limits, pricing, or availability at any time. We may also update these Terms, and continued use after updates means you accept the revised Terms.</p>
      </LegalSection>

      <LegalSection title="13. Governing Law">
        <p>These Terms are governed by the laws of British Columbia, Canada, unless Walnut Intelligence Inc. later designates a different governing law in updated terms.</p>
      </LegalSection>

      <LegalSection title="14. Contact">
        <p>Questions about these Terms may be sent to <a className="text-emerald-200 hover:text-emerald-100" href="mailto:support@walnut-intel.com">support@walnut-intel.com</a>.</p>
      </LegalSection>
    </LegalPageShell>
  );
}
