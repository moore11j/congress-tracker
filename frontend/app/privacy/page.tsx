import type { Metadata } from "next";
import { LegalPageShell, LegalSection } from "@/components/landing/LegalPageShell";

const lastUpdated = "June 4, 2026";

export const metadata: Metadata = {
  title: "Privacy Policy | Walnut Intelligence",
  description: "Privacy Policy for Walnut Intelligence Inc. and Walnut Market Terminal.",
};

export default function PrivacyPage() {
  return (
    <LegalPageShell
      eyebrow="Legal"
      title="Privacy Policy"
      description="This Privacy Policy explains how Walnut Intelligence Inc. collects, uses, and shares information in connection with Walnut Intel and Walnut Market Terminal. It is MVP legal content intended for later review by counsel."
      lastUpdated={lastUpdated}
    >
      <LegalSection title="1. Overview">
        <p>Walnut Intelligence Inc. provides market intelligence tools for informational and research purposes only. We collect information needed to operate, secure, bill for, support, and improve the service.</p>
      </LegalSection>

      <LegalSection title="2. Information We Collect">
        <p>We may collect account and contact information such as name, email address, authentication identifiers, and profile settings.</p>
        <p>We may collect billing and subscription information, including plan, billing interval, payment status, invoices, and limited payment metadata from payment providers. We do not intend to store full payment card numbers.</p>
        <p>We may collect watchlists, saved screens, preferences, notification settings, search activity, usage data, device and browser data, logs, IP address, approximate location inferred from technical data, and support communications.</p>
      </LegalSection>

      <LegalSection title="3. How We Use Information">
        <p>We use information to operate the service, authenticate users, provide account access, process billing, send notifications, deliver support, monitor security, prevent fraud or abuse, analyze reliability, and improve product features.</p>
      </LegalSection>

      <LegalSection title="4. Email Notifications and Preferences">
        <p>If you enable email notifications, we use your email address and selected watchlists, saved screens, or preferences to send alerts or digests. You can change notification preferences in the service or by using unsubscribe controls where available.</p>
      </LegalSection>

      <LegalSection title="5. Cookies and Analytics">
        <p>We may use cookies, local storage, session identifiers, and similar technologies for authentication, security, preferences, analytics, performance measurement, and product diagnostics.</p>
      </LegalSection>

      <LegalSection title="6. Third-Party Providers">
        <p>We may share information with service providers that help us provide hosting, storage, payments, email delivery, authentication, analytics, monitoring, customer support, market data, news, filings, government records, and related data services.</p>
        <p>Third-party providers process information under their own terms and privacy practices where applicable.</p>
      </LegalSection>

      <LegalSection title="7. Data Retention">
        <p>We retain information for as long as needed to provide the service, comply with legal and accounting obligations, resolve disputes, enforce agreements, maintain security, and support legitimate business purposes.</p>
      </LegalSection>

      <LegalSection title="8. Security">
        <p>We use reasonable administrative, technical, and organizational safeguards designed to protect information. No system is completely secure, and we cannot guarantee that information will never be accessed, disclosed, altered, or lost.</p>
      </LegalSection>

      <LegalSection title="9. User Choices">
        <p>You may update account details, notification preferences, saved items, and certain settings in the service. You may contact us to request access, correction, deletion, or other action regarding your information, subject to legal, security, and operational limits.</p>
      </LegalSection>

      <LegalSection title="10. Children's Privacy">
        <p>The service is not intended for children. We do not knowingly collect personal information from children under the age required by applicable law.</p>
      </LegalSection>

      <LegalSection title="11. International Users and Cross-Border Processing">
        <p>We may process and store information in Canada, the United States, and other jurisdictions where we or our providers operate. These jurisdictions may have data protection laws different from those in your location.</p>
      </LegalSection>

      <LegalSection title="12. Changes to This Policy">
        <p>We may update this Privacy Policy from time to time. The Last updated date shows when this policy was most recently revised, and continued use of the service means the updated policy applies.</p>
      </LegalSection>

      <LegalSection title="13. Contact">
        <p>Questions about this Privacy Policy may be sent to <a className="text-emerald-200 hover:text-emerald-100" href="mailto:support@walnut-intel.com">support@walnut-intel.com</a>.</p>
      </LegalSection>
    </LegalPageShell>
  );
}
