import type { Metadata } from "next";
import { ContactForm } from "@/components/landing/ContactForm";
import { LegalPageShell, LegalSection } from "@/components/landing/LegalPageShell";
import { legalPageChrome } from "@/lib/legalPageChrome";
import { marketingPageMetadata } from "@/lib/marketingMetadata";

const lastUpdated = "August 8, 2026";

export const metadata: Metadata = marketingPageMetadata("/contact", {
  title: "Contact Walnut Markets | Support",
  description: "Contact Walnut Markets for feedback, bug reports, feature requests, and general inquiries.",
});

export default async function ContactPage() {
  const chrome = await legalPageChrome();

  return (
    <LegalPageShell
      eyebrow="Support"
      title="Contact Walnut Markets"
      description="Send feedback, report a bug, request a feature, or ask a general question. Messages are directed to support@walnutmarkets.com."
      lastUpdated={lastUpdated}
      chrome={chrome}
    >
      <LegalSection title="Send a Message">
        <ContactForm />
      </LegalSection>
    </LegalPageShell>
  );
}
