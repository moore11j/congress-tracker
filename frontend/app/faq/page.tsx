import type { Metadata } from "next";
import { LegalPageShell, LegalSection } from "@/components/landing/LegalPageShell";

const lastUpdated = "June 10, 2026";

const faqCategories = [
  {
    title: "Data & Disclosures",
    items: [
      {
        question: "Why are Congress trade dates often older than insider trade dates?",
        answer:
          "Congress trades are reported under disclosure rules that may allow reporting delays. Insider filings are typically filed much sooner through SEC Form 4 disclosures. As a result, Congress activity often appears after the actual trade date while insider activity may appear much closer to the transaction date.",
      },
      {
        question: "What date am I looking at?",
        answer:
          "Walnut displays both trade dates and filing or report dates where available. Trade dates reflect when the transaction occurred. Report dates reflect when it became publicly available.",
      },
      {
        question: "Why do some disclosures appear days or weeks later?",
        answer:
          "Walnut can only display information once it becomes public through the relevant disclosure process.",
      },
      {
        question: "Why do some tickers have no Congress or insider activity?",
        answer: "Not every company has disclosed activity within the selected window.",
      },
      {
        question: "What are Class A common shares?",
        answer:
          "Class A common shares are a class of ownership shares issued by a company. Companies may issue multiple classes of shares with different voting rights or economic rights.",
      },
      {
        question: "Why are some securities unresolved?",
        answer:
          "Certain disclosures use descriptions that do not map cleanly to a public ticker symbol. Walnut attempts to resolve these automatically and continuously improves coverage.",
      },
    ],
  },
  {
    title: "Signals & Analytics",
    items: [
      {
        question: "What is a signal score?",
        answer:
          "A signal score summarizes multiple sources of information into a single research metric. Higher scores indicate stronger confirmation across the available data sources.",
      },
      {
        question: "Is a signal score a recommendation?",
        answer: "No. Signal scores are research tools and not investment recommendations.",
      },
      {
        question: "How often are signals updated?",
        answer:
          "Signals update as new public disclosures, market data, and supported sources become available.",
      },
      {
        question: "Why did a score change?",
        answer:
          "Scores may change when new filings, disclosures, price behavior, or other contributing data sources change.",
      },
    ],
  },
  {
    title: "Watchlists & Monitoring",
    items: [
      {
        question: "What is the difference between alerts and digests?",
        answer:
          "Alerts are intended for important activity requiring attention. Digests summarize activity over a scheduled period.",
      },
      {
        question: "Why did I not receive an email?",
        answer:
          "Email delivery depends on notification settings, alert eligibility, digest schedules, and account preferences.",
      },
      {
        question: "Can I disable emails?",
        answer: "Yes. Notification settings can be managed from Account Settings.",
      },
    ],
  },
  {
    title: "Billing & Subscriptions",
    items: [
      {
        question: "How do subscriptions work?",
        answer:
          "Subscriptions renew automatically based on the selected billing interval until canceled.",
      },
      {
        question: "Can I cancel at any time?",
        answer: "Yes. Subscriptions can be canceled through the customer billing portal.",
      },
      {
        question: "What happens when I cancel?",
        answer:
          "Access generally remains available through the end of the current billing period unless otherwise stated.",
      },
      {
        question: "How do upgrades and downgrades work?",
        answer: "Plan changes may be prorated depending on billing settings and timing.",
      },
      {
        question: "Can I download invoices?",
        answer: "Yes. Invoices are available through the billing portal.",
      },
    ],
  },
  {
    title: "Privacy & Security",
    items: [
      {
        question: "Do you store my credit card information?",
        answer:
          "No. Payment information is processed and stored by Stripe. Walnut does not store full card numbers.",
      },
      {
        question: "Do you sell my data?",
        answer: "No. Walnut does not sell personal customer data.",
      },
      {
        question: "What information do you collect?",
        answer:
          "Walnut collects account information, subscription information, preferences, watchlists, usage information, and information necessary to operate the service.",
      },
      {
        question: "How is my account protected?",
        answer:
          "Walnut uses authentication controls, encryption where appropriate, and secure third-party providers.",
      },
      {
        question: "Can I delete my account?",
        answer:
          "Yes. The delete account control in Subscriptions & Billing deactivates the account and marks it as deleted. Walnut may retain deleted account records where needed for audit, security, support, legal, or operational reasons.",
      },
      {
        question: "Why do I receive security emails?",
        answer:
          "Security-related emails help protect your account and notify you of important account changes.",
      },
    ],
  },
  {
    title: "Legal",
    items: [
      {
        question: "Is Walnut investment advice?",
        answer: "No. Walnut provides informational and research tools only.",
      },
      {
        question: "Are the disclosures accurate?",
        answer:
          "Walnut aggregates and processes public information from multiple sources. Users should independently verify important information before making decisions.",
      },
      {
        question: "Who operates Walnut Markets?",
        answer:
          "Walnut Markets and Walnut Market Terminal are operated by Walnut Intelligence Inc. Walnut Markets is a market intelligence platform for research and informational purposes only.",
      },
    ],
  },
] as const;

export const metadata: Metadata = {
  title: "Frequently Asked Questions | Walnut Markets",
  description:
    "Answers about data sources, disclosures, billing, privacy, security, and how Walnut Intelligence Inc. operates Walnut Market Terminal.",
};

export default function FaqPage() {
  return (
    <LegalPageShell
      eyebrow="Support"
      title="Frequently Asked Questions"
      description="Answers about data sources, disclosures, billing, privacy, and how Walnut Market Terminal works."
      lastUpdated={lastUpdated}
    >
      {faqCategories.map((category) => (
        <LegalSection key={category.title} title={category.title}>
          <div className="divide-y divide-white/10">
            {category.items.map((item) => (
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
        </LegalSection>
      ))}
    </LegalPageShell>
  );
}
