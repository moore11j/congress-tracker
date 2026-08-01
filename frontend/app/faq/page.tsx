import type { Metadata } from "next";
import { LegalPageShell, LegalSection } from "@/components/landing/LegalPageShell";
import { legalPageChrome } from "@/lib/legalPageChrome";
import { marketingPageMetadata } from "@/lib/marketingMetadata";

const lastUpdated = "July 31, 2026";

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
        question: "What does Gain / Loss mean?",
        answer:
          "Gain / Loss estimates the unrealized return you would have if you made the same disclosed trade. For example, if a Congress member disclosed a purchase and the stock later rose, Walnut may show a positive Gain / Loss. This does not necessarily mean the filer personally realized that gain or still holds the position. It is an estimate based on the disclosure, transaction timing, and latest available EOD prices.",
      },
      {
        question: "Why does Gain / Loss change?",
        answer:
          "Gain / Loss can change as market prices update. Walnut uses cached EOD pricing where available, so the estimate may update after new daily prices are refreshed.",
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
    title: "Research Glossary",
    items: [
      {
        question: "What are fundamentals?",
        answer:
          "Fundamentals are company-level business and financial measures such as revenue, earnings, cash flow, debt, margins, growth, valuation, and balance-sheet strength. They help describe what the company is producing economically, separate from the stock chart.",
      },
      {
        question: "What are technicals?",
        answer:
          "Technicals are market-based measures derived from price, volume, and trend behavior. Examples include moving averages, RSI, MACD, Bollinger Bands, support and resistance, relative volume, and breakout or breakdown patterns.",
      },
      {
        question: "What is P/E ratio?",
        answer:
          "P/E ratio, or price-to-earnings ratio, compares a company's stock price with its earnings per share. A higher P/E can imply investors are paying more for each dollar of earnings, while a lower P/E can imply a cheaper valuation or weaker growth expectations.",
      },
      {
        question: "What is the difference between trailing P/E and forward P/E?",
        answer:
          "Trailing P/E uses earnings already reported over a recent historical period, often the last twelve months. Forward P/E uses expected future earnings estimates. Forward P/E can be useful for growth expectations, but it depends on forecasts that may be wrong.",
      },
      {
        question: "What are EPS and ROE?",
        answer:
          "EPS, or earnings per share, is a company's profit allocated to each share of common stock. ROE, or return on equity, compares net income with shareholder equity and is often used to judge how efficiently a company turns equity capital into profit.",
      },
      {
        question: "What are EBITDA and EV/EBITDA?",
        answer:
          "EBITDA means earnings before interest, taxes, depreciation, and amortization. EV/EBITDA compares enterprise value with EBITDA and is commonly used to compare company valuations while accounting for debt and cash differences.",
      },
      {
        question: "What are margins, gross margins, and profit margins?",
        answer:
          "Margins compare profit with revenue. Gross margin measures revenue left after direct costs of goods or services. Operating margin measures operating income as a share of revenue. Profit margin usually means net income as a share of revenue after all expenses.",
      },
      {
        question: "What is operating margin change?",
        answer:
          "Operating margin change measures whether operating profit as a percentage of revenue is improving or deteriorating over time. Rising operating margin can suggest better efficiency or pricing power, while falling margin can suggest cost pressure or weaker profitability.",
      },
      {
        question: "What is FCF yield?",
        answer:
          "FCF yield, or free cash flow yield, compares free cash flow with a company's market value. It is often used to evaluate how much cash flow investors receive relative to the price they are paying for the business.",
      },
      {
        question: "What are discounted cash flow and market capitalization?",
        answer:
          "Discounted cash flow, or DCF, estimates a business's value by discounting expected future cash flows back to today's dollars. Market capitalization is the stock market value of a company's equity, calculated as share price multiplied by shares outstanding.",
      },
      {
        question: "What is beta?",
        answer:
          "Beta measures how much a stock has historically moved relative to a market benchmark. A beta above 1 has tended to move more than the benchmark, while a beta below 1 has tended to move less.",
      },
      {
        question: "What are SMA and EMA?",
        answer:
          "SMA, or simple moving average, is the average price over a selected period. EMA, or exponential moving average, gives more weight to recent prices. Both are used to evaluate trend direction and possible support or resistance.",
      },
      {
        question: "What are Bollinger Bands?",
        answer:
          "Bollinger Bands plot a moving average with upper and lower bands based on recent volatility. They are often used to identify unusually stretched price moves, volatility expansion, or consolidation.",
      },
      {
        question: "What are RSI and MACD?",
        answer:
          "RSI, or relative strength index, is a momentum indicator that compares recent gains and losses. MACD, or moving average convergence divergence, compares moving averages to evaluate momentum shifts and possible bullish or bearish crossovers.",
      },
      {
        question: "What is Walnut's confirmation score?",
        answer:
          "Walnut's proprietary confirmation score is a research metric that summarizes whether available evidence appears to support or contradict a ticker view. It is separate from the underlying data and is not investment advice, a price target, or a recommendation.",
      },
      {
        question: "What is the difference between institutionals, insiders, and Congress activity?",
        answer:
          "Institutionals refers to reported activity from large investment managers and funds. Insiders refers to reported trades by company officers, directors, or large beneficial owners. Congress activity refers to disclosed trades by members of Congress and covered related filers under congressional disclosure rules.",
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

const faqJsonLd = {
  "@context": "https://schema.org",
  "@type": "FAQPage",
  mainEntity: faqCategories.flatMap((category) =>
    category.items.map((item) => ({
      "@type": "Question",
      name: item.question,
      acceptedAnswer: {
        "@type": "Answer",
        text: item.answer,
      },
    })),
  ),
};

export const metadata: Metadata = marketingPageMetadata("/faq", {
  title: "Frequently Asked Questions | Walnut Markets",
  description:
    "Answers about data sources, disclosures, billing, privacy, security, and how Walnut Intelligence Inc. operates Walnut Market Terminal.",
});

export default async function FaqPage() {
  const chrome = await legalPageChrome();

  return (
    <LegalPageShell
      eyebrow="Support"
      title="Frequently Asked Questions"
      description="Answers about data sources, disclosures, billing, privacy, and how Walnut Market Terminal works."
      lastUpdated={lastUpdated}
      chrome={chrome}
    >
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(faqJsonLd).replace(/</g, "\\u003c") }} />

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
