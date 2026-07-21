import {
  WALNUT_MARKETING_DESCRIPTION,
  WALNUT_MARKETING_URL,
  WALNUT_SOCIAL_IMAGE_URL,
  WALNUT_SOCIAL_URLS,
  marketingCanonicalUrl,
} from "@/lib/marketingMetadata";

export type SeoLandingPageKey =
  | "congressTrades"
  | "insiderTradingTracker"
  | "governmentContracts"
  | "institutionalFilings"
  | "stockConfirmationScore"
  | "marketIntelligenceTerminal";

export type SeoLandingPageSection = {
  title: string;
  paragraphs: string[];
};

export type SeoLandingPageFaq = {
  question: string;
  answer: string;
};

export type SeoLandingPage = {
  pathname: string;
  title: string;
  description: string;
  eyebrow: string;
  h1: string;
  intro: string;
  highlights: string[];
  sections: SeoLandingPageSection[];
  popularTickers?: string[];
  faq: SeoLandingPageFaq[];
  primaryCta: {
    label: string;
    href: string;
  };
};

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");
const loginUrl = `${appUrl}/login`;

export const seoLandingPages: Record<SeoLandingPageKey, SeoLandingPage> = {
  congressTrades: {
    pathname: "/congress-trades",
    title: "Congress Trades Tracker | Walnut Markets",
    description:
      "Track reported U.S. Congress stock trades, disclosure dates, tickers, transaction types, and related market data with Walnut Markets.",
    eyebrow: "Public disclosure research",
    h1: "Congress Trades Tracker",
    intro:
      "Walnut Markets helps investors research reported U.S. Congress stock trades alongside disclosure dates, tickers, transaction types, price and volume context, fundamentals, insider activity, government contracts, institutional filings, and Walnut's proprietary confirmation score.",
    highlights: [
      "Reported House and Senate stock trade disclosures",
      "Disclosure timing, transaction type, ticker, and issuer context",
      "Research-only workflow with no buy or sell recommendations",
    ],
    sections: [
      {
        title: "What are Congress trades?",
        paragraphs: [
          "Congress trades are stock, option, or other reportable securities transactions disclosed by members of Congress or related filers under public disclosure rules. Walnut organizes reported Congress activity so researchers can review the ticker, filer, transaction type, reported amount range, trade date where available, and disclosure date.",
          "The key word is reported. Walnut displays public disclosures after they become available and does not imply real-time access to congressional trading activity.",
        ],
      },
      {
        title: "Why disclosure timing matters",
        paragraphs: [
          "Congressional disclosures often arrive after the transaction date. A trade can be disclosed days or weeks after it occurred, so the disclosure date is central to understanding what the market could know at the time the information became public.",
          "Walnut separates transaction timing from disclosure timing to help researchers avoid confusing a historical trade date with a current market signal.",
        ],
      },
      {
        title: "How Walnut tracks reported Congress activity",
        paragraphs: [
          "Walnut collects reported and disclosed Congress activity, resolves ticker context where possible, and presents the information in a searchable market research workflow. Users can review members, symbols, transaction categories, filing dates, and the market context surrounding the public disclosure.",
          "Walnut is designed for verification. It treats Congress disclosures as one category of public data, not as instructions to trade.",
        ],
      },
      {
        title: "How Congress data fits with price/volume, fundamentals, insider activity, contracts, and Walnut's proprietary confirmation score",
        paragraphs: [
          "Congress data is most useful when it is compared with other market information. Walnut places reported Congress trades next to price and volume behavior, company fundamentals, reported insider activity, government contract awards, institutional filings, and ticker-level context.",
          "Walnut's proprietary confirmation score is separate from those underlying data categories. The score helps summarize cross-source context, but it does not reveal a formula, replace the underlying data, or provide investment advice.",
        ],
      },
      {
        title: "Popular ticker examples",
        paragraphs: [
          "Researchers often start with widely followed tickers such as NVDA, AAPL, MSFT, TSLA, PLTR, and LMT to see how public disclosures line up with broader market context.",
        ],
      },
      {
        title: "Research only. Not investment advice.",
        paragraphs: [
          "Walnut Markets provides public data tools for informational research. Reported Congress trades can be incomplete, delayed, amended, or difficult to map to a public ticker. Walnut does not provide personalized advice, recommendations, or instructions to buy or sell any security.",
        ],
      },
    ],
    popularTickers: ["NVDA", "AAPL", "MSFT", "TSLA", "PLTR", "LMT"],
    faq: [
      {
        question: "Are Congress trades shown in real time?",
        answer:
          "No. Walnut displays reported Congress trades after they are disclosed through public reporting channels.",
      },
      {
        question: "Does Walnut recommend buying what members of Congress disclose?",
        answer:
          "No. Walnut is a research platform and does not provide buy or sell recommendations.",
      },
    ],
    primaryCta: {
      label: "View Congress Trades",
      href: `${appUrl}/feed?mode=congress`,
    },
  },
  insiderTradingTracker: {
    pathname: "/insider-trading-tracker",
    title: "Insider Trading Tracker | Walnut Markets",
    description:
      "Track reported insider buying and selling activity, Form 4 disclosures, ticker context, and market data with Walnut Markets.",
    eyebrow: "SEC disclosure research",
    h1: "Insider Trading Tracker",
    intro:
      "Walnut Markets tracks reported insider activity from public disclosure data, including Form 4 disclosures, transaction context, issuer details, and related market data.",
    highlights: [
      "Reported insider activity from public SEC filings",
      "Form 4 disclosures with ticker and issuer context",
      "Research workflow that does not imply illegal insider trading",
    ],
    sections: [
      {
        title: "Reported insider activity from public disclosures",
        paragraphs: [
          "Insider activity refers to legally reported transactions by corporate officers, directors, and other required filers. Walnut uses public disclosure data and Form 4 disclosures to help researchers review reported insider buying and selling activity.",
          "The phrase insider trading tracker on this page refers to disclosed public-company insider transactions. It does not imply illegal insider trading.",
        ],
      },
      {
        title: "Why Form 4 disclosures matter",
        paragraphs: [
          "Form 4 disclosures can show purchases, sales, option exercises, awards, and ownership changes by covered insiders. Walnut organizes that data with issuer, role, transaction type, transaction date, filing date, and ticker context where available.",
        ],
      },
      {
        title: "How Walnut adds ticker context",
        paragraphs: [
          "A single insider transaction is only one piece of research data. Walnut places reported insider activity next to price and volume behavior, fundamentals, Congress disclosures, institutional filings, government contracts, and Walnut's proprietary confirmation score.",
        ],
      },
      {
        title: "Research only. Not investment advice.",
        paragraphs: [
          "Reported insider activity can be routine, planned, tax-related, compensation-related, amended, or otherwise limited. Walnut provides public data for research and does not recommend buying or selling securities.",
        ],
      },
    ],
    faq: [
      {
        question: "Is this page about illegal insider trading?",
        answer:
          "No. Walnut tracks reported insider activity from public Form 4 disclosures and related public-company filings.",
      },
      {
        question: "Can insider filings be delayed or amended?",
        answer:
          "Yes. Public filings can be corrected, amended, or interpreted differently as additional context becomes available.",
      },
    ],
    primaryCta: {
      label: "View Insider Activity",
      href: `${appUrl}/feed?mode=insider`,
    },
  },
  governmentContracts: {
    pathname: "/government-contracts",
    title: "Government Contracts Tracker | Walnut Markets",
    description:
      "Track government contract awards, public disclosure activity, and ticker context in Walnut Markets.",
    eyebrow: "Contract award research",
    h1: "Government Contracts Tracker",
    intro:
      "Walnut Markets helps investors research government contract awards, public disclosure activity, issuer exposure, and ticker context in one market intelligence workflow.",
    highlights: [
      "Government contract awards and modifications",
      "Ticker context for public companies with contract exposure",
      "Research-only market intelligence, not investment advice",
    ],
    sections: [
      {
        title: "What government contract data can show",
        paragraphs: [
          "Government contract awards can help researchers understand which public companies are receiving disclosed public-sector work, when awards are announced, and how that activity may relate to the issuer's broader market context.",
        ],
      },
      {
        title: "How Walnut connects contracts to tickers",
        paragraphs: [
          "Walnut maps government contract information to ticker context where possible and keeps the award details separate from price, volume, fundamentals, institutional filings, insider activity, Congress disclosures, and Walnut's proprietary confirmation score.",
        ],
      },
      {
        title: "Why contract context matters",
        paragraphs: [
          "Contract awards can be material, immaterial, recurring, modified, or unrelated to the public company's main investment story. Walnut presents the data as research context so users can compare it against company fundamentals and other public disclosures.",
        ],
      },
      {
        title: "Research only. Not investment advice.",
        paragraphs: [
          "A disclosed contract award is not a buy or sell recommendation. Walnut Markets provides public data tools for informational research and expects users to verify important information independently.",
        ],
      },
    ],
    faq: [
      {
        question: "Does a government contract award mean a stock should move?",
        answer:
          "No. Contract awards require context, including size, timing, issuer exposure, margins, and whether the market already expected the work.",
      },
      {
        question: "Does Walnut keep contract data separate from its score?",
        answer:
          "Yes. Government contract data is an underlying research category. Walnut's proprietary confirmation score is a separate interpretive metric.",
      },
    ],
    primaryCta: {
      label: "View Contract Activity",
      href: `${appUrl}/feed?mode=government_contracts`,
    },
  },
  institutionalFilings: {
    pathname: "/institutional-filings",
    title: "Institutional Filings Tracker | Walnut Markets",
    description:
      "Track reported institutional filings, 13F activity, ticker context, and market data in Walnut Markets.",
    eyebrow: "13F filing research",
    h1: "Institutional Filings Tracker",
    intro:
      "Walnut Markets helps investors research reported institutional activity, 13F activity, filing date context, quarter-end holdings, ticker details, and related market data.",
    highlights: [
      "Reported institutional activity from public filings",
      "13F activity with filing date and quarter-end holdings context",
      "No implication of live institutional buying",
    ],
    sections: [
      {
        title: "What institutional filings show",
        paragraphs: [
          "Institutional filings can show reported institutional activity such as quarter-end holdings disclosed through 13F filings and related public reports. Walnut organizes that information by institution, issuer, ticker, filing date, and reported holdings context.",
        ],
      },
      {
        title: "Filing date versus quarter-end holdings",
        paragraphs: [
          "The filing date is when the information becomes publicly available. Quarter-end holdings describe a historical reporting date, not live institutional buying. Walnut keeps those dates visible so researchers can understand timing and limitations.",
        ],
      },
      {
        title: "How Walnut adds market context",
        paragraphs: [
          "Reported institutional activity can be compared with price and volume behavior, fundamentals, Congress disclosures, reported insider activity, government contracts, and Walnut's proprietary confirmation score.",
        ],
      },
      {
        title: "Research only. Not investment advice.",
        paragraphs: [
          "Institutional filings can be delayed, amended, aggregated, or incomplete for a research question. Walnut does not treat filings as real-time positioning or as recommendations to buy or sell securities.",
        ],
      },
    ],
    faq: [
      {
        question: "Are 13F holdings live institutional buys?",
        answer:
          "No. 13F data reflects reported institutional activity and quarter-end holdings disclosed after the reporting period.",
      },
      {
        question: "Why does the filing date matter?",
        answer:
          "The filing date shows when the information became public, which can be different from the quarter-end holdings date.",
      },
    ],
    primaryCta: {
      label: "View Institutional Filings",
      href: `${appUrl}/feed?mode=institutional`,
    },
  },
  stockConfirmationScore: {
    pathname: "/stock-confirmation-score",
    title: "Stock Confirmation Score | Walnut Markets",
    description:
      "Learn how Walnut's proprietary confirmation score helps investors interpret market data across price/volume, fundamentals, public disclosures, and ticker context.",
    eyebrow: "Proprietary research metric",
    h1: "Stock Confirmation Score",
    intro:
      "Walnut's proprietary confirmation score helps investors interpret market data across price and volume, fundamentals, public disclosures, and ticker context without replacing the underlying data.",
    highlights: [
      "Proprietary score built for research context",
      "Separate from the underlying data categories",
      "No formula disclosure and no investment advice",
    ],
    sections: [
      {
        title: "What the confirmation score is",
        paragraphs: [
          "Walnut's proprietary confirmation score is an interpretive research metric designed to summarize whether different categories of market information appear to support or contradict a ticker research view.",
          "The score is separate from the underlying data. Users should still review the actual price and volume behavior, fundamentals, public disclosures, and ticker context behind any score.",
        ],
      },
      {
        title: "What the score is not",
        paragraphs: [
          "The confirmation score is not investment advice, not a price target, not a rating from an adviser, and not a formula Walnut discloses publicly. It should not be used as a standalone basis for investment decisions.",
        ],
      },
      {
        title: "How the score fits into research",
        paragraphs: [
          "Walnut places the proprietary score near the supporting data categories so researchers can inspect the evidence, compare sources, and decide what deserves further review.",
        ],
      },
      {
        title: "Research only. Not investment advice.",
        paragraphs: [
          "Walnut Markets provides informational tools. Scores, screens, alerts, disclosures, and market data are not personalized recommendations to buy, sell, or hold any security.",
        ],
      },
    ],
    faq: [
      {
        question: "Does Walnut reveal the confirmation score formula?",
        answer:
          "No. The confirmation score is proprietary, and Walnut does not publish the formula.",
      },
      {
        question: "Is the confirmation score the same as the underlying data?",
        answer:
          "No. The score is separate from the underlying categories such as price and volume, fundamentals, and public disclosures.",
      },
    ],
    primaryCta: {
      label: "Explore Confirmation Score",
      href: `${appUrl}/ticker/NVDA`,
    },
  },
  marketIntelligenceTerminal: {
    pathname: "/market-intelligence-terminal",
    title: "Market Intelligence Terminal | Walnut Markets",
    description:
      "Walnut Markets is a market intelligence terminal for investors tracking technicals, fundamentals, public disclosures, and ticker context.",
    eyebrow: "Walnut Market Terminal",
    h1: "Market Intelligence Terminal",
    intro:
      "Walnut Markets is a market intelligence terminal for investors tracking technicals, fundamentals, public disclosures, ticker context, watchlists, screeners, and proprietary confirmation data.",
    highlights: [
      "Technicals, fundamentals, disclosures, and ticker research",
      "Congress trades, insider activity, contracts, and filings in context",
      "Built for research and informational use",
    ],
    sections: [
      {
        title: "What Walnut Markets is",
        paragraphs: [
          "Walnut Markets is the public brand for Walnut Market Terminal, a research platform operated by Walnut Intelligence Inc. The product brings market data and public disclosure context into a single workflow for ticker research.",
        ],
      },
      {
        title: "What investors can research",
        paragraphs: [
          "Users can research technicals, fundamentals, Congress trades, reported insider activity, government contracts, reported institutional filings, market pressure, event context, and proprietary confirmation data.",
        ],
      },
      {
        title: "How Walnut supports ticker research",
        paragraphs: [
          "Walnut is designed to help researchers move from a ticker to the surrounding evidence. The terminal emphasizes source context, dates, issuer details, and cross-source confirmation without presenting those signals as personalized advice.",
        ],
      },
      {
        title: "Research only. Not investment advice.",
        paragraphs: [
          "Walnut Markets provides market intelligence tools for informational research. Walnut does not provide investment advice, brokerage services, or recommendations to buy or sell securities.",
        ],
      },
    ],
    faq: [
      {
        question: "Who operates Walnut Markets?",
        answer:
          "Walnut Markets and Walnut Market Terminal are operated by Walnut Intelligence Inc.",
      },
      {
        question: "Is Walnut a brokerage or investment adviser?",
        answer:
          "No. Walnut is a market intelligence terminal for research and informational purposes only.",
      },
    ],
    primaryCta: {
      label: "Launch Terminal",
      href: appUrl,
    },
  },
};

export function seoLandingPageJsonLd(page: SeoLandingPage) {
  const canonicalUrl = marketingCanonicalUrl(page.pathname);
  const organization = {
    "@context": "https://schema.org",
    "@type": "Organization",
    name: "Walnut Markets",
    legalName: "Walnut Intelligence Inc.",
    url: WALNUT_MARKETING_URL,
    logo: `${WALNUT_MARKETING_URL}/walnut-intel-logo-mark.png`,
    description: WALNUT_MARKETING_DESCRIPTION,
    sameAs: WALNUT_SOCIAL_URLS,
  };

  const website = {
    "@context": "https://schema.org",
    "@type": "WebSite",
    name: "Walnut Markets",
    url: WALNUT_MARKETING_URL,
    publisher: {
      "@type": "Organization",
      name: "Walnut Intelligence Inc.",
    },
  };

  const application = {
    "@context": "https://schema.org",
    "@type": "SoftwareApplication",
    name: "Walnut Market Terminal",
    brand: {
      "@type": "Brand",
      name: "Walnut Markets",
    },
    applicationCategory: "FinanceApplication",
    operatingSystem: "Web",
    url: WALNUT_MARKETING_URL,
    image: WALNUT_SOCIAL_IMAGE_URL,
    description: WALNUT_MARKETING_DESCRIPTION,
    publisher: {
      "@type": "Organization",
      name: "Walnut Intelligence Inc.",
    },
  };

  const breadcrumb = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: [
      {
        "@type": "ListItem",
        position: 1,
        name: "Walnut Markets",
        item: WALNUT_MARKETING_URL,
      },
      {
        "@type": "ListItem",
        position: 2,
        name: page.h1,
        item: canonicalUrl,
      },
    ],
  };

  const webPage = {
    "@context": "https://schema.org",
    "@type": "WebPage",
    name: page.title,
    url: canonicalUrl,
    description: page.description,
    isPartOf: {
      "@type": "WebSite",
      name: "Walnut Markets",
      url: WALNUT_MARKETING_URL,
    },
    about: {
      "@type": "SoftwareApplication",
      name: "Walnut Market Terminal",
    },
  };

  const faq = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: page.faq.map((item) => ({
      "@type": "Question",
      name: item.question,
      acceptedAnswer: {
        "@type": "Answer",
        text: item.answer,
      },
    })),
  };

  return [organization, website, application, breadcrumb, webPage, faq];
}

export { loginUrl };
