import {
  WALNUT_MARKETING_DESCRIPTION,
  WALNUT_MARKETING_URL,
  WALNUT_SOCIAL_IMAGE_URL,
  WALNUT_SOCIAL_URLS,
  marketingCanonicalUrl,
} from "@/lib/marketingMetadata";

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");

type FeatureCta = {
  label: string;
  href: string;
};

type FeatureCard = {
  title: string;
  body: string;
  href?: string;
};

type FeatureFaq = {
  question: string;
  answer: string;
};

export type CommercialFeaturePage = {
  key: string;
  pathname: string;
  title: string;
  description: string;
  breadcrumbLabel: string;
  eyebrow: string;
  h1: string;
  intro: string;
  targetUser: string;
  imageAlt: string;
  highlights: string[];
  problem: string[];
  approach: string[];
  workflow: FeatureCard[];
  journeyTicker: string;
  journey: string[];
  access: string;
  relatedLinks: FeatureCard[];
  faq: FeatureFaq[];
  primaryCta: FeatureCta;
  secondaryCta: FeatureCta;
};

export const commercialFeaturePages = {
  stockResearchSoftware: {
    key: "stock-research-software",
    pathname: "/stock-research-software",
    title: "Stock Research Software for Investors | Walnut Markets",
    description:
      "Use Walnut Markets stock research software to review tickers, connect multiple data sources, read research briefs, and monitor what changed before making a decision.",
    breadcrumbLabel: "Stock Research Software",
    eyebrow: "Stock research software",
    h1: "Stock research software for investors who need more than a quote page.",
    intro:
      "Walnut helps investors move from a ticker to a researched view of what changed, what supports the thesis, what could weaken it, and what deserves another look.",
    targetUser:
      "Built for investors comparing end-to-end research tools, not for people who only need a fast price quote or a single chart.",
    imageAlt: "Walnut Markets stock comparison interface showing research context for two tickers.",
    highlights: [
      "Ticker research with price, volume, fundamentals, disclosures, and related context",
      "Research briefs that summarize what changed, risks, catalysts, and next questions",
      "Watchlists, screens, comparisons, and monitoring for repeatable research",
    ],
    problem: [
      "Most stock research starts in pieces: a quote page, a screener, a filing search, a news tab, and a notebook where the actual judgment lives.",
      "That can work, but it makes it easy to miss what changed or overweight one piece of data because it was the easiest to find.",
    ],
    approach: [
      "Walnut keeps the research job centered on the ticker. Price and volume, fundamentals, Congress activity, insider activity, institutional activity, government contracts, and research briefs sit close to the investment question.",
      "The confirmation score is a separate Walnut metric. It helps frame whether available evidence points in the same direction, while leaving the underlying data visible for review.",
    ],
    workflow: [
      {
        title: "Open the ticker",
        body: "Start with a company page and review the current market setup, fundamentals, ownership context, disclosures, and related research.",
        href: `${appUrl}/ticker/NVDA`,
      },
      {
        title: "Read the brief",
        body: "Use research briefs when the question needs more than a table: what changed, likely catalysts, key risks, and what to watch next.",
        href: `${appUrl}/insights`,
      },
      {
        title: "Monitor the thesis",
        body: "Save tickers to watchlists, set up screens, and come back when new activity changes the research case.",
        href: `${appUrl}/watchlists`,
      },
    ],
    journeyTicker: "NVDA",
    journey: [
      "Open NVDA and check the current price and volume setup.",
      "Compare fundamentals and disclosure activity against the existing thesis.",
      "Review the confirmation score as a separate research metric, then inspect the data behind it.",
      "Save the ticker to a watchlist if the next catalyst or risk is worth monitoring.",
    ],
    access:
      "Free access supports a starter workflow. Premium expands confirmation, comparison, alerts, and watchlists. Pro adds higher limits and advanced data layers such as institutional activity and market pressure.",
    relatedLinks: [
      { title: "Stock analysis platform", body: "See the page focused on combined technical, fundamental, ownership, and event-driven analysis.", href: "/stock-analysis-platform" },
      { title: "Compare Walnut", body: "Review Walnut against specialist research and market-data tools.", href: "/compare" },
      { title: "Pricing", body: "Check current Free, Premium, and Pro plan access.", href: "/pricing" },
    ],
    faq: [
      {
        question: "Is Walnut Markets stock research software or a brokerage?",
        answer: "Walnut is research software. It does not place trades or provide personalized investment advice.",
      },
      {
        question: "Does Walnut write research briefs?",
        answer: "Yes. Walnut includes research briefs that organize ticker context, thesis changes, risks, catalysts, and what to watch next.",
      },
      {
        question: "Can I start without paying?",
        answer: "Yes. The Free plan supports starter research with limits. Paid plans expand access and workflow depth.",
      },
    ],
    primaryCta: { label: "Research NVDA in Walnut", href: `${appUrl}/ticker/NVDA` },
    secondaryCta: { label: "Compare stock research tools", href: "/compare" },
  },
  stockAnalysisPlatform: {
    key: "stock-analysis-platform",
    pathname: "/stock-analysis-platform",
    title: "Stock Analysis Platform for Multi-Source Research | Walnut Markets",
    description:
      "Walnut Markets combines price and volume, fundamentals, Congress trades, insider activity, institutional activity, and research context in one stock analysis platform.",
    breadcrumbLabel: "Stock Analysis Platform",
    eyebrow: "Stock analysis platform",
    h1: "A stock analysis platform for checking whether the evidence lines up.",
    intro:
      "Walnut brings technical, fundamental, ownership, and event-driven data into one workflow so investors can move from market activity to a clearer research judgment.",
    targetUser:
      "Best for investors who already have a ticker in mind and want to test the setup across more than one source of evidence.",
    imageAlt: "Walnut Markets interface comparing stock evidence across market and research data.",
    highlights: [
      "Price and volume context next to company fundamentals",
      "Congress, insider, institutional, and government-contract context where available",
      "Cross-source interpretation with Walnut's proprietary confirmation score",
    ],
    problem: [
      "Technical strength can look convincing until fundamentals, filings, or ownership context tell a different story.",
      "A stock analysis platform should help investors compare the evidence, not just collect more tabs.",
    ],
    approach: [
      "Walnut groups market behavior, business quality, public disclosures, ownership data, and research notes around the ticker.",
      "The goal is not to flatten every source into one answer. It is to show where the data agrees, where it conflicts, and what needs a second look.",
    ],
    workflow: [
      {
        title: "Market setup",
        body: "Review price action, volume behavior, liquidity, trend state, and market pressure before moving deeper.",
        href: `${appUrl}/ticker/NVDA`,
      },
      {
        title: "Business context",
        body: "Check valuation, growth, margins, leverage, cash flow, returns, and earnings quality from the ticker view.",
        href: `${appUrl}/ticker/NVDA`,
      },
      {
        title: "Ownership and events",
        body: "Add Congress, insider, institutional, and government-contract context when those sources matter to the stock story.",
        href: "/alternative-data-stock-analysis",
      },
    ],
    journeyTicker: "MSFT",
    journey: [
      "Start with MSFT and inspect whether price and volume support the current trend.",
      "Check fundamentals to see whether the market setup has business support.",
      "Review ownership and disclosure activity for context, not as standalone trade instructions.",
      "Use the confirmation score to frame the current evidence, then inspect the underlying sections.",
    ],
    access:
      "Core research starts on Free. Premium adds fuller confirmation and comparison workflows. Pro is the better fit when institutional activity, market pressure, and higher limits are part of the research process.",
    relatedLinks: [
      { title: "Stock research software", body: "See Walnut's broader research workflow for briefs, watchlists, and monitoring.", href: "/stock-research-software" },
      { title: "Stock analysis tools", body: "Open the current directory of Walnut analysis surfaces.", href: "/stock-analysis-tools" },
      { title: "Compare Walnut", body: "Compare Walnut with market-data, screener, and alternative-data platforms.", href: "/compare" },
    ],
    faq: [
      {
        question: "Does Walnut include technical analysis?",
        answer: "Walnut includes price, volume, trend, liquidity, and setup context, but it is not positioned as an advanced charting automation platform.",
      },
      {
        question: "Does Walnut include fundamentals?",
        answer: "Yes. Walnut includes company fundamentals in the ticker research workflow where the data is available.",
      },
      {
        question: "How is the confirmation score used?",
        answer: "It is a proprietary research metric shown separately from the underlying data. It should not replace reviewing the evidence.",
      },
    ],
    primaryCta: { label: "Open a ticker analysis", href: `${appUrl}/ticker/MSFT` },
    secondaryCta: { label: "View pricing", href: "/pricing" },
  },
  insiderTradingAnalysisSoftware: {
    key: "insider-trading-analysis-software",
    pathname: "/insider-trading-analysis-software",
    title: "Insider Trading Analysis Software | Walnut Markets",
    description:
      "Research reported Form 4 insider activity with ticker context, filing dates, transaction history, and cross-source stock analysis in Walnut Markets.",
    breadcrumbLabel: "Insider Trading Analysis Software",
    eyebrow: "Insider trading analysis software",
    h1: "Insider trading analysis software for reading Form 4 activity in context.",
    intro:
      "Walnut tracks reported insider activity from public filings and places it next to the ticker's price action, fundamentals, Congress activity, institutional context, and research notes.",
    targetUser:
      "Useful for investors who want to understand insider behavior without treating every reported transaction as predictive.",
    imageAlt: "Walnut Markets stock research interface with disclosure and ticker context.",
    highlights: [
      "Reported insider transactions from public filing data",
      "Filing and transaction context at the ticker level",
      "Cross-checking insider activity against other market and company data",
    ],
    problem: [
      "An insider purchase, sale, grant, or option exercise can mean different things depending on role, timing, plan status, company context, and market setup.",
      "Looking at Form 4 data alone can make normal compensation or planned transactions feel more important than they are.",
    ],
    approach: [
      "Walnut presents reported insider activity as one research input. The same ticker workflow can also show price and volume behavior, fundamentals, Congress activity, institutional activity, contracts, and research briefs.",
      "The wording matters: this page is about legal public-company insider disclosures, not illegal insider trading.",
    ],
    workflow: [
      {
        title: "Review the filing",
        body: "Check transaction type, role, issuer context, transaction date, and filing date where available.",
        href: "/insider-trading-tracker",
      },
      {
        title: "Open the related ticker",
        body: "Move from the filing to ticker-level context so the activity is not interpreted in isolation.",
        href: `${appUrl}/ticker/AAPL`,
      },
      {
        title: "Compare with other sources",
        body: "Look for agreement or conflict across market behavior, fundamentals, institutional activity, and other disclosures.",
        href: "/stock-analysis-platform",
      },
    ],
    journeyTicker: "AAPL",
    journey: [
      "Find recent reported insider activity tied to AAPL.",
      "Check whether the transaction was a sale, purchase, award, option exercise, or another filing type.",
      "Open AAPL to compare the filing with current price, volume, fundamentals, and broader research context.",
      "Treat the insider activity as a research input, not a prediction by itself.",
    ],
    access:
      "Reported insider activity is part of Walnut's research surface. Premium and Pro expand the surrounding workflow with confirmation, monitoring, comparisons, higher limits, and advanced data layers.",
    relatedLinks: [
      { title: "Insider trading tracker", body: "Read the public overview of Walnut's reported insider activity workflow.", href: "/insider-trading-tracker" },
      { title: "Alternative data stock analysis", body: "See how insider activity fits with Congress, institutions, contracts, and options data where available.", href: "/alternative-data-stock-analysis" },
      { title: "Compare with Insider Screener", body: "Review Walnut's broader research workflow against an insider-focused specialist.", href: "/compare/walnut-markets-vs-insider-screener" },
    ],
    faq: [
      {
        question: "Does insider activity predict stock returns?",
        answer: "Not reliably on its own. Walnut treats reported insider activity as context that should be checked against other market and company data.",
      },
      {
        question: "Does Walnut show illegal insider trading?",
        answer: "No. Walnut tracks reported insider activity from public filings and related public-company disclosure data.",
      },
      {
        question: "Why do filing dates matter?",
        answer: "The filing date shows when the information became public. It can differ from the transaction date.",
      },
    ],
    primaryCta: { label: "View insider activity", href: `${appUrl}/feed?mode=insider` },
    secondaryCta: { label: "Read the insider tracker page", href: "/insider-trading-tracker" },
  },
  alternativeDataStockAnalysis: {
    key: "alternative-data-stock-analysis",
    pathname: "/alternative-data-stock-analysis",
    title: "Alternative Data Stock Analysis | Walnut Markets",
    description:
      "Use Walnut Markets to research alternative stock data including Congress activity, insider activity, institutional filings, government contracts, and available options context.",
    breadcrumbLabel: "Alternative Data Stock Analysis",
    eyebrow: "Alternative data stock analysis",
    h1: "Alternative data stock analysis without losing the ticker context.",
    intro:
      "Walnut helps investors examine nontraditional market data next to price, volume, fundamentals, and research judgment instead of treating alternative data as a shortcut.",
    targetUser:
      "Best for investors who want to use Congress, insider, institutional, contract, or options-related context carefully, with the limitations visible.",
    imageAlt: "Walnut Markets product screen showing stock research context and comparison evidence.",
    highlights: [
      "Congress activity, insider activity, institutional filings, and government contracts",
      "Options flow where available and plan-gated",
      "Interpretation that keeps alternative data tied to the stock thesis",
    ],
    problem: [
      "Alternative data is easy to overread. A disclosure, contract, or filing can look important before you know size, timing, issuer exposure, or whether the market already expected it.",
      "The useful question is not whether a data point is unusual. It is whether it changes the research case.",
    ],
    approach: [
      "Walnut keeps alternative data close to the ticker. Congress disclosures, insider filings, institutional holdings, government contracts, and available options context can be compared against price and volume, fundamentals, and research notes.",
      "Walnut's decision layer focuses on interpretation: what the data says, what changed, what could weaken the thesis, and what to watch next.",
    ],
    workflow: [
      {
        title: "Start with the data source",
        body: "Review the disclosure, filing, contract, or available options context and keep timing limitations visible.",
        href: "/stock-analysis-tools",
      },
      {
        title: "Check ticker impact",
        body: "Open the associated ticker to see whether the event fits the broader market and business context.",
        href: `${appUrl}/ticker/LMT`,
      },
      {
        title: "Use judgment, not shortcuts",
        body: "Compare the source with price, volume, fundamentals, and research notes before deciding whether it matters.",
        href: "/stock-research-software",
      },
    ],
    journeyTicker: "LMT",
    journey: [
      "Review government contract or disclosure activity connected to LMT.",
      "Check the filing or announcement timing before treating it as current information.",
      "Open the ticker to compare that activity with fundamentals, price behavior, and related risks.",
      "Use the research brief or monitoring workflow if the data changes the thesis.",
    ],
    access:
      "Congress, insider, and contract research are part of Walnut's public research surface. Institutional activity, market pressure, and options-related data are Pro-oriented or availability-gated where applicable.",
    relatedLinks: [
      { title: "Congress trades tracker", body: "Understand reported Congress disclosures and timing limits.", href: "/congress-trades" },
      { title: "Institutional activity tracker", body: "See the Phase 2 page focused on institutional holdings and filing lag.", href: "/institutional-activity-tracker" },
      { title: "Government contracts tracker", body: "Review how Walnut presents contract data as ticker context.", href: "/government-contracts" },
    ],
    faq: [
      {
        question: "What alternative data does Walnut include?",
        answer: "Walnut includes Congress activity, insider activity, institutional activity, government contracts, and options context where available.",
      },
      {
        question: "Is alternative data enough to make a trade?",
        answer: "No. Walnut presents alternative data as research context. It should be checked against fundamentals, market behavior, and risks.",
      },
      {
        question: "Does Walnut include options flow?",
        answer: "Options flow is treated as an availability-gated Pro data layer where available, not the center of Walnut's product positioning.",
      },
    ],
    primaryCta: { label: "Research LMT in Walnut", href: `${appUrl}/ticker/LMT` },
    secondaryCta: { label: "Compare alternative data platforms", href: "/compare/walnut-markets-vs-quiver-quant" },
  },
  institutionalActivityTracker: {
    key: "institutional-activity-tracker",
    pathname: "/institutional-activity-tracker",
    title: "Institutional Activity Tracker | Walnut Markets",
    description:
      "Track reported institutional holdings, position changes, filing dates, and ticker context in Walnut Markets without implying real-time institutional trading visibility.",
    breadcrumbLabel: "Institutional Activity Tracker",
    eyebrow: "Institutional activity tracker",
    h1: "Institutional activity tracking with filing timing kept in view.",
    intro:
      "Walnut helps investors research reported institutional holdings and position changes alongside the ticker context needed to interpret them.",
    targetUser:
      "For investors who want institutional ownership context while respecting that public filings are delayed, historical, and sometimes incomplete for a live trading question.",
    imageAlt: "Walnut Markets stock research interface with ownership and ticker context.",
    highlights: [
      "Reported holdings and position changes from public filing data",
      "Filing date and reporting-period context",
      "Integration with ticker research, watchlists, and Pro data layers",
    ],
    problem: [
      "Institutional filings can be useful, but they do not show live buying or selling. A position can change after the reporting period and before the public filing appears.",
      "Without timing context, institutional data can be mistaken for current conviction.",
    ],
    approach: [
      "Walnut presents institutional activity as reported data with filing limitations visible. Investors can review institutions, issuers, holdings context, and ticker-level research in the same workflow.",
      "The institutional layer is most useful when compared with fundamentals, price and volume, insider activity, Congress disclosures, contracts, and research notes.",
    ],
    workflow: [
      {
        title: "Review reported holdings",
        body: "Look at reported institution, issuer, filing date, and position context instead of assuming live trading visibility.",
        href: "/institutional-filings",
      },
      {
        title: "Open the ticker",
        body: "Move from a reported holder or position change into the stock's broader research context.",
        href: `${appUrl}/ticker/NVDA`,
      },
      {
        title: "Track what changes",
        body: "Use watchlists and monitoring when ownership context is one part of an active research thesis.",
        href: `${appUrl}/watchlists`,
      },
    ],
    journeyTicker: "NVDA",
    journey: [
      "Start with reported institutional activity tied to NVDA.",
      "Check the reporting period and filing date before interpreting the position.",
      "Open NVDA to compare ownership context with fundamentals, market behavior, and disclosures.",
      "Use Pro-level workflows when institutional activity is central to the research process.",
    ],
    access:
      "Institutional activity, institutional filters, market pressure, and related higher-limit workflows are Pro-level features in the current public plan model.",
    relatedLinks: [
      { title: "Institutional filings tracker", body: "Read the current public overview of Walnut's institutional filing workflow.", href: "/institutional-filings" },
      { title: "Pricing", body: "Confirm current Pro plan access for institutional activity.", href: "/pricing" },
      { title: "Stock analysis platform", body: "See how ownership context fits with fundamentals, price action, and disclosures.", href: "/stock-analysis-platform" },
    ],
    faq: [
      {
        question: "Does Walnut show real-time institutional trading?",
        answer: "No. Public institutional filings are delayed and historical. Walnut keeps filing timing visible so the data is not misread.",
      },
      {
        question: "Which Walnut plan includes institutional activity?",
        answer: "Institutional activity and related filters are Pro-level features in the current public plan configuration.",
      },
      {
        question: "Can institutional filings be incomplete for a research question?",
        answer: "Yes. Filings can be delayed, amended, aggregated, or limited by disclosure rules. They should be checked against other research data.",
      },
    ],
    primaryCta: { label: "Open institutional filings", href: `${appUrl}/feed?mode=institutional` },
    secondaryCta: { label: "View Pro pricing", href: "/pricing" },
  },
} satisfies Record<string, CommercialFeaturePage>;

export const commercialFeaturePageList = Object.values(commercialFeaturePages);

export function commercialFeaturePageJsonLd(page: CommercialFeaturePage) {
  const canonicalUrl = marketingCanonicalUrl(page.pathname);
  return [
    {
      "@context": "https://schema.org",
      "@type": "Organization",
      name: "Walnut Markets",
      legalName: "Walnut Intelligence Inc.",
      url: WALNUT_MARKETING_URL,
      logo: `${WALNUT_MARKETING_URL}/walnut-intel-logo-mark.png`,
      description: WALNUT_MARKETING_DESCRIPTION,
      sameAs: WALNUT_SOCIAL_URLS,
    },
    {
      "@context": "https://schema.org",
      "@type": "BreadcrumbList",
      itemListElement: [
        { "@type": "ListItem", position: 1, name: "Walnut Markets", item: WALNUT_MARKETING_URL },
        { "@type": "ListItem", position: 2, name: page.breadcrumbLabel, item: canonicalUrl },
      ],
    },
    {
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
    },
    {
      "@context": "https://schema.org",
      "@type": "SoftwareApplication",
      name: "Walnut Market Terminal",
      applicationCategory: "FinanceApplication",
      operatingSystem: "Web",
      url: WALNUT_MARKETING_URL,
      image: WALNUT_SOCIAL_IMAGE_URL,
      description: WALNUT_MARKETING_DESCRIPTION,
      publisher: {
        "@type": "Organization",
        name: "Walnut Intelligence Inc.",
      },
    },
    {
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
    },
  ];
}
