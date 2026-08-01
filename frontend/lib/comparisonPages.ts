import {
  WALNUT_MARKETING_DESCRIPTION,
  WALNUT_MARKETING_URL,
  WALNUT_SOCIAL_IMAGE_URL,
  WALNUT_SOCIAL_URLS,
  marketingCanonicalUrl,
} from "@/lib/marketingMetadata";

export type ComparisonRow = {
  label: string;
  walnut: string;
  competitor: string;
};

export type ComparisonFact = {
  sourceName: string;
  sourceUrl: string;
  checkedOn: string;
  notes: string[];
  requiresReverification?: boolean;
};

export type ComparisonFaq = {
  question: string;
  answer: string;
};

export type RelatedComparisonLink = {
  label: string;
  href: string;
  body: string;
};

export type CompetitorComparisonPage = {
  slug: string;
  competitorName: string;
  eyebrow: string;
  title: string;
  description: string;
  hubDescription: string;
  h1: string;
  intro: string;
  quickVerdict: {
    walnut: string;
    competitor: string;
  };
  rows: ComparisonRow[];
  walnutBestFor: string[];
  competitorBestFor: string[];
  workflowTitle: string;
  workflowBody: string[];
  differentiatorBody: string[];
  planContext: string;
  faq: ComparisonFaq[];
  primaryCta: {
    label: string;
    href: string;
  };
  secondaryCta: {
    label: string;
    href: string;
  };
  relatedLinks: RelatedComparisonLink[];
  facts: ComparisonFact[];
  claimsForOwnerReview: string[];
};

export const comparisonCheckedOn = "2026-08-01";
export const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");
export const comparisonScreenshot = "/landing/compare-nvda-mu-production.png";

const tickerHref = `${appUrl}/ticker/NVDA`;
const pricingHref = "/pricing";
const stockToolsHref = "/stock-analysis-tools";
const compareTickerHref = `${appUrl}/compare/NVDA/MU`;

const commonRelatedLinks: RelatedComparisonLink[] = [
  {
    label: "Stock analysis tools",
    href: stockToolsHref,
    body: "Open the product map for ticker research, disclosures, filings, contracts, and comparison workflows.",
  },
  {
    label: "Pricing",
    href: pricingHref,
    body: "Check which Walnut plan fits the research depth and monitoring limits you need.",
  },
  {
    label: "Ticker example",
    href: tickerHref,
    body: "Start from a live ticker page and inspect the available research context directly.",
  },
];

function fact(sourceName: string, sourceUrl: string, notes: string[], requiresReverification = true): ComparisonFact {
  return { sourceName, sourceUrl, checkedOn: comparisonCheckedOn, notes, requiresReverification };
}

export const comparisonPages: Record<string, CompetitorComparisonPage> = {
  "walnut-markets-vs-stockanalysis": {
    slug: "walnut-markets-vs-stockanalysis",
    competitorName: "StockAnalysis",
    eyebrow: "Stock research software comparison",
    title: "Walnut Markets vs StockAnalysis | Stock Research Comparison",
    description:
      "Compare Walnut Markets and StockAnalysis for investors choosing between broad financial data and a cross-source stock research workflow.",
    hubDescription:
      "For investors weighing a clean fundamentals and market-reference site against Walnut's cross-source research workflow.",
    h1: "Walnut Markets vs StockAnalysis",
    intro:
      "StockAnalysis is a strong place to look up company financials, statements, screeners, and market reference data. Walnut is built for the next step: asking whether price, fundamentals, disclosures, ownership, contracts, and research context point in the same direction.",
    quickVerdict: {
      walnut:
        "Walnut is better suited when the question is not just what the numbers are, but what changed and whether separate sources support the same stock thesis.",
      competitor:
        "StockAnalysis may be the better fit when you mainly want clean financial statements, broad coverage, market reference pages, watchlists, and straightforward screening.",
    },
    rows: [
      { label: "Primary use case", walnut: "Cross-source stock research and interpretation", competitor: "Financial data, market reference, screening, and watchlists" },
      { label: "Fundamentals", walnut: "Included in ticker research", competitor: "Core focus with long financial histories" },
      { label: "Price and volume", walnut: "Part of ticker context and screening", competitor: "Available as market data and quote context" },
      { label: "Congress trading", walnut: "Integrated with ticker research", competitor: "Not prominently positioned in reviewed public pages" },
      { label: "Insider activity", walnut: "Integrated with ticker research", competitor: "Company/officer data appears available; depth should be confirmed" },
      { label: "Institutional activity", walnut: "Pro-level filing layer", competitor: "ETF holdings listed; institutional workflow should be confirmed" },
      { label: "Government contracts", walnut: "Included where mapped to issuers", competitor: "Not prominently positioned in reviewed public pages" },
      { label: "Research briefs", walnut: "Built into the research workflow", competitor: "Market reference and data pages are the public emphasis" },
      { label: "Confirmation score", walnut: "Proprietary interpretive metric", competitor: "Not a visible equivalent in reviewed public pages" },
      { label: "Free access", walnut: "Free starter access available", competitor: "Free plan plus paid Pro and Unlimited plans" },
    ],
    walnutBestFor: [
      "Investors who already have a ticker in mind and want the evidence organized into a research judgment.",
      "Teams that care about alternative data next to fundamentals rather than in a separate tab or spreadsheet.",
      "Users who want the confirmation score kept separate from the underlying source data.",
    ],
    competitorBestFor: [
      "Investors who need clean financial statements, company metrics, broad stock and fund coverage, and exportable reference data.",
      "Users who want a low-friction market data site before they move into deeper thesis work.",
    ],
    workflowTitle: "Reference data versus thesis review",
    workflowBody: [
      "StockAnalysis is useful when the job is to retrieve financial data quickly and screen across a broad market universe.",
      "Walnut starts from the ticker and asks a different question: do fundamentals, price and volume, disclosures, ownership, contracts, and research briefs support the same conclusion?",
    ],
    differentiatorBody: [
      "Walnut's workflow is data -> interpretation -> judgment -> action-ready research.",
      "The confirmation score helps summarize cross-source support, but it does not hide the underlying evidence. Users can still inspect the source categories that shaped the readout.",
    ],
    planContext:
      "Walnut has Free, Premium, and Pro access. StockAnalysis lists a free tier and paid Pro/Unlimited options on its public pricing page; exact terms should be confirmed with StockAnalysis before purchase.",
    faq: [
      {
        question: "Does Walnut replace StockAnalysis?",
        answer:
          "Not for every job. StockAnalysis is strong for financial data lookup and broad reference. Walnut is stronger when you want a multi-source research workflow around a stock thesis.",
      },
      {
        question: "Does Walnut include fundamentals?",
        answer:
          "Yes. Walnut includes fundamental context alongside price and volume, disclosures, filings, contracts, research briefs, and the confirmation score.",
      },
      {
        question: "Is StockAnalysis better for raw financial statements?",
        answer:
          "It may be. StockAnalysis publicly emphasizes deep financial histories and data tables. Walnut focuses more on interpretation across sources.",
      },
      {
        question: "Can I use both?",
        answer:
          "Yes. Many investors may use a market-reference site for lookup and Walnut for deciding whether the broader evidence supports a thesis.",
      },
    ],
    primaryCta: { label: "Research NVDA in Walnut", href: tickerHref },
    secondaryCta: { label: "View pricing", href: pricingHref },
    relatedLinks: commonRelatedLinks,
    facts: [
      fact("StockAnalysis Pro", "https://stockanalysis.com/pro/", [
        "Public page lists free access plus paid Pro and Unlimited plans.",
        "Public page emphasizes global stock/fund data, watchlists, portfolios, alerts, downloads, analyst filtering, indicators, ETF holdings, and corporate actions.",
      ]),
      fact("StockAnalysis subscription FAQ", "https://stockanalysis.com/help/faq/subscription-tiers/", [
        "FAQ says StockAnalysis offers paid plans through Stock Analysis Pro.",
      ]),
    ],
    claimsForOwnerReview: [
      "Confirm Walnut's current fundamentals and export depth relative to StockAnalysis before making any stronger replacement claim.",
    ],
  },
  "walnut-markets-vs-insider-screener": {
    slug: "walnut-markets-vs-insider-screener",
    competitorName: "Insider Screener",
    eyebrow: "Insider trading analysis software comparison",
    title: "Walnut Markets vs Insider Screener | Insider Research Comparison",
    description:
      "Compare Walnut Markets and Insider Screener for insider transaction tracking, ticker context, disclosures, and broader stock research.",
    hubDescription:
      "For investors deciding between a specialist insider transaction screener and a broader ticker research workflow.",
    h1: "Walnut Markets vs Insider Screener",
    intro:
      "Insider Screener is focused on discovering and monitoring insider transactions. Walnut includes insider activity, but places it beside price and volume, fundamentals, Congress activity, institutions, contracts, and research briefs so the filing is not interpreted alone.",
    quickVerdict: {
      walnut:
        "Walnut is a better fit when insider activity is one part of a broader stock research process.",
      competitor:
        "Insider Screener may be better for users who want a dedicated insider database, alerts, screeners, exports, and insider-specific performance views.",
    },
    rows: [
      { label: "Primary use case", walnut: "Ticker research with insider context", competitor: "Insider transaction discovery and monitoring" },
      { label: "Insider activity", walnut: "Included and connected to ticker pages", competitor: "Specialist capability and core focus" },
      { label: "Insider alerts", walnut: "Monitoring and alert workflows by plan", competitor: "Prominently positioned plan feature" },
      { label: "Advanced insider screening", walnut: "Available through broader filters where supported", competitor: "Core product emphasis" },
      { label: "Fundamentals", walnut: "Included in ticker research", competitor: "Not the primary public positioning" },
      { label: "Congress trading", walnut: "Included", competitor: "Not prominently positioned on reviewed pricing page" },
      { label: "Institutional activity", walnut: "Available on Pro", competitor: "Not prominently positioned on reviewed pricing page" },
      { label: "Government contracts", walnut: "Included where mapped", competitor: "Not prominently positioned on reviewed pricing page" },
      { label: "Research briefs", walnut: "Included in research workflow", competitor: "Not a core public pricing emphasis" },
      { label: "Exports/API", walnut: "Exports by plan; API/webhooks roadmap in pricing config", competitor: "Exports and API pricing are prominently listed" },
    ],
    walnutBestFor: [
      "Investors who want to understand what an insider filing means inside the full ticker story.",
      "Users who want Congress, insider, institutional, contract, price, and fundamental context in one workflow.",
      "Researchers who want concise judgment about what changed and what to watch next.",
    ],
    competitorBestFor: [
      "Users who primarily need insider transaction screening, watchlists, custom alerts, and export limits.",
      "Analysts who want a specialist insider workflow before pulling the data into other tools.",
    ],
    workflowTitle: "Specialist filing discovery versus full ticker context",
    workflowBody: [
      "Insider Screener starts from the insider transaction universe and helps users screen, monitor, and export that activity.",
      "Walnut starts from the ticker question and treats insider activity as one evidence category. The filing matters more when it is compared with price behavior, fundamentals, other disclosures, ownership changes, and current risks.",
    ],
    differentiatorBody: [
      "Walnut's process is data -> interpretation -> judgment -> action-ready research.",
      "The confirmation score helps indicate whether different sources appear to support or contradict the same thesis. It is separate from the insider data itself.",
    ],
    planContext:
      "Walnut makes insider activity available in its stock research workflow. Insider Screener publicly lists Plus, Pro, Premium, and API plans with higher limits by tier; prices and limits can change.",
    faq: [
      {
        question: "Does Walnut track insider trading?",
        answer:
          "Yes. Walnut tracks reported insider activity from public filings and connects it to ticker-level research context.",
      },
      {
        question: "Is Walnut a deeper insider screener than Insider Screener?",
        answer:
          "Do not assume that. Insider Screener is a specialist product. Walnut's strength is putting insider activity into a broader stock research workflow.",
      },
      {
        question: "Does Walnut imply insider transactions are predictive?",
        answer:
          "No. Insider transactions can be routine, planned, tax-related, compensation-related, or otherwise limited. Walnut treats them as research context.",
      },
      {
        question: "Can I use Walnut alongside Insider Screener?",
        answer:
          "Yes. A specialist screener can help with insider discovery, while Walnut can help interpret the ticker context around selected names.",
      },
    ],
    primaryCta: { label: "Review insider context in Walnut", href: `${appUrl}/feed?mode=insider` },
    secondaryCta: { label: "Open the insider tracker", href: "/insider-trading-tracker" },
    relatedLinks: [
      { label: "Insider trading tracker", href: "/insider-trading-tracker", body: "See how Walnut presents reported insider activity from public filings." },
      ...commonRelatedLinks,
    ],
    facts: [
      fact("Insider Screener pricing", "https://www.insiderscreener.com/en/pricing", [
        "Public pricing page emphasizes insider buying/selling, alerts, screeners, exports, watchlists, advanced filters, and insider performance tiers.",
        "Pricing page lists Plus, Pro, Premium tiers and separate API pricing.",
      ]),
      fact("Insider Screener API pricing", "https://www.insiderscreener.com/en/api-pricing", [
        "API page lists transaction endpoints, issuer and market feeds, historical data, screener endpoints, ranked research, and usage limits.",
      ]),
    ],
    claimsForOwnerReview: [
      "Confirm current Walnut insider export/API posture before making stronger export or API comparisons.",
    ],
  },
  "walnut-markets-vs-quiver-quant": {
    slug: "walnut-markets-vs-quiver-quant",
    competitorName: "Quiver Quantitative",
    eyebrow: "Alternative data investing platform comparison",
    title: "Walnut Markets vs Quiver Quantitative | Alternative Data Comparison",
    description:
      "Compare Walnut Markets and Quiver Quantitative for alternative data, Congress trading, insider activity, institutions, and ticker-level research.",
    hubDescription:
      "For investors comparing alternative-data breadth with Walnut's ticker-level interpretation and confirmation workflow.",
    h1: "Walnut Markets vs Quiver Quantitative",
    intro:
      "Quiver Quantitative is known for making alternative datasets accessible, including Congress, insiders, government contracts, lobbying, trends, patents, and institutional data. Walnut also uses alternative data, but the product is organized around the ticker decision: what changed, what confirms the thesis, what weakens it, and what to watch next.",
    quickVerdict: {
      walnut:
        "Walnut is better suited when you want alternative data folded into a clear stock research judgment.",
      competitor:
        "Quiver Quantitative may be better when you want a broad alternative-data dashboard, dataset exploration, strategies, backtesters, alerts, and API-oriented workflows.",
    },
    rows: [
      { label: "Primary use case", walnut: "Ticker research with cross-source interpretation", competitor: "Alternative data discovery and dataset tools" },
      { label: "Congress trading", walnut: "Integrated with ticker context", competitor: "Core visible dataset" },
      { label: "Insider activity", walnut: "Integrated with ticker context", competitor: "Core visible dataset" },
      { label: "Institutional holdings", walnut: "Pro-level filing layer", competitor: "Visible dataset and premium backtester" },
      { label: "Government contracts", walnut: "Included where mapped", competitor: "Core visible dataset" },
      { label: "Strategies/backtesting", walnut: "Available in Walnut app workflows", competitor: "Prominently positioned in Premium" },
      { label: "Research briefs", walnut: "Action-ready research output", competitor: "News, articles, videos, and dataset views" },
      { label: "API", walnut: "API/webhooks listed as Pro workflow automation readiness", competitor: "API prominently positioned" },
      { label: "Confirmation score", walnut: "Proprietary interpretive metric", competitor: "Smart score and bull/bear analysis listed publicly" },
    ],
    walnutBestFor: [
      "Investors who want to know whether several sources support the same stock thesis.",
      "Users who prefer a ticker-level research brief over a dataset-first dashboard.",
      "Teams that want risks, catalysts, and what changed called out in the same workflow.",
    ],
    competitorBestFor: [
      "Researchers who want broad alternative datasets, API access, dataset-specific tools, and backtested strategy products.",
      "Users who want to browse political, consumer, SEC filing, and government datasets directly.",
    ],
    workflowTitle: "Dataset breadth versus ticker judgment",
    workflowBody: [
      "Quiver Quantitative is useful when the workflow starts by exploring alternative datasets and deciding which ones deserve attention.",
      "Walnut is built for the moment after a ticker enters the research queue. It connects alternative data to fundamentals, price behavior, risks, catalysts, and the confirmation score.",
    ],
    differentiatorBody: [
      "Walnut's process is data -> interpretation -> judgment -> action-ready research.",
      "The confirmation score is an interpretive layer. Congress trades, insider activity, institutions, contracts, and price/fundamental data remain separate evidence categories.",
    ],
    planContext:
      "Walnut uses Free, Premium, and Pro access. Quiver publicly lists a free Visitor plan and Premium subscription with alternative datasets, strategies, alerts, screeners, backtesters, and exportable samples.",
    faq: [
      {
        question: "Which platform is better for Congress trading research?",
        answer:
          "Both publicly emphasize Congress trading. Quiver is stronger as a dedicated alternative-data browsing surface; Walnut is stronger when Congress activity needs ticker context.",
      },
      {
        question: "Does Walnut have broader alternative datasets than Quiver?",
        answer:
          "That should not be claimed without a fresh dataset-by-dataset audit. Walnut's claim here is workflow and interpretation, not broader coverage.",
      },
      {
        question: "Does Walnut include government contracts?",
        answer:
          "Yes. Walnut includes government contract activity where it can be mapped into market context.",
      },
      {
        question: "Can I use Quiver Quantitative and Walnut together?",
        answer:
          "Yes. Quiver can be useful for dataset discovery, while Walnut can help evaluate whether a selected ticker has cross-source support.",
      },
    ],
    primaryCta: { label: "Run a ticker through Walnut", href: tickerHref },
    secondaryCta: { label: "Explore confirmation score", href: "/stock-confirmation-score" },
    relatedLinks: [
      { label: "Congress trades", href: "/congress-trades", body: "Review Walnut's public explanation of reported congressional trading data." },
      { label: "Government contracts", href: "/government-contracts", body: "See how contract awards fit into Walnut's issuer research." },
      ...commonRelatedLinks,
    ],
    facts: [
      fact("Quiver Quantitative homepage/pricing", "https://www.quiverquant.com/corehomepage/", [
        "Public page lists datasets including Congress trading, insider trading, government contracts, corporate lobbying, Google Search Trends, institutional trading, patents, and more.",
        "Public page lists a free Visitor plan and Premium subscription with strategies, alerts, stock screener, Congress and institutional backtesters, stock smart score, watchlist, and exportable samples.",
      ]),
      fact("Quiver Quantitative FAQ", "https://www.quiverquant.com/faq/", [
        "FAQ positions Quiver as an investment research platform for alternative data and says the web platform is free to use.",
      ]),
    ],
    claimsForOwnerReview: [
      "Do not claim Walnut has broader alternative-data coverage than Quiver without a source-by-source audit.",
    ],
  },
  "walnut-markets-vs-unusual-whales": {
    slug: "walnut-markets-vs-unusual-whales",
    competitorName: "Unusual Whales",
    eyebrow: "Options flow alternative comparison",
    title: "Walnut Markets vs Unusual Whales | Market Data Platform Comparison",
    description:
      "Compare Walnut Markets and Unusual Whales for options flow, market activity, alternative data, and investor-oriented stock research.",
    hubDescription:
      "For investors deciding between options-flow monitoring and a broader stock research interpretation layer.",
    h1: "Walnut Markets vs Unusual Whales",
    intro:
      "Unusual Whales is closely associated with options flow, dark pool data, alerts, API access, and active trader tools. Walnut should be evaluated differently: it is a stock research platform that uses available options data as one layer beside fundamentals, price, ownership, Congress, insiders, contracts, risks, and catalysts.",
    quickVerdict: {
      walnut:
        "Walnut is better suited when the goal is to research a stock across multiple evidence categories and reach a sober view of the thesis.",
      competitor:
        "Unusual Whales may be better for traders who primarily need deep options-flow, dark-pool, alert, and API workflows.",
    },
    rows: [
      { label: "Primary use case", walnut: "Investor-oriented stock research", competitor: "Options flow, market activity, data APIs, and trader tools" },
      { label: "Options flow", walnut: "Available where supported; not Walnut's primary use case", competitor: "Core public positioning" },
      { label: "Dark pool data", walnut: "Not Walnut's primary focus", competitor: "Prominently positioned publicly" },
      { label: "Congress trading", walnut: "Included in ticker research", competitor: "Listed in public API/tooling materials" },
      { label: "Insider activity", walnut: "Included in ticker research", competitor: "Listed in public API/tooling materials" },
      { label: "Institutions", walnut: "Pro-level filing layer", competitor: "Listed in public API/tooling materials" },
      { label: "Fundamentals", walnut: "Included in ticker research", competitor: "Available in stock data context; depth should be confirmed" },
      { label: "Research briefs", walnut: "Core research output", competitor: "Not the main public positioning reviewed" },
      { label: "Confirmation score", walnut: "Proprietary interpretive metric", competitor: "No equivalent verified in reviewed public pages" },
    ],
    walnutBestFor: [
      "Investors who want options data interpreted next to the rest of the stock case rather than treated as the whole case.",
      "Researchers who care about fundamentals, disclosures, ownership, contracts, catalysts, and risks in one ticker view.",
      "Users who want concise research briefs rather than a flow-monitoring dashboard as the primary surface.",
    ],
    competitorBestFor: [
      "Active traders who spend most of their time in options flow, dark-pool prints, alerts, and short-horizon market activity.",
      "API users who need dedicated market-data endpoints for options, flow, dark pool, Congress, insiders, institutions, and related categories.",
    ],
    workflowTitle: "Flow monitoring versus stock research context",
    workflowBody: [
      "Unusual Whales is a better-known fit for traders who want to watch market activity and options flow closely.",
      "Walnut is designed for investors asking whether multiple sources confirm a ticker thesis and what could change that view.",
    ],
    differentiatorBody: [
      "Walnut's process is data -> interpretation -> judgment -> action-ready research.",
      "Options activity can be useful, but Walnut keeps it in context. The confirmation score remains distinct from the data categories that inform it.",
    ],
    planContext:
      "Walnut Pro is the right plan context for institutional activity and planned options-flow depth. Unusual Whales publishes pricing and API access publicly; current pricing should be confirmed on its site because flow-data plans change often.",
    faq: [
      {
        question: "Is Walnut a full Unusual Whales replacement?",
        answer:
          "Not for advanced options-flow monitoring. Walnut is a broader stock research platform, not a one-for-one replacement for a specialist options-flow workflow.",
      },
      {
        question: "Does Walnut include options flow?",
        answer:
          "Options flow is treated as an available data layer where supported and is not Walnut's primary public use case.",
      },
      {
        question: "Which platform is better for options traders?",
        answer:
          "Users whose main workflow is options flow, dark-pool activity, and trading alerts may prefer Unusual Whales.",
      },
      {
        question: "Where does Walnut fit?",
        answer:
          "Walnut fits when you want to see whether options activity lines up with fundamentals, price, ownership, disclosures, contracts, and research context.",
      },
    ],
    primaryCta: { label: "See what multiple sources say", href: tickerHref },
    secondaryCta: { label: "Compare Walnut plans", href: pricingHref },
    relatedLinks: [
      { label: "Institutional filings", href: "/institutional-filings", body: "Review Walnut's plan-gated institutional activity context." },
      ...commonRelatedLinks,
    ],
    facts: [
      fact("Unusual Whales pricing", "https://unusualwhales.com/pricing?product=api", [
        "Public pricing page metadata positions Unusual Whales around options flow, dark pool data, market analysis tools, and API access.",
      ]),
      fact("Unusual Whales public API", "https://unusualwhales.com/public-api", [
        "Public API page metadata references options flow, dark pool, stock data, API documentation, and API tokens.",
      ]),
      fact("Unusual Whales official MCP repository", "https://github.com/unusual-whales/unusual-whales-official-mcp", [
        "Official repository describes tool categories including stock, options, flow, dark pool, Congress, insider, institutions, market, earnings, ETF, and shorts.",
      ]),
    ],
    claimsForOwnerReview: [
      "Confirm Walnut's live options-flow availability and plan gating before making stronger options comparisons.",
    ],
  },
  "walnut-markets-vs-finviz": {
    slug: "walnut-markets-vs-finviz",
    competitorName: "Finviz",
    eyebrow: "Stock screener comparison",
    title: "Walnut Markets vs Finviz | Stock Screener and Research Comparison",
    description:
      "Compare Walnut Markets and Finviz for market scanning, stock screening, charts, fundamentals, and deeper research interpretation.",
    hubDescription:
      "For investors who discover ideas in a fast market scanner and need a deeper research workflow afterward.",
    h1: "Walnut Markets vs Finviz",
    intro:
      "Finviz is a fast way to scan the market, read maps, review charts, and find stocks. Walnut is better framed as the investigation layer after discovery: open the ticker, inspect the evidence, and decide whether the data supports the thesis.",
    quickVerdict: {
      walnut:
        "Walnut is better suited after a stock is on your shortlist and the question becomes whether the thesis holds up.",
      competitor:
        "Finviz may be better for fast screening, market maps, chart scanning, broad market overview, and trader-style discovery.",
    },
    rows: [
      { label: "Primary use case", walnut: "Ticker investigation and research judgment", competitor: "Market scanning, screening, maps, charts, and overview" },
      { label: "Stock screening", walnut: "Available with research-context filters", competitor: "Core focus" },
      { label: "Market maps", walnut: "Market pressure maps by plan", competitor: "Core visible workflow" },
      { label: "Charting", walnut: "Ticker chart context", competitor: "Broader charting and Elite real-time/advanced chart features" },
      { label: "Fundamentals", walnut: "Included in ticker research", competitor: "Available and recalculated regularly per FAQ" },
      { label: "Congress trading", walnut: "Integrated with ticker research", competitor: "Insider/nav data visible; Congress workflow not prominent in reviewed FAQ" },
      { label: "Insider activity", walnut: "Integrated with ticker research", competitor: "Insider section and Elite alerts listed" },
      { label: "Research briefs", walnut: "Built for thesis review", competitor: "Not the main public positioning" },
      { label: "Confirmation score", walnut: "Proprietary interpretive metric", competitor: "No equivalent verified in reviewed public pages" },
    ],
    walnutBestFor: [
      "Investors who already discovered a name and need a structured read on fundamentals, disclosures, ownership, contracts, catalysts, and risks.",
      "Users who want to compare source categories instead of stopping at a screen result.",
      "Researchers who want a concise view of what changed and what to watch next.",
    ],
    competitorBestFor: [
      "Users who want fast discovery, market maps, broad screens, chart snapshots, watchlists, and real-time Elite market tools.",
      "Traders who care more about scanning breadth and speed than an interpreted research brief.",
    ],
    workflowTitle: "Discovery versus investigation",
    workflowBody: [
      "Finviz helps investors scan the market and find possible names quickly.",
      "Walnut helps investors investigate whether the underlying data supports a thesis once the stock is worth a closer look.",
    ],
    differentiatorBody: [
      "Walnut's process is data -> interpretation -> judgment -> action-ready research.",
      "The confirmation score is not a raw screen filter alone. It is an interpretive metric shown alongside the source evidence.",
    ],
    planContext:
      "Walnut's Free, Premium, and Pro plans determine research depth and limits. Finviz publicly lists a free Elite trial and paid monthly/annual Elite plans with the same full suite of Elite features.",
    faq: [
      {
        question: "Does Walnut replace Finviz?",
        answer:
          "Not completely. Finviz is strong for screening, maps, and broad market scanning. Walnut is stronger for deeper ticker research and cross-source interpretation.",
      },
      {
        question: "Which platform is better for finding stocks?",
        answer:
          "Finviz is likely better for fast market discovery. Walnut is built for evaluating a stock after it reaches your research list.",
      },
      {
        question: "Does Walnut have a stock screener?",
        answer:
          "Yes. Walnut includes stock screening with research-context filters and plan-based limits.",
      },
      {
        question: "Can I use Finviz and Walnut together?",
        answer:
          "Yes. A practical workflow is to discover candidates in Finviz and then run selected names through Walnut.",
      },
    ],
    primaryCta: { label: "Investigate a ticker in Walnut", href: tickerHref },
    secondaryCta: { label: "Open Walnut stock tools", href: stockToolsHref },
    relatedLinks: [
      { label: "Stock screener", href: `${appUrl}/screener`, body: "Open Walnut's screener for research-context filters and saved workflows." },
      ...commonRelatedLinks,
    ],
    facts: [
      fact("Finviz FAQ", "https://finviz.com/help/faq", [
        "FAQ lists Free 7-day Elite trial, monthly and annual Elite plans.",
        "FAQ says Elite includes real-time quotes and charts, ad-free interface, advanced screener, ETF and fundamental data, export/API access, push alerts, and more.",
        "FAQ says regular quotes are delayed 1 minute for NASDAQ/NYSE/AMEX and Elite has real-time stock quotes.",
      ]),
      fact("Finviz Elite", "https://elite.finviz.com/elite", [
        "Elite page emphasizes real-time/extended hours, advanced charts, backtests, correlations, advanced screener, alerts, and ad-free layout.",
      ]),
    ],
    claimsForOwnerReview: [
      "Do not claim Walnut replaces Finviz's screening, map, charting, or real-time Elite workflow.",
    ],
  },
  "walnut-markets-vs-capitol-trades": {
    slug: "walnut-markets-vs-capitol-trades",
    competitorName: "Capitol Trades",
    eyebrow: "Congress stock trading tracker comparison",
    title: "Walnut Markets vs Capitol Trades | Congress Trading Research Comparison",
    description:
      "Compare Walnut Markets and Capitol Trades for congressional trading disclosures, ticker context, and broader stock research.",
    hubDescription:
      "For investors deciding between a focused congressional-trades database and integrated ticker research.",
    h1: "Walnut Markets vs Capitol Trades",
    intro:
      "Capitol Trades is built around public-official trading disclosures. Walnut includes Congress activity too, but the product goal is broader: connect the disclosure to ticker-level price, fundamentals, insider activity, institutions, contracts, and research judgment.",
    quickVerdict: {
      walnut:
        "Walnut is better when a Congress disclosure needs to be evaluated inside the full ticker context.",
      competitor:
        "Capitol Trades may be better for users who want a focused database of politician trades, issuers, politicians, and disclosure filters.",
    },
    rows: [
      { label: "Primary use case", walnut: "Congress activity inside stock research", competitor: "Politician trade tracking and disclosure browsing" },
      { label: "Congress trading", walnut: "Integrated with ticker pages and feed", competitor: "Core focus" },
      { label: "Politician profiles", walnut: "Member pages and disclosure context", competitor: "Core navigation includes politicians" },
      { label: "Issuer/ticker context", walnut: "Ticker research plus source modules", competitor: "Issuer filters and trade tables visible" },
      { label: "Price and volume", walnut: "Integrated ticker context", competitor: "Trade table price context visible in indexed pages" },
      { label: "Fundamentals", walnut: "Included in ticker research", competitor: "Not the primary public positioning reviewed" },
      { label: "Insider activity", walnut: "Included", competitor: "Not the core public positioning reviewed" },
      { label: "Institutional activity", walnut: "Pro-level filing layer", competitor: "Not the core public positioning reviewed" },
      { label: "Research briefs", walnut: "Included in research workflow", competitor: "Insights/buzz visible; depth should be confirmed" },
    ],
    walnutBestFor: [
      "Investors who want Congress disclosures interpreted with price action, fundamentals, insiders, institutions, and contract exposure.",
      "Users who want to move from a disclosed trade to a ticker research page quickly.",
      "Researchers who need disclosure timing handled carefully instead of treating every filing as fresh trading intent.",
    ],
    competitorBestFor: [
      "Users who want a dedicated congressional trading database and filtered trade table.",
      "Readers who primarily follow politicians, issuers, and public-official trade disclosures.",
    ],
    workflowTitle: "Disclosure tracking versus integrated ticker analysis",
    workflowBody: [
      "Capitol Trades is useful when the job is to browse and filter politician trading disclosures.",
      "Walnut is useful when the job is to ask whether that disclosure matters for the public company and whether other data agrees.",
    ],
    differentiatorBody: [
      "Walnut's process is data -> interpretation -> judgment -> action-ready research.",
      "Congress activity is one input. The confirmation score is a separate interpretive layer and should not be confused with the Congress dataset itself.",
    ],
    planContext:
      "Walnut includes Congress activity in its broader product. Capitol Trades public pages were partially available through indexed preview pages; current pricing or paywall details should be confirmed directly with the provider.",
    faq: [
      {
        question: "Does Walnut include Congress trading?",
        answer:
          "Yes. Walnut includes reported Congress activity and connects it to ticker research where possible.",
      },
      {
        question: "Are Congress disclosures real time?",
        answer:
          "No. Congressional disclosures are governed by filing rules and can arrive after the trade date. That timing limitation is not caused by any one tracker.",
      },
      {
        question: "Is Capitol Trades better for browsing politician trades?",
        answer:
          "It may be. Capitol Trades is focused on politician trade tracking. Walnut is focused on ticker-level interpretation.",
      },
      {
        question: "Why use Walnut for Congress trades?",
        answer:
          "Use Walnut when you want to see whether a disclosed trade lines up with fundamentals, price behavior, insider activity, institutions, contracts, and risks.",
      },
    ],
    primaryCta: { label: "Go beyond the disclosure", href: `${appUrl}/feed?mode=congress` },
    secondaryCta: { label: "Read about Congress trades", href: "/congress-trades" },
    relatedLinks: [
      { label: "Congress trades tracker", href: "/congress-trades", body: "Read Walnut's public explanation of reported congressional disclosures and timing." },
      ...commonRelatedLinks,
    ],
    facts: [
      fact("Capitol Trades indexed preview", "https://preview.capitoltrades.com/trades?filingId=204852497&page=1", [
        "Indexed page shows Capitol Trades navigation for Trades, Politicians, Issuers, Insights, Buzz, and Press.",
        "Indexed trade table shows politician, traded issuer, published date, traded date, filed-after timing, owner, type, size, and price.",
        "Indexed page notes historical website data is restricted to the past 3 years.",
      ]),
      fact("Capitol Trades public homepage", "https://www.capitoltrades.com/", [
        "Live public site was not fetchable in this environment; claims should remain cautious and reviewed by owner.",
      ]),
    ],
    claimsForOwnerReview: [
      "Confirm current Capitol Trades pricing, access limits, and public feature set directly because the live site was not fully fetchable.",
    ],
  },
  "walnut-markets-vs-trendspider": {
    slug: "walnut-markets-vs-trendspider",
    competitorName: "TrendSpider",
    eyebrow: "Technical analysis software comparison",
    title: "Walnut Markets vs TrendSpider | Stock Analysis Platform Comparison",
    description:
      "Compare Walnut Markets and TrendSpider for technical analysis, charting automation, alternative data, and cross-source stock research.",
    hubDescription:
      "For investors comparing advanced technical-analysis automation with Walnut's evidence-confirmation research workflow.",
    h1: "Walnut Markets vs TrendSpider",
    intro:
      "TrendSpider is a technical-analysis, charting, scanning, backtesting, alerting, and automation platform. Walnut is not trying to be that same charting workstation. Walnut is built for stock research: fundamentals, disclosures, ownership, contracts, confirmation, and concise judgment around a ticker.",
    quickVerdict: {
      walnut:
        "Walnut is better suited when fundamentals, alternative data, and research judgment matter more than chart automation.",
      competitor:
        "TrendSpider may be better for traders who need advanced charting, automated technical analysis, strategy testing, alerts, bots, and scanning.",
    },
    rows: [
      { label: "Primary use case", walnut: "Stock research and evidence confirmation", competitor: "Technical analysis, scanning, charting, alerts, bots, and backtesting" },
      { label: "Advanced charting", walnut: "Ticker chart context", competitor: "Core focus" },
      { label: "Technical analysis automation", walnut: "Not Walnut's primary use case", competitor: "Core public positioning" },
      { label: "Backtesting", walnut: "Available in app workflows", competitor: "Core strategy-development feature" },
      { label: "Stock scanning", walnut: "Research-context screener", competitor: "Advanced scanning and idea generation" },
      { label: "Fundamentals", walnut: "Included in ticker research", competitor: "Included in charting/analysis platform" },
      { label: "Congress trading", walnut: "Integrated with ticker research", competitor: "Government trading listed in idea generation" },
      { label: "Insider activity", walnut: "Integrated with ticker research", competitor: "Recent insider trading listed in product navigation" },
      { label: "Research briefs", walnut: "Core output", competitor: "Not the primary public positioning" },
      { label: "Confirmation score", walnut: "Proprietary interpretive metric", competitor: "No equivalent verified in reviewed public pages" },
    ],
    walnutBestFor: [
      "Investors who want to connect fundamentals, Congress, insiders, institutional activity, contracts, risks, catalysts, and research briefs.",
      "Users who want less chart automation and more explanation of whether the broader evidence supports the ticker.",
      "Teams that need a research record, not only a technical trading setup.",
    ],
    competitorBestFor: [
      "Traders who need advanced charts, scanners, strategy testing, automated alerts, bots, custom indicators, and AI chart assistance.",
      "Users whose daily workflow is technical setup discovery and execution timing.",
    ],
    workflowTitle: "Charting workstation versus research judgment",
    workflowBody: [
      "TrendSpider is built for charting, scanning, testing, alerts, and automated trading workflows.",
      "Walnut is built for evaluating the investment case across data sources and turning that into a concise research view.",
    ],
    differentiatorBody: [
      "Walnut's process is data -> interpretation -> judgment -> action-ready research.",
      "The confirmation score summarizes cross-source support without replacing fundamentals, price context, disclosures, institutions, or contracts.",
    ],
    planContext:
      "Walnut uses Free, Premium, and Pro research plans. TrendSpider publicly lists several plans plus add-ons, trials, market-data fees, and AI assistant tiers; current pricing should be checked on TrendSpider before buying.",
    faq: [
      {
        question: "Is Walnut a TrendSpider replacement?",
        answer:
          "No, not for advanced charting and automation. Walnut is a research platform, while TrendSpider is a technical analysis and trading workflow platform.",
      },
      {
        question: "Which platform is better for technical analysis?",
        answer:
          "TrendSpider is likely better for advanced technical analysis, scanning, backtesting, chart automation, and alerts.",
      },
      {
        question: "Where is Walnut stronger?",
        answer:
          "Walnut is stronger when you need fundamentals, public disclosures, ownership, contracts, research briefs, and cross-source interpretation in one ticker workflow.",
      },
      {
        question: "Can I use both?",
        answer:
          "Yes. A trader may use TrendSpider for setup and timing, then use Walnut to check whether the broader research context supports the idea.",
      },
    ],
    primaryCta: { label: "Check the full ticker context", href: tickerHref },
    secondaryCta: { label: "View confirmation score", href: "/stock-confirmation-score" },
    relatedLinks: [
      { label: "Confirmation score", href: "/stock-confirmation-score", body: "See how Walnut keeps its interpretive score separate from the source data." },
      ...commonRelatedLinks,
    ],
    facts: [
      fact("TrendSpider pricing", "https://trendspider.com/pricing/", [
        "Pricing page describes product areas including chart and market data analysis, idea generation, strategy testing, ML Quant Lab, alerts and bots, AI analyst, and custom indicators.",
        "Pricing page says all plans include access to all features, with plan differences in usage limits.",
        "Pricing page lists add-ons and market-data fees for certain data types and user categories.",
      ]),
      fact("TrendSpider product", "https://trendspider.com/product/", [
        "Product page positions TrendSpider around automated technical analysis, charting, pattern recognition, scanning, backtesting, and strategy workflows.",
      ]),
    ],
    claimsForOwnerReview: [
      "Confirm Walnut's current technical-analysis and backtesting positioning before comparing against TrendSpider's specialist charting depth.",
    ],
  },
};

export const comparisonPageList = Object.values(comparisonPages);

export function comparisonPath(slug: string) {
  return `/compare/${slug}`;
}

export function comparisonPageForSlug(slug: string) {
  return comparisonPages[slug] ?? null;
}

export function comparisonHubJsonLd() {
  const canonicalUrl = marketingCanonicalUrl("/compare");
  return [
    organizationJsonLd(),
    {
      "@context": "https://schema.org",
      "@type": "WebPage",
      name: "Walnut Markets Comparisons",
      url: canonicalUrl,
      description: "Compare Walnut Markets with stock research, screening, alternative data, Congress trading, options flow, and technical analysis tools.",
      isPartOf: { "@type": "WebSite", name: "Walnut Markets", url: WALNUT_MARKETING_URL },
    },
    breadcrumbJsonLd([{ name: "Compare", url: canonicalUrl }]),
  ];
}

export function comparisonPageJsonLd(page: CompetitorComparisonPage) {
  const canonicalUrl = marketingCanonicalUrl(comparisonPath(page.slug));
  return [
    organizationJsonLd(),
    {
      "@context": "https://schema.org",
      "@type": "WebPage",
      name: page.title,
      url: canonicalUrl,
      description: page.description,
      isPartOf: { "@type": "WebSite", name: "Walnut Markets", url: WALNUT_MARKETING_URL },
      about: { "@type": "SoftwareApplication", name: "Walnut Market Terminal" },
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
      publisher: { "@type": "Organization", name: "Walnut Intelligence Inc." },
    },
    breadcrumbJsonLd([
      { name: "Compare", url: marketingCanonicalUrl("/compare") },
      { name: page.competitorName, url: canonicalUrl },
    ]),
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

function organizationJsonLd() {
  return {
    "@context": "https://schema.org",
    "@type": "Organization",
    name: "Walnut Markets",
    legalName: "Walnut Intelligence Inc.",
    url: WALNUT_MARKETING_URL,
    logo: `${WALNUT_MARKETING_URL}/walnut-intel-logo-mark.png`,
    description: WALNUT_MARKETING_DESCRIPTION,
    sameAs: WALNUT_SOCIAL_URLS,
  };
}

function breadcrumbJsonLd(items: Array<{ name: string; url: string }>) {
  return {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: [
      {
        "@type": "ListItem",
        position: 1,
        name: "Walnut Markets",
        item: WALNUT_MARKETING_URL,
      },
      ...items.map((item, index) => ({
        "@type": "ListItem",
        position: index + 2,
        name: item.name,
        item: item.url,
      })),
    ],
  };
}
