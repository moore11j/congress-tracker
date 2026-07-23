export type ResearchBriefCard = {
  slug: string;
  route: string;
  title: string;
  description: string;
  tickers: string[];
  category: string;
  judgment?: "bullish" | "bearish" | "mixed" | "macro" | "policy";
  publishedAt: string;
  readingMinutes: number;
  featured?: boolean;
};

export const researchBriefs: ResearchBriefCard[] = [
  {
    slug: "nbis-vs-crwv-ai-neoclouds",
    route: "/research/nbis-vs-crwv-ai-neoclouds",
    title: "NBIS vs CRWV: Which AI Neocloud Trade Has Better Risk/Reward?",
    description: "Nebius and CoreWeave compared across revenue, backlog, margins, debt, capex, Nvidia alignment, and Walnut market data.",
    tickers: ["NBIS", "CRWV", "NVDA"],
    category: "AI Infrastructure",
    judgment: "mixed",
    publishedAt: "2026-07-23",
    readingMinutes: 9,
    featured: true,
  },
  {
    slug: "ai-earnings-dd",
    route: "/research/ai-earnings-dd",
    title: "AI earnings week DD: numbers over hype",
    description: "Prior-quarter revenue, margins, cash flow, delivery data, and guidance bars for GOOGL, TSLA, SNOW, IBM, and TXN.",
    tickers: ["GOOGL", "TSLA", "SNOW", "IBM", "TXN"],
    category: "AI Infrastructure",
    judgment: "macro",
    publishedAt: "2026-07-22",
    readingMinutes: 8,
    featured: true,
  },
  {
    slug: "mu-dd",
    route: "/research/mu-dd",
    title: "Is the MU momentum trade dead?",
    description: "The bear case needs memory demand to roll over. The latest cycle data says the trade still has a pulse.",
    tickers: ["MU"],
    category: "Semiconductors",
    judgment: "bullish",
    publishedAt: "2026-07-20",
    readingMinutes: 7,
    featured: true,
  },
];

export function getResearchBriefBySlug(slug: string): ResearchBriefCard | undefined {
  return researchBriefs.find((brief) => brief.slug === slug);
}

export function getPublishedResearchBriefs(): ResearchBriefCard[] {
  return [...researchBriefs].sort((left, right) => {
    if (left.featured !== right.featured) return left.featured ? -1 : 1;
    const dateDelta = new Date(right.publishedAt).getTime() - new Date(left.publishedAt).getTime();
    if (dateDelta !== 0) return dateDelta;
    return researchBriefs.indexOf(left) - researchBriefs.indexOf(right);
  });
}
