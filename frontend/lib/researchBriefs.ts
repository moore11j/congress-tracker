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
  premium?: boolean;
  requiredPlan?: "premium" | "pro" | string | null;
  thumbnailUrl?: string;
};

export const researchBriefs: ResearchBriefCard[] = [
  {
    slug: "spcx-earnings-preview",
    route: "/research/spcx-earnings-preview",
    title: "SPCX earnings preview: can growth justify its valuation?",
    description: "SpaceX's first public-company earnings report tests Starlink growth, AI capex, Starship progress, launch economics, guidance, and the upcoming share unlock.",
    tickers: ["SPCX"],
    category: "Space Infrastructure",
    judgment: "mixed",
    publishedAt: "2026-08-03",
    readingMinutes: 7,
    featured: true,
  },
  {
    slug: "nvda-vs-mu",
    route: "/research/nvda-vs-mu",
    title: "NVDA vs MU: which is the better buy right now?",
    description: "NVIDIA has the cleaner AI demand and margin story, while Micron offers cheaper memory-cycle torque if DRAM and HBM pricing keep recovering.",
    tickers: ["NVDA", "MU"],
    category: "AI Semiconductors",
    judgment: "bullish",
    publishedAt: "2026-08-03",
    readingMinutes: 6,
    featured: true,
  },
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
    thumbnailUrl: "/ad-thumbnails/nbis-crwv-neoclouds.jpg",
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
    thumbnailUrl: "/ad-thumbnails/ai-earnings-dd-thumbnail.png",
  },
  {
    slug: "mu-dd",
    route: "/research/mu-dd",
    title: "Is the MU momentum trade dead?",
    description: "A Walnut preview of the MU momentum setup, memory-cycle question, and key reported revenue, margin, and guidance data.",
    tickers: ["MU"],
    category: "Semiconductors",
    publishedAt: "2026-07-20",
    readingMinutes: 7,
    featured: true,
    premium: true,
  },
];

export function getResearchBriefBySlug(slug: string): ResearchBriefCard | undefined {
  return researchBriefs.find((brief) => brief.slug === slug);
}

export function getPublishedResearchBriefs(): ResearchBriefCard[] {
  return [...researchBriefs].sort((left, right) => {
    const dateDelta = new Date(right.publishedAt).getTime() - new Date(left.publishedAt).getTime();
    if (dateDelta !== 0) return dateDelta;
    if (left.featured !== right.featured) return left.featured ? -1 : 1;
    return researchBriefs.indexOf(left) - researchBriefs.indexOf(right);
  });
}
