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
    slug: "mu-dd",
    route: "/research/mu-dd",
    title: "Is the MU momentum trade dead?",
    description: "A research-only Micron DD landing page reviewing the memory-cycle data behind the MU momentum question. Not investment advice.",
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
