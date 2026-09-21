import type { OperationalIntelligenceItem, TickerOperationalIntelligence } from "./api";

export const findingCategories = { catalysts: "Catalyst", risks: "Risk", opportunities: "Opportunity", watch_next: "Watch next" } as const;
export type FindingCategory = keyof typeof findingCategories;
export type ResearchFinding = {
  id: string;
  item: OperationalIntelligenceItem;
  categories: FindingCategory[];
  variants: { category: FindingCategory; item: OperationalIntelligenceItem }[];
};

// Deduplicate shared event identity, never just a similar headline. Retain every
// category's interpretation, including date-only watch labels, in expanded detail.
export function collectResearchFindings(data: TickerOperationalIntelligence | null): ResearchFinding[] {
  const findings = new Map<string, ResearchFinding>();
  if (!data) return [];
  for (const category of Object.keys(findingCategories) as FindingCategory[]) {
    for (const item of data[category]) {
      let finding = findings.get(item.id);
      if (!finding) {
        finding = { id: item.id, item, categories: [], variants: [] };
        findings.set(item.id, finding);
      }
      if (!finding.categories.includes(category)) finding.categories.push(category);
      if (!finding.variants.some((variant) => variant.category === category && JSON.stringify(variant.item) === JSON.stringify(item))) {
        finding.variants.push({ category, item });
      }
    }
  }
  return [...findings.values()];
}

export function selectResearchFindings(findings: ResearchFinding[], category: FindingCategory | "all", sort: "latest" | "material") {
  const time = (value?: string | null) => Date.parse(value ?? "") || 0;
  const rank = (value: string) => ({ high: 3, medium: 2, low: 1 })[value] ?? 0;
  return findings.filter((finding) => category === "all" || finding.categories.includes(category)).sort((a, b) =>
    (sort === "material" ? rank(b.item.materiality) - rank(a.item.materiality) : 0) || time(b.item.published_at) - time(a.item.published_at) || a.id.localeCompare(b.id));
}
