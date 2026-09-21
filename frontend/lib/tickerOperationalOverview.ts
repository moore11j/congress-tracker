import type { OperationalIntelligenceItem, TickerDecisionItem, TickerDecisionLayer, TickerOperationalIntelligence } from "./api";

export type OverviewDecisionItem = TickerDecisionItem & { evidenceId?: string; sourceType?: string; sourceUrl?: string | null };
export const operatingSourceLabels: Record<string, string> = { news_article: "News", press_release: "Press release", earnings_transcript: "Earnings call" };

function unique(items: OperationalIntelligenceItem[]) {
  const ids = new Set<string>();
  const headlines = new Set<string>();
  return items.filter((item) => {
    const key = item.title.trim().toLowerCase();
    if (!key || ids.has(item.id) || headlines.has(key)) return false;
    ids.add(item.id); headlines.add(key); return true;
  }).sort((a, b) => Number(b.materiality === "high") - Number(a.materiality === "high") || (Date.parse(b.published_at ?? "") || 0) - (Date.parse(a.published_at ?? "") || 0));
}

function asDecision(item: OperationalIntelligenceItem): OverviewDecisionItem {
  // Reuse the already validated AI interpretation. Do not generate new beliefs,
  // truncate away qualifications, or make another model call just for display.
  return { category: "company_development", title: operatingSourceLabels[item.source_type] ?? "Company development", description: item.title, date: item.published_at, confidence: item.confidence, evidenceId: item.id, sourceType: item.source_type, sourceUrl: item.source_url };
}

export function mergeOperationalOverview(layer: TickerDecisionLayer, data: TickerOperationalIntelligence | null, now = Date.now()) {
  const merge = (items: OperationalIntelligenceItem[], existing: TickerDecisionItem[] = []): OverviewDecisionItem[] => {
    const operating = unique(items).slice(0, 3).map(asDecision);
    return [...operating, ...existing.filter((item) => !operating.some((entry) => entry.description === item.description))].slice(0, 5);
  };
  // Ignore stale responses for another ticker and keep the existing context on failure.
  if (!data || data.symbol.toUpperCase() !== layer.symbol.toUpperCase()) return layer;
  const developments = unique([...data.catalysts, ...data.risks, ...data.opportunities]);
  const changed = developments.filter((item) => {
    const published = Date.parse(item.published_at ?? "");
    return Number.isFinite(published) && published <= now && published >= now - 30 * 86400000;
  });
  return {
    ...layer,
    catalysts: merge([...data.catalysts, ...data.opportunities], layer.catalysts),
    risks: merge(data.risks, layer.risks),
    what_changed: merge(changed, layer.what_changed),
    // The API's watch title can be just a date (e.g. "calendar 2028").
    // Keep its source-supported company context instead of showing a bare label.
    watch_items: merge(data.watch_next.map((item) => ({ ...item, title: developments.find((development) => development.id === item.id)?.title || item.summary || item.title })), layer.watch_items),
  };
}
