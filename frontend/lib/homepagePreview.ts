/** Public homepage projection: never pass raw ranking/context responses to JSX. */
type Data = Record<string, any>;
const record = (value: unknown): Data => value && typeof value === "object" && !Array.isArray(value) ? value as Data : {};
const list = (value: unknown): unknown[] => Array.isArray(value) ? value : [];
const text = (value: unknown, max = 300): string => typeof value === "string" ? value.trim().slice(0, max) : "";
const categories = ["Analysts", "Government contracts", "Government Contracts", "Institutions", "Institutional Activity", "Options flow", "Options Flow", "Congress", "Insiders", "Signals", "Price / Volume", "Fundamentals", "Macro Positioning", "Confirmation Score", "Insider clusters", "Congress + insider clusters", "Strategy entries"];

export function publicDate(value: unknown): string | null {
  return typeof value === "string" && Number.isFinite(Date.parse(value)) ? value : null;
}

export type HomepageStock = {rank: number; symbol: string; companyName: string; drivers: string[]; whyRanked?: string; updatedAt: string | null};
export type HomepageRanking = {items: HomepageStock[]; generatedAt: string | null};
export type HomepageEvidence = {category: string; title: string; description: string; dataAsOf: string | null; source: string; anchor: string; details: string[]};
export type HomepageResearch = {stock: HomepageStock; generatedAt: string; supporting: HomepageEvidence[]; risks: HomepageEvidence[]; watch: string[]};

export function publicHomepageRanking(response: unknown, authenticated = false): HomepageRanking {
  const raw = record(record(response).top_stocks);
  const seen = new Set<string>();
  const items = list(raw.items).filter(value => { const rank = record(value).rank; return Number.isInteger(rank) && rank >= (authenticated ? 1 : 3) && rank <= 5; }).slice(0, authenticated ? 5 : 3).flatMap(value => {
    const item = record(value), symbol = text(item.symbol, 16);
    if (!/^[A-Z0-9][A-Z0-9.-]{0,14}$/.test(symbol) || seen.has(symbol)) return [];
    seen.add(symbol);
    return [{rank: item.rank, symbol, companyName: text(item.company_name, 160) || symbol,
      drivers: list(item.key_drivers).filter((v): v is string => typeof v === "string" && categories.includes(v)).slice(0, 4),
      whyRanked: text(item.why_ranked, 140), updatedAt: publicDate(item.updated_at)}];
  });
  return {items, generatedAt: publicDate(raw.generated_at)};
}

const publicSources: Record<string, {source: string; anchor: string}> = {
  fundamentals: {source: "Walnut fundamentals · reported financial statements", anchor: ""},
  price_volume: {source: "Walnut price / volume · market price history", anchor: ""},
  insiders: {source: "Walnut insider activity · SEC Form 4 disclosures", anchor: "#insider-activity"},
  congress: {source: "Walnut Congress activity · public trade disclosures", anchor: "#congress-activity"},
  government_contracts: {source: "Walnut contracts · government award records", anchor: "#government-contracts-activity"},
};

export function publicHomepageResearch(stock: HomepageStock, response: unknown): HomepageResearch | null {
  const raw = record(response), decision = record(raw.decision_layer);
  const generatedAt = publicDate(raw.generated_at);
  if (raw.symbol !== stock.symbol || decision.symbol !== stock.symbol || !generatedAt) return null;
  const permissions = record(raw.source_entitlements), cards = record(raw.source_cards);
  const permitted = (category: string) => {
    const permission = record(permissions[category]);
    return Boolean(publicSources[category] && permission.available === true && permission.locked === false && permission.required_plan == null);
  };
  const project = (values: unknown): HomepageEvidence[] => list(values).flatMap(value => {
    const item = record(value), category = text(item.category, 50);
    if (!permitted(category) || !text(item.title) || !text(item.description)) return [];
    const card = record(cards[category]);
    if (["unavailable", "error", "disabled", "premium_locked", "pro_locked"].includes(card.status)) return [];
    const details: string[] = [];
    if (category === "fundamentals") {
      for (const [key, label] of [["revenue_growth", "Revenue growth"], ["return_on_equity", "Return on equity"]]) {
        const metric = record(record(card.metrics)[key]);
        if (typeof metric.value === "number" && Number.isFinite(metric.value) && text(metric.display)) details.push(`${label}: ${text(metric.display, 30)}`);
      }
    }
    if (category === "price_volume") {
      const macd = record(card.macd);
      if (macd.status === "ok" && text(macd.message)) details.push(text(macd.message));
    }
    return [{category, title: text(item.title, 120), description: text(item.description),
      dataAsOf: publicDate(card.as_of) || publicDate(card.latest_date) || publicDate(item.date),
      ...publicSources[category], details}];
  }).slice(0, 2);
  const supporting = project(decision.catalysts), risks = project(decision.risks);
  if (!supporting.length) return null;
  const watch = list(decision.watch_items).flatMap(value => {
    const item = record(value);
    return permitted(text(item.category)) && text(item.description) ? [text(item.description)] : [];
  }).slice(0, 2);
  // Omit scores, score history, analyst consensus, Pro sources and performance
  // entirely, even if an upstream response accidentally contains those fields.
  return {stock, generatedAt, supporting, risks, watch};
}

export function selectHomepageResearch(examples: Array<HomepageResearch | null>): HomepageResearch | null {
  const valid = examples.filter((item): item is HomepageResearch => item !== null);
  return valid.find(item => item.risks.length > 0) || valid[0] || null;
}
