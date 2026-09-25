// Significant template/link updates not represented by an article's original
// publication timestamp. Add dates only when the rendered article changes.
const reviewedUpdates: Readonly<Record<string, string>> = {
  "public-companies-winning-nasa-contracts": "2026-09-25",
  "public-companies-winning-department-of-defense-contracts": "2026-09-25",
  "boeing-government-contract-backlog-ba-stock": "2026-09-25",
  "who-is-buying-nvidia-stock-in-the-latest-13f-filings": "2026-09-25",
  "aapl-pre-earnings-quality-clear-ai-catalyst-needs-proof": "2026-09-25",
  "are-institutions-accumulating-apple-stock-in-2026": "2026-09-25",
  "spacex-stock-price-2030-forecast": "2026-09-25",
  "ai-memory-shortage-2030": "2026-09-25",
};

export function researchLastmod(brief: { slug: string; publishedAt?: string | null; updatedAt?: string | null }) {
  const today = new Date().toISOString().slice(0, 10);
  const dates = [brief.publishedAt, brief.updatedAt, reviewedUpdates[brief.slug]]
    .map(value => value?.match(/^\d{4}-\d{2}-\d{2}/)?.[0] ?? "")
    .filter(value => value && Number.isFinite(Date.parse(value)) && value <= today);
  return dates.sort().at(-1) ?? "";
}
