import type { ReactNode } from "react";
import Link from "next/link";
import type { AdminResearchBriefDraft } from "@/lib/api";
import { WALNUT_MARKETING_URL, marketingCanonicalUrl } from "@/lib/marketingMetadata";
import { ResearchBriefContextualCta } from "@/components/research/ResearchBriefContextualCta";
import { PremiumResearchGate } from "@/components/research/MuPremiumGate";

type StoredSignalResult = {
  ticker: string;
  eventDate: string;
  storedSignal: string;
  startClose: string;
  latestClose: string;
  returnPct: string;
  aligned: boolean;
  signalDirection: "bullish" | "bearish" | "mixed" | "neutral" | string;
};

type PriceMovePoint = {
  ticker: string;
  startDate: string;
  latestDate: string;
  startClose: number;
  latestClose: number;
  returnPct: string;
  aligned: boolean;
};

type GeneratedResearchArticleExtras = {
  signal_results?: StoredSignalResult[];
  price_move_charts?: PriceMovePoint[];
  current_data_as_of?: string;
  paywall_copy?: {
    heading?: string;
    description?: string;
  };
  analytics?: Record<string, string | number | boolean | null>;
};

function paragraphs(markdown: string) {
  return markdown
    .split(/\n{2,}/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function cleanInlineText(value: string) {
  return value.replace(/\*\*/g, "");
}

function safeLinkHref(value: string) {
  const href = value.trim();
  if (href.startsWith("https://") || href.startsWith("http://") || href.startsWith("/")) return href;
  return "";
}

function linkClassName() {
  return "font-semibold text-emerald-200 underline decoration-emerald-300/40 underline-offset-4 transition hover:text-emerald-100 hover:decoration-emerald-200";
}

function inlineMarkdown(text: string): ReactNode[] {
  const cleaned = cleanInlineText(text);
  const nodes: ReactNode[] = [];
  const markdownLinkPattern = /\[([^\]\n]+)\]\((https?:\/\/[^\s)]+|\/[^)\s]+)\)/g;
  let cursor = 0;
  let match: RegExpExecArray | null;
  let nodeIndex = 0;

  while ((match = markdownLinkPattern.exec(cleaned)) !== null) {
    if (match.index > cursor) {
      nodes.push(...autoLinkUrls(cleaned.slice(cursor, match.index), `text-${nodeIndex++}`));
    }
    const href = safeLinkHref(match[2]);
    nodes.push(
      href ? (
        <a key={`link-${nodeIndex++}`} href={href} target={href.startsWith("http") ? "_blank" : undefined} rel={href.startsWith("http") ? "noreferrer" : undefined} className={linkClassName()}>
          {match[1]}
        </a>
      ) : (
        match[1]
      ),
    );
    cursor = markdownLinkPattern.lastIndex;
  }

  if (cursor < cleaned.length) {
    nodes.push(...autoLinkUrls(cleaned.slice(cursor), `text-${nodeIndex++}`));
  }
  return nodes;
}

function autoLinkUrls(text: string, keyPrefix: string): ReactNode[] {
  const nodes: ReactNode[] = [];
  const urlPattern = /https?:\/\/[^\s<>()]+/g;
  let cursor = 0;
  let match: RegExpExecArray | null;
  let nodeIndex = 0;

  while ((match = urlPattern.exec(text)) !== null) {
    if (match.index > cursor) nodes.push(text.slice(cursor, match.index));
    const href = safeLinkHref(match[0].replace(/[.,;:!?]+$/, ""));
    const trailing = match[0].slice(href.length);
    nodes.push(
      <a key={`${keyPrefix}-url-${nodeIndex++}`} href={href} target="_blank" rel="noreferrer" className={linkClassName()}>
        {href}
      </a>,
    );
    if (trailing) nodes.push(trailing);
    cursor = match.index + match[0].length;
  }

  if (cursor < text.length) nodes.push(text.slice(cursor));
  return nodes;
}

type MarkdownBlock =
  | { type: "paragraph"; key: string; text: string }
  | { type: "table"; key: string; header: string[]; rows: string[][] };

function markdownBlocks(markdown: string): MarkdownBlock[] {
  return paragraphs(markdown).map((part, index) => parsePipeTable(part, index) ?? { type: "paragraph", key: `paragraph-${index}`, text: cleanInlineText(part) });
}

function parsePipeTable(part: string, index: number): MarkdownBlock | null {
  const lines = part.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n").map((line) => line.trim()).filter(Boolean);
  if (!lines.length || lines.some((line) => !line.includes("|"))) return null;

  if (lines.length === 1) {
    const cells = pipeCells(lines[0]);
    const columnCount = 3;
    if (cells.length < columnCount * 2) return null;
    const header = cells.slice(0, columnCount);
    let cursor = columnCount;
    if (isMarkdownDivider(cells.slice(cursor, cursor + columnCount))) cursor += columnCount;
    const rows: string[][] = [];
    for (; cursor + columnCount <= cells.length; cursor += columnCount) {
      rows.push(cells.slice(cursor, cursor + columnCount));
    }
    return rows.length ? { type: "table", key: `table-${index}`, header, rows } : null;
  }

  const parsedRows = lines.map(pipeCells).filter((cells) => cells.length >= 2);
  if (parsedRows.length < 2) return null;
  const header = parsedRows[0];
  const rows = parsedRows.slice(1).filter((cells) => !isMarkdownDivider(cells)).map((cells) => cells.slice(0, header.length));
  return rows.length ? { type: "table", key: `table-${index}`, header, rows } : null;
}

function pipeCells(line: string) {
  const cells = line.split("|").map((cell) => cleanInlineText(cell.trim()));
  if (cells[0] === "") cells.shift();
  if (cells[cells.length - 1] === "") cells.pop();
  return cells;
}

function isMarkdownDivider(cells: string[]) {
  return cells.length > 0 && cells.every((cell) => /^:?-{3,}:?$/.test(cell.replace(/\s/g, "")));
}

export function GeneratedResearchBriefPage({
  draft,
  returnTo,
  authenticated = false,
}: {
  draft: AdminResearchBriefDraft;
  returnTo?: string;
  authenticated?: boolean;
}) {
  const article = draft.article as AdminResearchBriefDraft["article"] & GeneratedResearchArticleExtras;
  const tickerHref = `/ticker/${encodeURIComponent(article.primary_ticker || draft.primary_ticker)}`;
  const primaryCtaHref = `/login?mode=register&return_to=${encodeURIComponent(tickerHref)}`;
  const canonicalUrl = marketingCanonicalUrl(`/research/${article.slug}`);
  const jsonLd = generatedResearchJsonLd(draft, canonicalUrl);
  const results = article.signal_results || [];
  const chartByTicker = new Map((article.price_move_charts || []).map((chart) => [chart.ticker.toLowerCase(), chart]));
  const showWalnutJudgment = Boolean(String(article.judgment || "").trim());
  const access = article.access;
  const fullArticleVisible = access?.premium_required ? access.full_article_visible !== false : true;
  const requiredPlan = access?.required_plan || article.required_plan || "premium";
  const tickers = [article.primary_ticker || draft.primary_ticker, ...(article.comparison_tickers || draft.comparison_tickers || [])].filter((ticker): ticker is string => Boolean(ticker));
  const heroImage = article.thumbnail_asset?.url || article.hero_image || "";
  const fallbackPaywallTitle = `Unlock Walnut's Full ${tickers.length > 1 ? `${tickers[0]} vs ${tickers[1]}` : article.primary_ticker || draft.primary_ticker} Research Brief`;
  const paywallHeading = article.paywall_copy?.heading || fallbackPaywallTitle;
  const paywallDescription = article.paywall_copy?.description || "See the full judgment, confirmation evidence, catalysts, risks, source trail, and what could change the setup.";

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd.article) }} />
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd.breadcrumbs) }} />
      <section className="border-b border-white/10 bg-[radial-gradient(circle_at_20%_0%,rgba(16,185,129,0.18),transparent_28%),linear-gradient(180deg,rgba(2,6,23,0.96),rgba(2,6,23,1))]">
        <div className="mx-auto max-w-5xl px-4 py-8 sm:px-6 lg:px-8">
          <Link href="/insights" className="inline-flex items-center gap-2 text-sm font-semibold text-emerald-200">
            <img src="/walnut-intel-logo-mark.png" alt="" className="h-6 w-6" />
            Walnut Research
          </Link>
          <div className="mt-10 max-w-3xl">
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{article.category || "Research Brief"}</p>
            <h1 className="mt-3 text-4xl font-semibold leading-tight text-white sm:text-5xl">{cleanInlineText(article.title)}</h1>
            <p className="mt-5 text-lg leading-8 text-slate-300">{cleanInlineText(article.subtitle || article.summary)}</p>
            <div className="mt-7 flex flex-wrap gap-3">
              <Link href={primaryCtaHref} className="inline-flex min-h-11 items-center justify-center rounded-lg bg-emerald-300 px-5 py-2.5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
                See what Walnut is showing now
              </Link>
              <Link href={tickerHref} className="inline-flex min-h-11 items-center justify-center rounded-lg border border-white/15 px-5 py-2.5 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/50 hover:text-emerald-100">
                Research a ticker
              </Link>
            </div>
            <p className="mt-4 text-xs leading-5 text-slate-500">Research and informational purposes only. Not investment advice. Historical outcomes do not guarantee future results.</p>
          </div>
          {heroImage ? <ResearchHeroImage src={heroImage} title={article.title} /> : results.length ? <StoredSignalsHeroGraphic results={results} /> : null}
        </div>
      </section>

      <section className="mx-auto grid max-w-5xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[minmax(0,1fr)_18rem] lg:px-8">
        <article className="min-w-0 space-y-8">
          {results.length ? <StoredSignalResultsTable results={results} /> : null}
          {article.sections.map((section) => (
            <section key={section.key} className={`rounded-lg border bg-slate-950/50 p-5 ${section.key === "meta-miss" ? "border-rose-300/35 shadow-[0_18px_60px_-42px_rgba(251,113,133,0.65)]" : "border-white/10"}`}>
              <h2 className="text-2xl font-semibold text-white">{cleanInlineText(section.heading)}</h2>
              {chartByTicker.get(section.key) ? <PriceMoveChart chart={chartByTicker.get(section.key)!} /> : null}
              <div className="mt-4 space-y-4 text-sm leading-7 text-slate-300">
                {markdownBlocks(section.body_markdown).map((block) =>
                  block.type === "table" ? <ResearchDataTable key={block.key} header={block.header} rows={block.rows} /> : <p key={block.key}>{inlineMarkdown(block.text)}</p>,
                )}
              </div>
            </section>
          ))}
          {fullArticleVisible ? (
            <ResearchBriefContextualCta
              ticker={article.primary_ticker || draft.primary_ticker}
              companyName={article.primary_ticker || draft.primary_ticker}
              researchSlug={article.slug}
            />
          ) : (
            <PremiumResearchGate
              authState={authenticated ? "free" : "logged_out"}
              entitlement={authenticated ? "free" : "logged_out"}
              returnTo={returnTo || `/research/${article.slug}`}
              articleSlug={article.slug}
              tickers={tickers}
              requiredPlan={requiredPlan}
              heading={paywallHeading}
              description={paywallDescription}
              analytics={article.analytics}
            />
          )}
        </article>

        <aside className="space-y-4">
          {showWalnutJudgment && fullArticleVisible ? (
            <div className="rounded-lg border border-white/10 bg-slate-950/60 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Walnut Judgment</p>
              <p className="mt-2 text-lg font-semibold capitalize text-white">{article.judgment}</p>
              <p className="mt-2 text-sm leading-6 text-slate-400">{cleanInlineText(article.summary)}</p>
            </div>
          ) : null}
          {fullArticleVisible ? <SideList title="Catalysts" items={article.catalysts} /> : null}
          {fullArticleVisible ? <SideList title="Risks" items={article.risks} /> : null}
          {fullArticleVisible ? <SideList title="What to watch" items={article.watch_items} /> : null}
          {fullArticleVisible ? <SourceList items={article.source_links || []} /> : null}
          <TickerLookupCard currentDataAsOf={article.current_data_as_of} />
        </aside>
      </section>
    </main>
  );
}

function ResearchHeroImage({ src, title }: { src: string; title: string }) {
  return (
    <div className="mt-10 overflow-hidden rounded-lg border border-white/10 bg-slate-900">
      <img src={safeLinkHref(src)} alt={`${cleanInlineText(title)} hero image`} className="aspect-[16/9] w-full object-cover" />
    </div>
  );
}

export function generatedResearchJsonLd(draft: AdminResearchBriefDraft, canonicalUrl: string) {
  const article = draft.article;
  const logoUrl = `${WALNUT_MARKETING_URL}/walnut-intel-logo-mark.png`;
  const imageUrl = absoluteMarketingAssetUrl(article.thumbnail_asset?.url || article.hero_image || logoUrl);
  return {
    article: {
      "@context": "https://schema.org",
      "@type": "Article",
      headline: cleanInlineText(article.title),
      description: article.seo?.description || article.summary,
      datePublished: draft.published_at || draft.created_at,
      dateModified: draft.updated_at,
      mainEntityOfPage: canonicalUrl,
      publisher: {
        "@type": "Organization",
        name: "Walnut Markets",
        logo: {
          "@type": "ImageObject",
          url: logoUrl,
        },
      },
      image: imageUrl,
      about: [article.primary_ticker, ...(article.comparison_tickers || [])].filter(Boolean),
    },
    breadcrumbs: {
      "@context": "https://schema.org",
      "@type": "BreadcrumbList",
      itemListElement: [
        {
          "@type": "ListItem",
          position: 1,
          name: "Insights",
          item: marketingCanonicalUrl("/insights"),
        },
        {
          "@type": "ListItem",
          position: 2,
          name: cleanInlineText(article.title),
          item: canonicalUrl,
        },
      ],
    },
  };
}

function absoluteMarketingAssetUrl(value: string) {
  if (value.startsWith("https://") || value.startsWith("http://")) return value;
  return marketingCanonicalUrl(value.startsWith("/") ? value : `/${value}`);
}

function StoredSignalsHeroGraphic({ results }: { results: StoredSignalResult[] }) {
  const alignedCount = results.filter((item) => item.aligned).length;
  const missCount = results.length - alignedCount;
  return (
    <div className="mt-10 grid gap-3 sm:grid-cols-3">
      <HeroMetric label="Aligned stored signals" value={`${alignedCount}`} detail="Audited case studies" />
      <HeroMetric label="Clear miss" value={`${missCount}`} detail="META moved against the signal" tone="warn" />
      <HeroMetric label="Latest close date" value="2026-07-29" detail="Stored price_cache closes" />
    </div>
  );
}

function HeroMetric({ label, value, detail, tone = "default" }: { label: string; value: string; detail: string; tone?: "default" | "warn" }) {
  return (
    <div className={`rounded-lg border p-4 ${tone === "warn" ? "border-rose-300/30 bg-rose-950/20" : "border-emerald-300/20 bg-emerald-300/10"}`}>
      <p className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">{label}</p>
      <p className={`mt-2 text-3xl font-semibold ${tone === "warn" ? "text-rose-100" : "text-emerald-100"}`}>{value}</p>
      <p className="mt-1 text-xs leading-5 text-slate-400">{detail}</p>
    </div>
  );
}

function StoredSignalResultsTable({ results }: { results: StoredSignalResult[] }) {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/50 p-5">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">Audited stored signals</p>
          <h2 className="mt-2 text-2xl font-semibold text-white">Signal results table</h2>
        </div>
        <p className="text-xs leading-5 text-slate-500">Historical records only. Current data is separate.</p>
      </div>
      <div className="mt-4 overflow-x-auto rounded-lg border border-white/10">
        <table className="min-w-full border-collapse text-left text-sm">
          <thead className="bg-emerald-300/10 text-slate-100">
            <tr>
              {["Ticker", "Event", "Stored signal", "Start close", "Latest close", "Move", "Result"].map((cell) => (
                <th key={cell} className="px-3 py-3 text-xs font-semibold uppercase tracking-[0.08em]">
                  {cell}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {results.map((row) => (
              <tr key={row.ticker} className={row.aligned ? "bg-slate-900/55" : "bg-rose-950/30"}>
                <td className="border-t border-white/10 px-3 py-3 align-top font-semibold text-white">
                  <Link href={`/ticker/${row.ticker}`} className={linkClassName()}>{row.ticker}</Link>
                </td>
                <td className="border-t border-white/10 px-3 py-3 align-top text-slate-300">{row.eventDate}</td>
                <td className="border-t border-white/10 px-3 py-3 align-top text-slate-300">{row.storedSignal}</td>
                <td className="border-t border-white/10 px-3 py-3 align-top text-slate-300">{row.startClose}</td>
                <td className="border-t border-white/10 px-3 py-3 align-top text-slate-300">{row.latestClose}</td>
                <td className={`border-t border-white/10 px-3 py-3 align-top font-semibold ${row.returnPct.startsWith("+") ? "text-emerald-200" : "text-rose-200"}`}>{row.returnPct}</td>
                <td className="border-t border-white/10 px-3 py-3 align-top">
                  <span className={`inline-flex rounded-md border px-2 py-1 text-xs font-semibold ${row.aligned ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100" : "border-rose-300/40 bg-rose-300/10 text-rose-100"}`}>
                    {row.aligned ? "Aligned" : "Miss"}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function PriceMoveChart({ chart }: { chart: PriceMovePoint }) {
  const start = Number.isFinite(chart.startClose) ? chart.startClose : 0;
  const latest = Number.isFinite(chart.latestClose) ? chart.latestClose : 0;
  const high = Math.max(start, latest, 1);
  const startHeight = Math.max(12, Math.round((start / high) * 96));
  const latestHeight = Math.max(12, Math.round((latest / high) * 96));
  return (
    <div className={`mt-4 rounded-lg border p-4 ${chart.aligned ? "border-emerald-300/20 bg-emerald-300/5" : "border-rose-300/30 bg-rose-950/20"}`}>
      <div className="flex items-center justify-between gap-3">
        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">{chart.ticker} stored-close move</p>
        <p className={`text-sm font-semibold ${chart.returnPct.startsWith("+") ? "text-emerald-200" : "text-rose-200"}`}>{chart.returnPct}</p>
      </div>
      <div className="mt-4 flex h-32 items-end gap-5">
        <ChartBar label={chart.startDate} value={`$${chart.startClose.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`} height={startHeight} />
        <ChartBar label={chart.latestDate} value={`$${chart.latestClose.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`} height={latestHeight} />
      </div>
    </div>
  );
}

function ChartBar({ label, value, height }: { label: string; value: string; height: number }) {
  return (
    <div className="flex min-w-24 flex-1 flex-col items-center gap-2">
      <div className="flex h-24 w-full items-end justify-center">
        <div className="w-full max-w-24 rounded-t-md bg-emerald-300/55" style={{ height }} />
      </div>
      <p className="text-xs font-semibold text-slate-200">{value}</p>
      <p className="text-[11px] text-slate-500">{label}</p>
    </div>
  );
}

function ResearchDataTable({ header, rows }: { header: string[]; rows: string[][] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-white/10">
      <table className="min-w-full border-collapse text-left text-sm">
        <thead className="bg-emerald-300/10 text-slate-100">
          <tr>
            {header.map((cell) => (
              <th key={cell} className="px-3 py-3 text-xs font-semibold uppercase tracking-[0.08em]">
                {cell}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={`${rowIndex}-${row.join("|")}`} className={row.some((cell) => /\bMETA\b|miss/i.test(cell)) ? "bg-rose-950/30" : rowIndex % 2 === 0 ? "bg-slate-900/55" : "bg-slate-800/35"}>
              {header.map((_, cellIndex) => (
                <td key={`${rowIndex}-${cellIndex}`} className="border-t border-white/10 px-3 py-3 align-top text-slate-300">
                  {inlineMarkdown(row[cellIndex] || "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SideList({ title, items }: { title: string; items?: string[] }) {
  if (!items?.length) return null;
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/60 p-4">
      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">{title}</p>
      <ul className="mt-3 space-y-2 text-sm leading-6 text-slate-300">
        {(items || []).slice(0, 5).map((item) => (
          <li key={item}>{inlineMarkdown(item)}</li>
        ))}
      </ul>
    </div>
  );
}

function TickerLookupCard({ currentDataAsOf }: { currentDataAsOf?: string }) {
  return (
    <div className="rounded-lg border border-emerald-300/20 bg-emerald-300/10 p-4">
      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-200">Current setup</p>
      {currentDataAsOf ? <p className="mt-2 text-xs leading-5 text-slate-400">Current data should be read separately from this historical audit. Current as of {currentDataAsOf}.</p> : null}
      <form action="/search" className="mt-4 flex gap-2">
        <input name="q" placeholder="Enter a ticker" className="min-w-0 flex-1 rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-emerald-300/50" />
        <button type="submit" className="rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
          See setup
        </button>
      </form>
    </div>
  );
}

function SourceList({ items }: { items: Array<{ label: string; url: string; source_type: string }> }) {
  if (!items.length) return null;
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/60 p-4">
      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Sources</p>
      <ul className="mt-3 space-y-2 text-sm leading-6 text-slate-300">
        {items.slice(0, 6).map((item) => (
          <li key={`${item.label}:${item.url}`}>
            <a href={safeLinkHref(item.url)} className={linkClassName()}>
              {cleanInlineText(item.label)}
            </a>
          </li>
        ))}
      </ul>
    </div>
  );
}
