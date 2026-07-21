"use client";

import { useMemo, useState } from "react";
import type { PeerCompareResponse, TickerContextBundleResponse, TickerDecisionLayer, TickerSignalsSummaryResponse } from "@/lib/api";
import { ghostButtonClassName } from "@/lib/styles";

type TickerResearchSubject = {
  kind: "ticker";
  symbol: string;
  companyName?: string | null;
  canonicalUrl?: string | null;
  quote?: TickerContextBundleResponse["quote"] | null;
  decisionLayer?: TickerDecisionLayer | null;
  signalsSummary?: TickerSignalsSummaryResponse | null;
};

type CompareResearchSubject = {
  kind: "compare";
  data: PeerCompareResponse;
};

type ResearchSubject = TickerResearchSubject | CompareResearchSubject;

export function ResearchActions({ subject, canCreateResearch }: { subject: ResearchSubject; canCreateResearch: boolean }) {
  const [open, setOpen] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const outputs = useMemo(() => buildResearchOutputs(subject), [subject]);

  if (!canCreateResearch) return null;

  const run = async (action: () => Promise<string> | string) => {
    try {
      const message = await action();
      setStatus(message);
      window.setTimeout(() => setStatus(null), 2500);
    } catch {
      setStatus("Action unavailable");
    }
  };

  return (
    <div className="relative flex items-center gap-2">
      {subject.kind === "compare" ? (
        <button type="button" onClick={() => run(() => shareResearchUrl())} className={ghostButtonClassName}>
          Share
        </button>
      ) : null}
      <div className="relative">
        <button type="button" onClick={() => setOpen((value) => !value)} className={ghostButtonClassName}>
          Create Research
        </button>
        {open ? (
          <div className="absolute right-0 z-30 mt-2 w-72 rounded-lg border border-white/10 bg-slate-950 p-2 text-sm shadow-2xl shadow-black/40">
            <ResearchActionButton label="Copy Walnut Take" onClick={() => run(() => copyText(outputs.take, "Walnut Take copied"))} />
            <ResearchActionButton label="Copy Data Bullets" onClick={() => run(() => copyText(outputs.bullets.join("\n"), "Data bullets copied"))} />
            <ResearchActionButton label="Create X Card" onClick={() => run(() => downloadText(outputs.xCardSvg, outputs.slug + "-x-card.svg", "image/svg+xml", "X card downloaded"))} />
            <ResearchActionButton label="Create Reddit DD Outline" onClick={() => run(() => copyText(outputs.redditOutline, "Reddit outline copied"))} />
            <ResearchActionButton label="Export Research Brief" onClick={() => run(() => downloadPdf(outputs.title, outputs.briefLines, outputs.slug + "-research-brief.pdf"))} />
          </div>
        ) : null}
      </div>
      {status ? <p className="absolute right-0 top-full z-30 mt-2 rounded-md border border-emerald-300/20 bg-slate-950 px-3 py-2 text-xs font-medium text-emerald-200 shadow-xl shadow-black/30">{status}</p> : null}
    </div>
  );
}

function ResearchActionButton({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="block w-full rounded-md px-3 py-2 text-left font-semibold text-slate-200 transition hover:bg-white/10 hover:text-white"
    >
      {label}
    </button>
  );
}

type ResearchOutputs = {
  title: string;
  slug: string;
  take: string;
  bullets: string[];
  comparisonConclusion?: string | null;
  redditOutline: string;
  briefLines: string[];
  xCardSvg: string;
};

export function buildResearchOutputs(subject: ResearchSubject): ResearchOutputs {
  return subject.kind === "compare" ? buildCompareOutputs(subject.data) : buildTickerOutputs(subject);
}

function buildTickerOutputs(subject: TickerResearchSubject): ResearchOutputs {
  const symbol = clean(subject.symbol).toUpperCase();
  const layer = subject.decisionLayer;
  const confirmation = layer?.confirmation ?? {};
  const score = typeof confirmation.score === "number" ? Math.round(confirmation.score) : null;
  const label = clean(confirmation.label) || clean(confirmation.band) || "No active confirmation";
  const direction = clean(confirmation.direction) || "neutral";
  const company = clean(subject.companyName);
  const summary = clean(layer?.summary) || `${symbol} has limited decision-layer context in the current view.`;
  const price = typeof subject.quote?.current_price === "number" ? `$${subject.quote.current_price.toFixed(2)}` : null;
  const changeText = typeof subject.quote?.change_percent === "number" ? `${subject.quote.change_percent >= 0 ? "+" : ""}${subject.quote.change_percent.toFixed(2)}%` : "Unavailable";
  const volumeVsAverage = typeof subject.signalsSummary?.price_volume?.volume_vs_avg === "number" ? `${subject.signalsSummary.price_volume.volume_vs_avg.toFixed(2)}x` : "Unavailable";
  const recentSignals = typeof subject.signalsSummary?.recent_signal_count === "number" ? String(subject.signalsSummary.recent_signal_count) : "Unavailable";
  const chartPoints = (confirmation.history ?? [])
    .map((point) => (typeof point.score === "number" ? point.score : null))
    .filter((value): value is number => value !== null)
    .slice(-24);
  const evidence = [
    ...itemsToPlain(layer?.what_changed, "").slice(0, 1),
    ...itemsToPlain(layer?.catalysts, "").slice(0, 2),
    ...itemsToPlain(layer?.risks, "").slice(0, 1),
  ]
    .map((item) => clean(item.replace(/^-+\s*/, "")))
    .filter(Boolean)
    .slice(0, 4);
  const generatedAt = clean(layer?.generated_at || subject.quote?.as_of);
  const title = `${symbol}${company ? " / " + company : ""} Research Brief`;
  const bullets = [
    `- Confirmation: ${score === null ? "unavailable" : score + "/100"} ${label} (${direction}).`,
    price ? `- Last quote: ${price}${typeof subject.quote?.change_percent === "number" ? ` (${subject.quote.change_percent.toFixed(2)}%)` : ""}.` : "- Last quote: unavailable in current payload.",
    ...itemsToBullets("What changed", layer?.what_changed),
    ...itemsToBullets("Catalysts", layer?.catalysts),
    ...itemsToBullets("Risks", layer?.risks),
    ...itemsToBullets("Watch next", layer?.watch_items),
    generatedAt ? `- Data generated: ${generatedAt}.` : "- Data generated: unavailable.",
  ];
  const take = `${symbol}: ${score === null ? label : `${score}/100 ${label}`}. ${summary}`;
  const redditOutline = [
    `Title: ${symbol} DD - Walnut decision-layer snapshot`,
    "",
    "TL;DR",
    take,
    "",
    "Why this name came up",
    bullets.slice(0, 4).join("\n"),
    "",
    "What would confirm the setup",
    itemsToPlain(layer?.catalysts, "No catalyst confirmation is visible in the current data.").join("\n"),
    "",
    "What would weaken the setup",
    itemsToPlain(layer?.risks, "No explicit weakening signal is visible in the current data.").join("\n"),
    "",
    "What to watch next",
    itemsToPlain(layer?.watch_items, "No watch items are available in the current data.").join("\n"),
    "",
    "Disclosure",
    "Generated from Walnut production data. Not financial advice.",
  ].join("\n");
  const briefLines = [title, "", take, "", "Data bullets", ...bullets, "", "Reddit DD outline", ...redditOutline.split("\n")];
  return {
    title,
    slug: symbol.toLowerCase(),
    take,
    bullets,
    comparisonConclusion: null,
    redditOutline,
    briefLines,
    xCardSvg: socialCardSvg({
      eyebrow: "Walnut Ticker Research",
      title: symbol,
      subtitle: company || summary,
      accent: direction === "bearish" ? "#fb7185" : direction === "bullish" ? "#34d399" : "#38bdf8",
      tone: direction,
      metric: score === null ? label : `${score}/100`,
      metricLabel: label,
      kicker: summary,
      stats: [
        { label: "Last price", value: price ?? "Unavailable" },
        { label: "1D change", value: changeText },
        { label: "Volume vs avg", value: volumeVsAverage },
        { label: "Signals", value: recentSignals },
      ],
      evidence,
      chartLabel: chartPoints.length >= 2 ? "Confirmation trend" : "Visible setup",
      chartPoints: chartPoints.length >= 2 ? chartPoints : fallbackCardSeries(score ?? subject.signalsSummary?.latest_signal_score ?? 50, subject.quote?.change_percent ?? 0),
      footer: "walnut.markets",
    }),
  };
}

function buildCompareOutputs(data: PeerCompareResponse): ResearchOutputs {
  const left = data.left.symbol;
  const right = data.right.symbol;
  const winner = data.call.winner === "even" ? "Even" : data.call.symbol || (data.call.winner === "left" ? left : right);
  const title = `${left} vs ${right} Research Brief`;
  const comparisonConclusion = `${left} vs ${right}: ${winner === "Even" ? "too close to call" : `${winner} leads`}. ${data.call.summary}`;
  const visibleCategories = data.categories.filter((category) => !category.locked);
  const leftEdges = visibleCategories.filter((category) => category.edge === "left").length;
  const rightEdges = visibleCategories.filter((category) => category.edge === "right").length;
  const evenEdges = visibleCategories.filter((category) => category.edge === "even").length;
  const bullets = [
    `- Call: ${comparisonConclusion}`,
    ...data.call.drivers.map((driver) => `- Driver: ${driver}.`),
    ...data.categories.filter((category) => !category.locked).map((category) => `- ${category.label}: ${edgeLabel(category.edge, data)} edge.`),
    ...data.tradeoffs.map((tradeoff) => `- Tradeoff: ${tradeoff}.`),
    ...data.notes.map((note) => `- Note: ${note}.`),
  ];
  const redditOutline = [
    `Title: ${left} vs ${right} - Walnut peer compare`,
    "",
    "TL;DR",
    comparisonConclusion,
    "",
    "Why compare these",
    `${left} and ${right} are being compared across the visible Walnut peer categories.`,
    "",
    "Evidence bullets",
    bullets.join("\n"),
    "",
    "Counterpoints",
    data.tradeoffs.length ? data.tradeoffs.map((item) => `- ${item}`).join("\n") : "- No material counter-edge in the visible categories.",
    "",
    "Bottom line",
    comparisonConclusion,
    "",
    "Disclosure",
    "Generated from Walnut production data. Not financial advice.",
  ].join("\n");
  const briefLines = [title, "", comparisonConclusion, "", "Data bullets", ...bullets, "", "Reddit DD outline", ...redditOutline.split("\n")];
  return {
    title,
    slug: `${left.toLowerCase()}-${right.toLowerCase()}`,
    take: comparisonConclusion,
    bullets,
    comparisonConclusion,
    redditOutline,
    briefLines,
    xCardSvg: socialCardSvg({
      eyebrow: "Walnut Peer Compare",
      title: `${left} vs ${right}`,
      subtitle: data.call.summary,
      accent: data.call.winner === "right" ? "#a78bfa" : data.call.winner === "left" ? "#22d3ee" : "#94a3b8",
      tone: data.call.winner === "even" ? "neutral" : "bullish",
      metric: winner === "Even" ? "Even" : `${winner} leads`,
      metricLabel: "Our call",
      kicker: comparisonConclusion,
      stats: [
        { label: left, value: `${leftEdges} edges` },
        { label: right, value: `${rightEdges} edges` },
        { label: "Even", value: `${evenEdges} categories` },
        { label: "Window", value: `${data.lookback_days}D` },
      ],
      evidence: [
        ...data.call.drivers.map((driver) => `${driver} driver`),
        ...data.tradeoffs.map((tradeoff) => `${tradeoff} tradeoff`),
      ].slice(0, 4),
      chartLabel: "Category edge map",
      chartPoints: visibleCategories.map((category) => (category.edge === "left" ? 78 : category.edge === "right" ? 28 : 52)).slice(0, 12),
      footer: "walnut.markets",
    }),
  };
}

function itemsToBullets(label: string, items?: { title?: string; description?: string; value?: number | string | null }[] | null): string[] {
  const visible = (items ?? []).slice(0, 3);
  if (!visible.length) return [];
  return visible.map((item) => {
    const value = item.value === null || item.value === undefined ? "" : ` (${item.value})`;
    return `- ${label}: ${clean(item.title) || "Untitled"}${value}${item.description ? ` - ${clean(item.description)}` : ""}.`;
  });
}

function itemsToPlain(items?: { title?: string; description?: string }[] | null, fallback = "Unavailable."): string[] {
  const visible = (items ?? []).slice(0, 4);
  if (!visible.length) return [`- ${fallback}`];
  return visible.map((item) => `- ${clean(item.title) || "Untitled"}${item.description ? `: ${clean(item.description)}` : ""}`);
}

function edgeLabel(edge: "left" | "right" | "even", data: PeerCompareResponse) {
  if (edge === "left") return data.left.symbol;
  if (edge === "right") return data.right.symbol;
  return "Even";
}

function clean(value: unknown): string {
  return typeof value === "string" ? value.replace(/\s+/g, " ").trim() : "";
}

async function copyText(value: string, message: string) {
  await navigator.clipboard.writeText(value);
  return message;
}

function downloadText(value: string, filename: string, type: string, message: string) {
  const blob = new Blob([value], { type });
  downloadBlob(blob, filename);
  return message;
}

async function shareResearchUrl() {
  const url = window.location.href;
  if (navigator.share) {
    await navigator.share({ title: document.title, url });
    return "Share sheet opened";
  }
  await navigator.clipboard.writeText(url);
  return "Research URL copied";
}

function downloadPdf(title: string, lines: string[], filename: string) {
  const blob = new Blob([buildSimplePdf(title, lines)], { type: "application/pdf" });
  downloadBlob(blob, filename);
  return "Research brief downloaded";
}

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function socialCardSvg({
  eyebrow,
  title,
  subtitle,
  metric,
  metricLabel,
  kicker,
  accent,
  tone,
  stats = [],
  evidence = [],
  chartLabel = "Score trend",
  chartPoints = [],
  footer,
}: {
  eyebrow: string;
  title: string;
  subtitle: string;
  metric: string;
  metricLabel?: string;
  kicker?: string;
  accent: string;
  tone?: string;
  stats?: { label: string; value: string }[];
  evidence?: string[];
  chartLabel?: string;
  chartPoints?: number[];
  footer: string;
}) {
  const safeTitle = escapeXml(title).slice(0, 42);
  const subtitleLines = svgTextLines(subtitle, 44, 2);
  const kickerLines = svgTextLines(kicker || subtitle, 70, 2);
  const safeMetric = escapeXml(metric).slice(0, 28);
  const safeMetricLabel = escapeXml(metricLabel || "Walnut score").slice(0, 34);
  const chartPath = svgLinePath(normalizeCardSeries(chartPoints), 674, 276, 402, 152);
  const areaPath = chartPath ? `${chartPath} L 1076 428 L 674 428 Z` : "";
  const bars = normalizeCardSeries(chartPoints.length ? chartPoints : fallbackCardSeries(52, 0)).slice(0, 18);
  const toneLabel = clean(tone).toUpperCase() || "ACTIVE";
  return `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="675" viewBox="0 0 1200 675">
  <defs>
    <linearGradient id="walnut-bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#07111f"/>
      <stop offset="52%" stop-color="#0b1726"/>
      <stop offset="100%" stop-color="#020617"/>
    </linearGradient>
    <linearGradient id="walnut-accent" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0%" stop-color="${accent}" stop-opacity="0.95"/>
      <stop offset="100%" stop-color="#38bdf8" stop-opacity="0.72"/>
    </linearGradient>
    <filter id="soft-shadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="0" dy="18" stdDeviation="18" flood-color="#000000" flood-opacity="0.28"/>
    </filter>
  </defs>
  <rect width="1200" height="675" fill="url(#walnut-bg)"/>
  <rect x="36" y="34" width="1128" height="607" rx="30" fill="#0b1726" stroke="#244058" stroke-width="2" filter="url(#soft-shadow)"/>
  <rect x="36" y="34" width="1128" height="7" rx="3" fill="url(#walnut-accent)"/>
  <path d="M764 34 L1164 34 L1164 641 L944 641 C974 538 958 438 895 342 C842 262 786 177 764 34Z" fill="${accent}" opacity="0.08"/>
  <text x="76" y="94" fill="#34d399" font-family="Inter, Arial, sans-serif" font-size="20" font-weight="800" letter-spacing="5">${escapeXml(eyebrow)}</text>
  <rect x="964" y="72" width="142" height="34" rx="17" fill="${accent}" opacity="0.16" stroke="${accent}" stroke-opacity="0.55"/>
  <text x="1006" y="95" fill="${accent}" font-family="Inter, Arial, sans-serif" font-size="13" font-weight="800" letter-spacing="2">${escapeXml(toneLabel).slice(0, 18)}</text>

  <text x="76" y="184" fill="#f8fafc" font-family="Inter, Arial, sans-serif" font-size="${safeTitle.length > 16 ? 58 : 72}" font-weight="900">${safeTitle}</text>
  ${subtitleLines.map((line, index) => `<text x="78" y="${228 + index * 31}" fill="#cbd5e1" font-family="Inter, Arial, sans-serif" font-size="28" font-weight="500">${escapeXml(line)}</text>`).join("\n  ")}

  <rect x="76" y="304" width="250" height="118" rx="18" fill="#0f2032" stroke="${accent}" stroke-opacity="0.55"/>
  <text x="102" y="344" fill="#8aa4bd" font-family="Inter, Arial, sans-serif" font-size="14" font-weight="800" letter-spacing="3">${safeMetricLabel.toUpperCase()}</text>
  <text x="102" y="396" fill="${accent}" font-family="Inter, Arial, sans-serif" font-size="52" font-weight="900">${safeMetric}</text>

  <rect x="352" y="304" width="260" height="118" rx="18" fill="#081524" stroke="#203447"/>
  ${kickerLines.map((line, index) => `<text x="378" y="${350 + index * 28}" fill="#dbeafe" font-family="Inter, Arial, sans-serif" font-size="21" font-weight="650">${escapeXml(line)}</text>`).join("\n  ")}

  <rect x="76" y="454" width="536" height="118" rx="18" fill="#081524" stroke="#203447"/>
  ${stats.slice(0, 4).map((stat, index) => {
    const x = 102 + index * 128;
    return `<text x="${x}" y="494" fill="#7f97b2" font-family="Inter, Arial, sans-serif" font-size="12" font-weight="800" letter-spacing="2">${escapeXml(stat.label).slice(0, 18).toUpperCase()}</text>
  <text x="${x}" y="538" fill="#f8fafc" font-family="Inter, Arial, sans-serif" font-size="25" font-weight="850">${escapeXml(stat.value).slice(0, 18)}</text>`;
  }).join("\n  ")}

  <rect x="646" y="142" width="486" height="324" rx="22" fill="#07111f" stroke="#203447"/>
  <text x="674" y="186" fill="#8aa4bd" font-family="Inter, Arial, sans-serif" font-size="13" font-weight="800" letter-spacing="3">${escapeXml(chartLabel).toUpperCase()}</text>
  <line x1="674" y1="428" x2="1076" y2="428" stroke="#22384d"/>
  <line x1="674" y1="352" x2="1076" y2="352" stroke="#17283a"/>
  <line x1="674" y1="276" x2="1076" y2="276" stroke="#17283a"/>
  ${areaPath ? `<path d="${areaPath}" fill="url(#walnut-accent)" opacity="0.12"/>` : ""}
  ${chartPath ? `<path d="${chartPath}" fill="none" stroke="${accent}" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/>` : ""}
  ${bars.map((value, index) => {
    const width = 12;
    const gap = 10;
    const x = 684 + index * (width + gap);
    const h = 18 + (value / 100) * 70;
    const y = 430 - h;
    const barColor = value >= 55 ? "#34d399" : value <= 45 ? "#fb7185" : "#38bdf8";
    return `<rect x="${x}" y="${y.toFixed(1)}" width="${width}" height="${h.toFixed(1)}" rx="4" fill="${barColor}" opacity="0.72"/>`;
  }).join("\n  ")}

  <rect x="646" y="490" width="486" height="82" rx="18" fill="#081524" stroke="#203447"/>
  ${evidence.length ? evidence.slice(0, 3).map((item, index) => {
    const y = 522 + index * 22;
    return `<rect x="674" y="${y - 12}" width="9" height="9" rx="2" fill="${accent}"/>
  <text x="696" y="${y}" fill="#dbeafe" font-family="Inter, Arial, sans-serif" font-size="16" font-weight="650">${escapeXml(item).slice(0, 52)}</text>`;
  }).join("\n  ") : `<text x="674" y="538" fill="#94a3b8" font-family="Inter, Arial, sans-serif" font-size="17" font-weight="650">Production data snapshot from Walnut.</text>`}

  <text x="76" y="614" fill="#64748b" font-family="Inter, Arial, sans-serif" font-size="20" font-weight="700">${escapeXml(footer)}</text>
  <text x="958" y="614" fill="#64748b" font-family="Inter, Arial, sans-serif" font-size="16" font-weight="700" letter-spacing="2">NOT FINANCIAL ADVICE</text>
</svg>`;
}

function svgTextLines(value: string, maxChars: number, maxLines: number) {
  const words = clean(value).split(" ").filter(Boolean);
  const lines: string[] = [];
  let current = "";
  words.forEach((word) => {
    const next = current ? `${current} ${word}` : word;
    if (next.length > maxChars && current) {
      lines.push(current);
      current = word;
    } else {
      current = next;
    }
  });
  if (current) lines.push(current);
  const visible = lines.slice(0, maxLines);
  if (lines.length > maxLines && visible.length) {
    visible[visible.length - 1] = `${visible[visible.length - 1].replace(/[.,;:]$/, "")}...`;
  }
  return visible.length ? visible : ["Production data snapshot"];
}

function normalizeCardSeries(values: number[]) {
  const parsed = values.filter((value) => Number.isFinite(value));
  if (!parsed.length) return fallbackCardSeries(50, 0);
  return parsed.map((value) => Math.max(0, Math.min(100, value)));
}

function fallbackCardSeries(score: number, changePercent: number) {
  const base = Math.max(12, Math.min(88, score));
  const tilt = Math.max(-12, Math.min(12, changePercent * 2));
  return [
    base - 10,
    base - 4,
    base + tilt * 0.2,
    base + 6 + tilt * 0.4,
    base + 2 + tilt * 0.8,
    base + tilt,
  ].map((value) => Math.max(0, Math.min(100, Math.round(value))));
}

function svgLinePath(values: number[], x: number, y: number, width: number, height: number) {
  if (values.length < 2) return "";
  const step = width / Math.max(1, values.length - 1);
  return values
    .map((value, index) => {
      const px = x + index * step;
      const py = y + height - (Math.max(0, Math.min(100, value)) / 100) * height;
      return `${index === 0 ? "M" : "L"} ${px.toFixed(1)} ${py.toFixed(1)}`;
    })
    .join(" ");
}

function buildSimplePdf(title: string, lines: string[]) {
  const sections = briefSections(title, lines);
  const summary = sections[0]?.lines[0] || "Generated from Walnut production data.";
  const pages: string[] = [];
  let commands: string[] = [];
  let pageNumber = 0;
  let y = 0;

  const startPage = (first = false) => {
    if (commands.length) pages.push(commands.join("\n"));
    pageNumber += 1;
    commands = [];
    drawPageBase(commands, title, pageNumber, first);
    y = first ? 500 : 650;
    if (first) {
      drawSummaryCard(commands, summary);
    }
  };

  const ensureSpace = (height: number) => {
    if (y - height < 62) startPage(false);
  };

  startPage(true);
  sections.forEach((section, sectionIndex) => {
    ensureSpace(sectionIndex === 0 ? 42 : 58);
    drawText(commands, section.heading.toUpperCase(), 54, y, 10, "F2", [0.08, 0.33, 0.48], 1.6);
    y -= 19;
    if (!section.lines.length) {
      drawText(commands, "No additional details available in the current payload.", 54, y, 10, "F1", [0.38, 0.45, 0.55]);
      y -= 20;
      return;
    }
    section.lines.forEach((line) => {
      const bullet = line.trim().startsWith("-");
      const cleanLine = bullet ? line.replace(/^-+\s*/, "") : line;
      const wrapped = wrapPdfLine(asPdfText(cleanLine), bullet ? 86 : 92);
      wrapped.forEach((wrappedLine, wrappedIndex) => {
        ensureSpace(18);
        if (bullet && wrappedIndex === 0) {
          drawRect(commands, 58, y + 1, 4, 4, [0.05, 0.75, 0.53]);
        }
        drawText(commands, wrappedLine, bullet ? 70 : 54, y, 10, "F1", [0.1, 0.16, 0.24]);
        y -= 15;
      });
      y -= 3;
    });
    y -= 10;
  });
  if (commands.length) pages.push(commands.join("\n"));

  const pageObjectsStart = 3;
  const fontObjectStart = pageObjectsStart + pages.length * 2;
  const pageRefs = pages.map((_, index) => `${pageObjectsStart + index * 2} 0 R`).join(" ");
  const objects = [
    "<< /Type /Catalog /Pages 2 0 R >>",
    `<< /Type /Pages /Kids [${pageRefs}] /Count ${pages.length} >>`,
  ];
  pages.forEach((content, index) => {
    const pageObjectId = pageObjectsStart + index * 2;
    const contentObjectId = pageObjectId + 1;
    objects.push(
      `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 ${fontObjectStart} 0 R /F2 ${fontObjectStart + 1} 0 R /F3 ${fontObjectStart + 2} 0 R >> >> /Contents ${contentObjectId} 0 R >>`,
      `<< /Length ${content.length} >>\nstream\n${content}\nendstream`,
    );
  });
  objects.push(
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>",
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Oblique >>",
  );

  let pdf = "%PDF-1.4\n";
  const offsets = [0];
  objects.forEach((object, index) => {
    offsets.push(pdf.length);
    pdf += `${index + 1} 0 obj\n${object}\nendobj\n`;
  });
  const xref = pdf.length;
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  offsets.slice(1).forEach((offset) => {
    pdf += `${String(offset).padStart(10, "0")} 00000 n \n`;
  });
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF`;
  return pdf;
}

function briefSections(title: string, lines: string[]) {
  const headingSet = new Set([
    "Data bullets",
    "Reddit DD outline",
    "TL;DR",
    "Why compare these",
    "Why this name came up",
    "Evidence bullets",
    "Counterpoints",
    "What would confirm the setup",
    "What would weaken the setup",
    "What to watch next",
    "Bottom line",
    "Disclosure",
  ]);
  const cleaned = lines.map(asPdfText).filter((line, index, source) => {
    if (!line) return true;
    if (line === asPdfText(title) && index < 2) return false;
    return source.findIndex((item) => item === line) === index || headingSet.has(line);
  });
  const sections: { heading: string; lines: string[] }[] = [{ heading: "Walnut Take", lines: [] }];
  cleaned.forEach((line) => {
    if (!line) return;
    if (headingSet.has(line)) {
      sections.push({ heading: line, lines: [] });
      return;
    }
    sections[sections.length - 1].lines.push(line);
  });
  return sections.filter((section) => section.lines.length || section.heading === "Walnut Take");
}

function drawPageBase(commands: string[], title: string, pageNumber: number, first: boolean) {
  drawRect(commands, 0, 0, 612, 792, [0.97, 0.98, 1]);
  drawRect(commands, 0, first ? 626 : 682, 612, first ? 166 : 110, [0.02, 0.06, 0.12]);
  drawRect(commands, 0, first ? 624 : 680, 612, 2, [0.05, 0.75, 0.53]);
  drawText(commands, "WALNUT", 54, first ? 744 : 744, 13, "F2", [0.9, 0.98, 1], 1.6);
  drawText(commands, "Market Terminal Research Brief", 54, first ? 724 : 724, 9, "F1", [0.42, 0.55, 0.67]);
  if (first) {
    drawText(commands, title, 54, 686, 24, "F2", [1, 1, 1]);
    drawText(commands, `Generated ${new Date().toLocaleDateString()}`, 54, 660, 10, "F1", [0.72, 0.8, 0.88]);
    drawRect(commands, 418, 684, 140, 42, [0.04, 0.16, 0.23]);
    drawText(commands, "PRODUCTION DATA", 435, 707, 8, "F2", [0.2, 0.83, 0.63], 1.3);
    drawText(commands, "Not financial advice", 435, 691, 8, "F1", [0.72, 0.8, 0.88]);
  } else {
    drawText(commands, title, 54, 704, 14, "F2", [1, 1, 1]);
  }
  drawText(commands, `Page ${pageNumber}`, 514, 34, 8, "F1", [0.45, 0.52, 0.62]);
}

function drawSummaryCard(commands: string[], summary: string) {
  drawRect(commands, 54, 526, 504, 74, [0.9, 0.98, 0.95]);
  drawRect(commands, 54, 526, 5, 74, [0.05, 0.75, 0.53]);
  drawText(commands, "WALNUT TAKE", 72, 574, 9, "F2", [0.05, 0.45, 0.34], 1.4);
  wrapPdfLine(asPdfText(summary), 88).slice(0, 3).forEach((line, index) => {
    drawText(commands, line, 72, 554 - index * 15, 10, "F1", [0.1, 0.16, 0.24]);
  });
}

function drawText(commands: string[], value: string, x: number, y: number, size: number, font: "F1" | "F2" | "F3", color: [number, number, number], spacing = 0) {
  const spacingOp = spacing ? `${spacing} Tc ` : "";
  commands.push(`BT ${color.join(" ")} rg /${font} ${size} Tf ${spacingOp}${x} ${y} Td (${pdfEscape(asPdfText(value))}) Tj ET`);
}

function drawRect(commands: string[], x: number, y: number, width: number, height: number, color: [number, number, number]) {
  commands.push(`${color.join(" ")} rg ${x} ${y} ${width} ${height} re f`);
}

function asPdfText(value: string) {
  return value.replace(/[^\x20-\x7E]/g, "-").replace(/\s+/g, " ").trim();
}

function wrapPdfLine(value: string, width: number) {
  if (!value) return [""];
  const words = value.split(" ");
  const lines: string[] = [];
  let current = "";
  words.forEach((word) => {
    const next = current ? `${current} ${word}` : word;
    if (next.length > width && current) {
      lines.push(current);
      current = word;
    } else {
      current = next;
    }
  });
  if (current) lines.push(current);
  return lines;
}

function pdfEscape(value: string) {
  return value.replace(/\\/g, "\\\\").replace(/\(/g, "\\(").replace(/\)/g, "\\)");
}

function escapeXml(value: string) {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
