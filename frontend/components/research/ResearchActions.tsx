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

export function ResearchActions({ subject }: { subject: ResearchSubject }) {
  const [open, setOpen] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const outputs = useMemo(() => buildResearchOutputs(subject), [subject]);

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
    <div className="relative">
      <button type="button" onClick={() => setOpen((value) => !value)} className={ghostButtonClassName}>
        Create Research
      </button>
      {open ? (
        <div className="absolute right-0 z-30 mt-2 w-72 rounded-lg border border-white/10 bg-slate-950 p-2 text-sm shadow-2xl shadow-black/40">
          <ResearchActionButton label="Copy Walnut Take" onClick={() => run(() => copyText(outputs.take, "Walnut Take copied"))} />
          <ResearchActionButton label="Copy Data Bullets" onClick={() => run(() => copyText(outputs.bullets.join("\n"), "Data bullets copied"))} />
          {outputs.comparisonConclusion ? (
            <ResearchActionButton label="Copy Comparison Conclusion" onClick={() => run(() => copyText(outputs.comparisonConclusion ?? "", "Comparison copied"))} />
          ) : null}
          <ResearchActionButton label="Create X Card" onClick={() => run(() => downloadText(outputs.xCardSvg, outputs.slug + "-x-card.svg", "image/svg+xml", "X card downloaded"))} />
          <ResearchActionButton label="Create Reddit DD Outline" onClick={() => run(() => copyText(outputs.redditOutline, "Reddit outline copied"))} />
          <ResearchActionButton label="Export Research Brief" onClick={() => run(() => downloadPdf(outputs.title, outputs.briefLines, outputs.slug + "-research-brief.pdf"))} />
          <ResearchActionButton label="Share Research URL" onClick={() => run(() => shareResearchUrl())} />
          {status ? <p className="px-2 py-1.5 text-xs font-medium text-emerald-200">{status}</p> : null}
        </div>
      ) : null}
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
      metric: score === null ? label : `${score}/100`,
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
      metric: winner === "Even" ? "Even" : `${winner} leads`,
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
  accent,
  footer,
}: {
  eyebrow: string;
  title: string;
  subtitle: string;
  metric: string;
  accent: string;
  footer: string;
}) {
  const safeTitle = escapeXml(title).slice(0, 42);
  const safeSubtitle = escapeXml(subtitle).slice(0, 130);
  const safeMetric = escapeXml(metric).slice(0, 28);
  return `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="675" viewBox="0 0 1200 675">
  <rect width="1200" height="675" fill="#06111f"/>
  <rect x="42" y="42" width="1116" height="591" rx="26" fill="#0b1726" stroke="#1f3347" stroke-width="2"/>
  <circle cx="1052" cy="146" r="92" fill="${accent}" opacity="0.16"/>
  <text x="82" y="122" fill="#34d399" font-family="Inter, Arial, sans-serif" font-size="28" font-weight="700" letter-spacing="4">${escapeXml(eyebrow)}</text>
  <text x="82" y="254" fill="#f8fafc" font-family="Inter, Arial, sans-serif" font-size="92" font-weight="800">${safeTitle}</text>
  <text x="82" y="342" fill="#cbd5e1" font-family="Inter, Arial, sans-serif" font-size="32">${safeSubtitle}</text>
  <rect x="82" y="420" width="360" height="92" rx="18" fill="${accent}" opacity="0.18" stroke="${accent}" stroke-width="2"/>
  <text x="112" y="480" fill="${accent}" font-family="Inter, Arial, sans-serif" font-size="42" font-weight="800">${safeMetric}</text>
  <text x="82" y="584" fill="#64748b" font-family="Inter, Arial, sans-serif" font-size="26">${escapeXml(footer)}</text>
</svg>`;
}

function buildSimplePdf(title: string, lines: string[]) {
  const wrapped = [title, "", ...lines].flatMap((line) => wrapPdfLine(asPdfText(line), 96)).slice(0, 54);
  const textOps = wrapped.map((line, index) => `${index === 0 ? "BT /F1 18 Tf 72 760 Td" : "0 -14 Td"} (${pdfEscape(line)}) Tj`).join("\n") + "\nET";
  const content = textOps;
  const objects = [
    "<< /Type /Catalog /Pages 2 0 R >>",
    "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    `<< /Length ${content.length} >>\nstream\n${content}\nendstream`,
  ];
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
