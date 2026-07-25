"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { getGeneratedResearchBrief, type AdminResearchBriefDraft } from "@/lib/api";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";

function paragraphs(markdown: string) {
  return markdown
    .split(/\n{2,}/)
    .map((part) => part.trim())
    .filter(Boolean);
}

type MarkdownBlock =
  | { type: "paragraph"; key: string; text: string }
  | { type: "table"; key: string; header: string[]; rows: string[][] };

function markdownBlocks(markdown: string): MarkdownBlock[] {
  return paragraphs(markdown).map((part, index) => parsePipeTable(part, index) ?? { type: "paragraph", key: `paragraph-${index}`, text: part });
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
  const cells = line.split("|").map((cell) => cell.trim());
  if (cells[0] === "") cells.shift();
  if (cells[cells.length - 1] === "") cells.pop();
  return cells;
}

function isMarkdownDivider(cells: string[]) {
  return cells.length > 0 && cells.every((cell) => /^:?-{3,}:?$/.test(cell.replace(/\s/g, "")));
}

export function GeneratedResearchBriefPage({ slug }: { slug: string }) {
  const [draft, setDraft] = useState<AdminResearchBriefDraft | null>(null);
  const [status, setStatus] = useState<"loading" | "ready" | "missing" | "error">("loading");

  useEffect(() => {
    let alive = true;
    getGeneratedResearchBrief(slug)
      .then((payload) => {
        if (!alive) return;
        setDraft(payload);
        setStatus("ready");
      })
      .catch((error) => {
        if (!alive) return;
        setStatus(error instanceof Error && error.message.toLowerCase().includes("not found") ? "missing" : "error");
      });
    return () => {
      alive = false;
    };
  }, [slug]);

  if (status === "loading") {
    return (
      <main className="min-h-screen bg-slate-950 px-4 py-12 text-slate-100">
        <div className="mx-auto max-w-4xl rounded-lg border border-white/10 bg-slate-950/60 p-6">Loading research brief...</div>
      </main>
    );
  }

  if (status !== "ready" || !draft) {
    return (
      <main className="min-h-screen bg-slate-950 px-4 py-12 text-slate-100">
        <div className="mx-auto max-w-4xl rounded-lg border border-white/10 bg-slate-950/60 p-6">
          <h1 className="text-2xl font-semibold text-white">Research brief unavailable</h1>
          <p className="mt-2 text-sm text-slate-400">This brief is not published or could not be loaded.</p>
          <Link href="/insights" className="mt-5 inline-flex rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-100">
            Back to Insights
          </Link>
        </div>
      </main>
    );
  }

  const article = draft.article;
  const tickerHref = `/ticker/${encodeURIComponent(article.primary_ticker || draft.primary_ticker)}`;
  const signupHref = `/login?mode=register&return_to=${encodeURIComponent(tickerHref)}`;

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <section className="border-b border-white/10 bg-[radial-gradient(circle_at_20%_0%,rgba(16,185,129,0.18),transparent_28%),linear-gradient(180deg,rgba(2,6,23,0.96),rgba(2,6,23,1))]">
        <div className="mx-auto max-w-5xl px-4 py-8 sm:px-6 lg:px-8">
          <Link href="/insights" className="inline-flex items-center gap-2 text-sm font-semibold text-emerald-200">
            <WalnutBrandMark className="h-6 w-6" />
            Walnut Research
          </Link>
          <div className="mt-10 max-w-3xl">
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{article.category || "Research Brief"}</p>
            <h1 className="mt-3 text-4xl font-semibold leading-tight text-white sm:text-5xl">{article.title}</h1>
            <p className="mt-5 text-lg leading-8 text-slate-300">{article.subtitle || article.summary}</p>
            <div className="mt-7 flex flex-wrap gap-3">
              <Link href={signupHref} className="inline-flex min-h-11 items-center justify-center rounded-lg bg-emerald-300 px-5 py-2.5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
                Create a free account
              </Link>
              <Link href={tickerHref} className="inline-flex min-h-11 items-center justify-center rounded-lg border border-white/15 px-5 py-2.5 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/50 hover:text-emerald-100">
                Open {article.primary_ticker || draft.primary_ticker} terminal
              </Link>
            </div>
            <p className="mt-4 text-xs leading-5 text-slate-500">Research only. Not investment advice. No buy or sell recommendation.</p>
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-5xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[minmax(0,1fr)_18rem] lg:px-8">
        <article className="min-w-0 space-y-8">
          {article.sections.map((section) => (
            <section key={section.key} className="rounded-lg border border-white/10 bg-slate-950/50 p-5">
              <h2 className="text-2xl font-semibold text-white">{section.heading}</h2>
              <div className="mt-4 space-y-4 text-sm leading-7 text-slate-300">
                {markdownBlocks(section.body_markdown).map((block) =>
                  block.type === "table" ? <ResearchDataTable key={block.key} header={block.header} rows={block.rows} /> : <p key={block.key}>{block.text}</p>,
                )}
              </div>
            </section>
          ))}
        </article>

        <aside className="space-y-4">
          <div className="rounded-lg border border-white/10 bg-slate-950/60 p-4">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Walnut Judgment</p>
            <p className="mt-2 text-lg font-semibold capitalize text-white">{article.judgment}</p>
            <p className="mt-2 text-sm leading-6 text-slate-400">{article.summary}</p>
          </div>
          <SideList title="Catalysts" items={article.catalysts} />
          <SideList title="Risks" items={article.risks} />
          <SideList title="What to watch" items={article.watch_items} />
        </aside>
      </section>
    </main>
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
            <tr key={`${rowIndex}-${row.join("|")}`} className={rowIndex % 2 === 0 ? "bg-slate-900/55" : "bg-slate-800/35"}>
              {header.map((_, cellIndex) => (
                <td key={`${rowIndex}-${cellIndex}`} className="border-t border-white/10 px-3 py-3 align-top text-slate-300">
                  {row[cellIndex] || ""}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SideList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/60 p-4">
      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">{title}</p>
      <ul className="mt-3 space-y-2 text-sm leading-6 text-slate-300">
        {(items || []).slice(0, 5).map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </div>
  );
}
