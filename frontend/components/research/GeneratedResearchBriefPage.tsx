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
                {paragraphs(section.body_markdown).map((part) => (
                  <p key={part.slice(0, 80)}>{part}</p>
                ))}
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
