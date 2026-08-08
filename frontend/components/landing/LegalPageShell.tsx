import type { ReactNode } from "react";
import { MarketingHeader } from "@/components/landing/MarketingHeader";
import { WALNUT_REDDIT_URL, WALNUT_X_HANDLE, WALNUT_X_URL } from "@/lib/marketingMetadata";

type LegalPageShellProps = {
  eyebrow: string;
  title: string;
  description: string;
  lastUpdated: string;
  chrome?: "public" | "embedded";
  children: ReactNode;
};

export function LegalPageShell({ eyebrow, title, description, lastUpdated, chrome = "public", children }: LegalPageShellProps) {
  const content = (
    <div className={chrome === "embedded" ? "mx-auto max-w-5xl px-0 py-0" : "mx-auto max-w-5xl px-4 py-14 sm:px-6 lg:px-8"}>
      <div className="border-b border-white/10 pb-8">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{eyebrow}</p>
        <h1 className="mt-4 text-4xl font-semibold text-white">{title}</h1>
        <p className="mt-4 max-w-3xl text-base leading-7 text-slate-300">{description}</p>
        <p className="mt-4 text-sm text-slate-400">Last updated: {lastUpdated}</p>
      </div>

      <div className="legal-content mt-10 space-y-8 text-sm leading-7 text-slate-300">{children}</div>
    </div>
  );

  if (chrome === "embedded") {
    return <section className="text-slate-100">{content}</section>;
  }

  return (
    <main className="min-h-screen bg-[#030712] text-slate-100">
      <MarketingHeader />

      {content}

      <footer className="border-t border-white/10 px-4 py-8 sm:px-6 lg:px-8">
        <div className="mx-auto flex max-w-5xl flex-col gap-4 text-sm text-slate-400 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="font-semibold text-slate-300">Walnut Market Terminal</p>
            <p className="mt-1 text-xs leading-5 text-slate-400">
              Walnut Market Terminal is operated by Walnut Intelligence Inc.
            </p>
            <p className="mt-1 text-xs leading-5 text-slate-400">
              Walnut is a stock research and analysis platform operated by Walnut Intelligence Inc. It is provided for research and informational purposes only and does not provide investment advice.
            </p>
          </div>
          <nav className="flex flex-wrap gap-4" aria-label="Legal footer">
            <a href="/about" className="hover:text-white">
              About
            </a>
            <a href="/faq" className="hover:text-white">
              FAQ
            </a>
            <a href="/terms" className="hover:text-white">
              Terms
            </a>
            <a href="/privacy" className="hover:text-white">
              Privacy
            </a>
            <a href="/contact" className="hover:text-white">
              Contact / support@walnutmarkets.com
            </a>
            <a href={WALNUT_X_URL} target="_blank" rel="noreferrer" className="hover:text-white">
              X / {WALNUT_X_HANDLE}
            </a>
            <a href={WALNUT_REDDIT_URL} target="_blank" rel="noreferrer" className="hover:text-white">
              Reddit / r/walnutmarkets
            </a>
          </nav>
        </div>
      </footer>
    </main>
  );
}

export function LegalSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
      <h2 className="text-lg font-semibold text-white">{title}</h2>
      <div className="mt-3 space-y-3">{children}</div>
    </section>
  );
}
