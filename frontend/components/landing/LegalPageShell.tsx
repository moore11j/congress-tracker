import type { ReactNode } from "react";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnut-intel.com").replace(/\/+$/, "");

type LegalPageShellProps = {
  eyebrow: string;
  title: string;
  description: string;
  lastUpdated: string;
  children: ReactNode;
};

export function LegalPageShell({ eyebrow, title, description, lastUpdated, children }: LegalPageShellProps) {
  return (
    <main className="min-h-screen bg-[#030712] text-slate-100">
      <header className="border-b border-white/10 bg-slate-950/88">
        <div className="mx-auto flex max-w-5xl items-center justify-between gap-4 px-4 py-4 sm:px-6 lg:px-8">
          <a href="/" className="flex min-w-0 items-center gap-3" aria-label="Walnut home">
            <WalnutBrandMark
              className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-emerald-300/35 bg-slate-950 shadow-[0_0_28px_rgba(16,185,129,0.18)]"
              svgClassName="h-6 w-6 overflow-visible"
            />
            <span className="leading-none">
              <span className="block whitespace-nowrap text-base font-semibold text-white">Walnut</span>
              <span className="mt-1 block whitespace-nowrap text-[11px] font-medium text-slate-400">Market Terminal</span>
            </span>
          </a>
          <a href={appUrl} className="rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
            Launch Terminal
          </a>
        </div>
      </header>

      <div className="mx-auto max-w-5xl px-4 py-14 sm:px-6 lg:px-8">
        <div className="border-b border-white/10 pb-8">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{eyebrow}</p>
          <h1 className="mt-4 text-4xl font-semibold text-white">{title}</h1>
          <p className="mt-4 max-w-3xl text-base leading-7 text-slate-300">{description}</p>
          <p className="mt-4 text-sm text-slate-500">Last updated: {lastUpdated}</p>
        </div>

        <div className="legal-content mt-10 space-y-8 text-sm leading-7 text-slate-300">{children}</div>
      </div>
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
