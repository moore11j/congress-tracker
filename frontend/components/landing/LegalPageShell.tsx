import type { ReactNode } from "react";

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnut-intel.com").replace(/\/+$/, "");

type LegalPageShellProps = {
  eyebrow: string;
  title: string;
  description: string;
  lastUpdated: string;
  children: ReactNode;
};

function WalnutMark() {
  return (
    <span className="flex h-9 w-9 items-center justify-center rounded-lg border border-emerald-300/35 bg-slate-950 shadow-[0_0_28px_rgba(16,185,129,0.18)]">
      <svg viewBox="0 0 48 48" aria-hidden="true" className="h-6 w-6">
        <path
          d="M24 7c-4.5 0-7.8 3.2-8.1 7.5-4.2.5-7.3 3.9-7.3 8.1 0 1.6.4 3 1.2 4.3-2 1.6-3.1 3.9-3.1 6.5 0 4.7 3.8 8.6 8.5 8.6 2.6 0 4.8-1.1 6.4-2.9.7.2 1.5.3 2.4.3s1.7-.1 2.4-.3c1.6 1.8 3.8 2.9 6.4 2.9 4.7 0 8.5-3.9 8.5-8.6 0-2.6-1.1-4.9-3.1-6.5.8-1.3 1.2-2.7 1.2-4.3 0-4.2-3.1-7.6-7.3-8.1C31.8 10.2 28.5 7 24 7Z"
          fill="#020617"
          stroke="#34d399"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="3"
        />
        <path
          d="M24 8.5v30M16 16c3.2 2.4 5.4 5.5 6.4 9M32 16c-3.2 2.4-5.4 5.5-6.4 9M10.5 27c4.1 1.5 7.1 3.9 9.1 7.4M37.5 27c-4.1 1.5-7.1 3.9-9.1 7.4"
          fill="none"
          stroke="#ccfbf1"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="2.4"
        />
      </svg>
    </span>
  );
}

export function LegalPageShell({ eyebrow, title, description, lastUpdated, children }: LegalPageShellProps) {
  return (
    <main className="min-h-screen bg-[#030712] text-slate-100">
      <header className="border-b border-white/10 bg-slate-950/88">
        <div className="mx-auto flex max-w-5xl items-center justify-between gap-4 px-4 py-4 sm:px-6 lg:px-8">
          <a href="/" className="flex min-w-0 items-center gap-3" aria-label="Walnut home">
            <WalnutMark />
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
