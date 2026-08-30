import type { ReactNode } from "react";

type LegalPageShellProps = {
  eyebrow: string;
  title: string;
  description: string;
  lastUpdated: string;
  children: ReactNode;
};

export function LegalPageShell({ eyebrow, title, description, lastUpdated, children }: LegalPageShellProps) {
  const content = (
    <div className="w-full min-w-0 px-0 py-0">
      <div className="border-b border-white/10 pb-8">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{eyebrow}</p>
        <h1 className="mt-4 text-4xl font-semibold text-white">{title}</h1>
        <p className="mt-4 max-w-3xl text-base leading-7 text-slate-300">{description}</p>
        <p className="mt-4 text-sm text-slate-400">Last updated: {lastUpdated}</p>
      </div>

      <div className="legal-content mt-10 space-y-8 text-sm leading-7 text-slate-300">{children}</div>
    </div>
  );

  return (
    <section className="text-slate-100">
      {content}
    </section>
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
