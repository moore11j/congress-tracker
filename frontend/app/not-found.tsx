import Link from "next/link";

export default function NotFound() {
  return (
    <main className="min-h-screen bg-[#050b14] px-6 py-20 text-slate-100">
      <div className="mx-auto max-w-3xl">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">Walnut Markets</p>
        <h1 className="mt-4 text-4xl font-semibold">Page not found</h1>
        <p className="mt-4 max-w-xl text-sm leading-6 text-slate-400">
          The page may have moved, or the link may be incomplete.
        </p>
        <Link
          href="/"
          prefetch={false}
          className="mt-8 inline-flex h-10 items-center justify-center rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20"
        >
          Back to Walnut
        </Link>
      </div>
    </main>
  );
}
