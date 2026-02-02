import { cardClassName } from "@/lib/styles";

export default function SignalsLoading() {
  return (
    <div className="space-y-8">
      <section className="space-y-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Unusual signals</p>
          <h1 className="text-3xl font-semibold text-white">Unusual trade radar.</h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            Scan anomalous congressional trades against historical baselines. Tuned for quick, terminal-like triage.
          </p>
        </div>
        <div className="h-16 rounded-3xl border border-white/10 bg-slate-900/70" />
      </section>

      <section className={cardClassName}>
        <div className="animate-pulse space-y-4">
          <div className="h-4 w-40 rounded-full bg-white/10" />
          <div className="overflow-hidden rounded-2xl border border-white/10">
            <div className="grid grid-cols-9 gap-2 bg-white/5 px-4 py-3 text-[11px] uppercase tracking-[0.2em] text-slate-500">
              {Array.from({ length: 9 }).map((_, index) => (
                <div key={`head-${index}`} className="h-3 rounded bg-white/10" />
              ))}
            </div>
            <div className="divide-y divide-white/5">
              {Array.from({ length: 6 }).map((_, row) => (
                <div key={`row-${row}`} className="grid grid-cols-9 gap-2 px-4 py-3">
                  {Array.from({ length: 9 }).map((_, cell) => (
                    <div key={`cell-${row}-${cell}`} className="h-3 rounded bg-white/10" />
                  ))}
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
