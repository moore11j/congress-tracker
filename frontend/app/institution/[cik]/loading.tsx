import { cardClassName } from "@/lib/styles";

export default function InstitutionLoading() {
  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <section className={`${cardClassName} min-w-0 space-y-5`}>
        <div className="h-4 w-44 rounded bg-slate-800" />
        <div className="h-10 w-full max-w-xl rounded bg-slate-800" />
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-6">
          {Array.from({ length: 6 }).map((_, index) => (
            <div key={index} className="h-24 rounded-2xl border border-white/10 bg-slate-950/45" />
          ))}
        </div>
      </section>
      <section className={`${cardClassName} min-w-0`}>
        <div className="h-6 w-64 rounded bg-slate-800" />
        <div className="mt-4 space-y-3">
          {Array.from({ length: 4 }).map((_, index) => (
            <div key={index} className="h-20 rounded-2xl border border-white/10 bg-slate-950/45" />
          ))}
        </div>
      </section>
    </div>
  );
}
