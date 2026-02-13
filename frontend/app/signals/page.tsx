import { SignalsClient } from "./signals-client";

export default function SignalsPage() {
  const apiBase = process.env.NEXT_PUBLIC_API_BASE ?? "https://congress-tracker-api.fly.dev";

  return (
    <div className="space-y-8">
      <div>
        <div className="text-xs tracking-[0.25em] text-emerald-300/70">SIGNALS</div>
        <h1 className="mt-2 text-3xl font-semibold text-white">Unusual trade radar</h1>
        <p className="mt-2 max-w-2xl text-sm text-slate-300/80">
          Presets for quick scanning with server-side ranking controls.
        </p>
      </div>

      <SignalsClient apiBase={apiBase} />
    </div>
  );
}
