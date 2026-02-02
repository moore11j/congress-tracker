"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { selectClassName } from "@/lib/styles";

const presets = ["discovery", "balanced", "strict"] as const;
const limits = [25, 50, 100] as const;

type Props = {
  preset: (typeof presets)[number];
  limit: (typeof limits)[number];
  debug: boolean;
};

function buildHref(pathname: string, params: URLSearchParams) {
  const query = params.toString();
  return query ? `${pathname}?${query}` : pathname;
}

export function SignalsControls({ preset, limit, debug }: Props) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const updateParams = (updates: Record<string, string | null>) => {
    const params = new URLSearchParams(searchParams.toString());
    Object.entries(updates).forEach(([key, value]) => {
      if (!value) {
        params.delete(key);
      } else {
        params.set(key, value);
      }
    });
    router.push(buildHref(pathname, params));
  };

  return (
    <div className="flex flex-wrap items-center gap-4 rounded-3xl border border-white/10 bg-slate-900/70 p-4 text-xs text-slate-300">
      <div className="flex flex-col gap-2">
        <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Preset</span>
        <div className="flex rounded-full border border-white/10 bg-black/30 p-1">
          {presets.map((option) => {
            const isActive = option === preset;
            return (
              <button
                key={option}
                type="button"
                onClick={() => updateParams({ preset: option })}
                className={`px-3 py-1 text-[11px] font-semibold uppercase tracking-wide transition ${
                  isActive
                    ? "rounded-full border border-emerald-400/40 bg-emerald-400/20 text-emerald-100"
                    : "rounded-full border border-transparent text-slate-300 hover:border-white/20 hover:text-white"
                }`}
              >
                {option}
              </button>
            );
          })}
        </div>
      </div>

      <div className="flex flex-col gap-2">
        <label htmlFor="signals-limit" className="text-[11px] uppercase tracking-[0.2em] text-slate-400">
          Limit
        </label>
        <select
          id="signals-limit"
          value={String(limit)}
          onChange={(event) => updateParams({ limit: event.target.value })}
          className={`min-w-[120px] ${selectClassName}`}
        >
          {limits.map((value) => (
            <option key={value} value={value}>
              {value}
            </option>
          ))}
        </select>
      </div>

      <label className="flex items-center gap-3 rounded-full border border-white/10 bg-black/30 px-4 py-2 text-[11px] font-semibold uppercase tracking-wide text-slate-300">
        <input
          type="checkbox"
          checked={debug}
          onChange={(event) => updateParams({ debug: event.target.checked ? "true" : null })}
          className="h-4 w-4 rounded border-white/20 bg-slate-900 text-emerald-400 focus:ring-emerald-400/40"
        />
        Debug
      </label>
    </div>
  );
}
