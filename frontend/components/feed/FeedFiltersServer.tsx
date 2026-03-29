import { FeedFilterAutoSubmit } from "@/components/feed/FeedFilterAutoSubmit";
import { FeedMinAmountInputEnhancer } from "@/components/feed/FeedMinAmountInputEnhancer";
import { FeedSymbolAutosuggestEnhancer } from "@/components/feed/FeedSymbolAutosuggestEnhancer";
import { cardClassName, ghostButtonClassName, inputClassName, selectClassName } from "@/lib/styles";

type FeedMode = "congress" | "insider" | "all";

type FeedFiltersServerProps = {
  mode: FeedMode;
  params: {
    symbol?: string;
    min_amount?: string;
    recent_days?: string;
    member?: string;
    chamber?: string;
    party?: string;
    trade_type?: string;
    role?: string;
  };
};

function modeHref(nextMode: FeedMode, params: FeedFiltersServerProps["params"]) {
  const url = new URLSearchParams();
  url.set("mode", nextMode);
  const keys = ["symbol", "min_amount", "recent_days", "member", "chamber", "party", "trade_type", "role"] as const;
  for (const key of keys) {
    const value = params[key]?.trim();
    if (value) url.set(key, value);
  }
  return `/?${url.toString()}`;
}

function formatMinAmountDisplay(value?: string): string {
  const digits = (value ?? "").replace(/[^\d]/g, "");
  if (!digits) return "";
  return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export function FeedFiltersServer({ mode, params }: FeedFiltersServerProps) {
  return (
    <section className={cardClassName}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Feed mode & filters</h2>
          <p className="text-sm text-slate-400">Filter the live feed by symbol, member, trade type, and more.</p>
        </div>
        <a href={`/?mode=${mode}`} className={ghostButtonClassName}>
          Reset
        </a>
      </div>

      <div className="mt-4 flex flex-wrap gap-1">
        {([
          ["all", "All"],
          ["congress", "Congress"],
          ["insider", "Insider"],
        ] as const).map(([value, label]) => {
          const active = mode === value;
          return (
            <a
              key={value}
              href={modeHref(value, params)}
              className={`rounded-full border px-3 py-1 text-xs font-semibold transition ${
                active
                  ? "border-emerald-300/60 bg-emerald-500/20 text-emerald-100"
                  : "border-white/15 bg-white/[0.03] text-slate-300 hover:bg-white/[0.06]"
              }`}
            >
              {label}
            </a>
          );
        })}
      </div>

      <form id="feed-filters-form" method="GET" action="/" className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <input type="hidden" name="mode" value={mode} />

        <div className="relative">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
          <input id="feed-filter-symbol" name="symbol" defaultValue={params.symbol ?? ""} className={inputClassName} placeholder="NVDA" autoComplete="off" />
          <FeedSymbolAutosuggestEnhancer formId="feed-filters-form" inputName="symbol" mode={mode} />
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Min amount</label>
          <input
            name="min_amount"
            inputMode="numeric"
            defaultValue={formatMinAmountDisplay(params.min_amount)}
            className={inputClassName}
            placeholder="250,000"
          />
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent days</label>
          <select name="recent_days" defaultValue={params.recent_days ?? ""} className={selectClassName}>
            <option value="">Anytime</option>
            <option value="1">1 day</option>
            <option value="7">7 days</option>
            <option value="30">30 days</option>
            <option value="90">90 days</option>
          </select>
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Trade Type</label>
          <select name="trade_type" defaultValue={params.trade_type ?? ""} className={selectClassName}>
            <option value="">All types</option>
            <option value="purchase">Purchase</option>
            <option value="sale">Sale</option>
          </select>
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Member</label>
          <input name="member" defaultValue={params.member ?? ""} className={inputClassName} placeholder="Pelosi" />
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Chamber</label>
          <select name="chamber" defaultValue={params.chamber ?? ""} className={selectClassName}>
            <option value="">All chambers</option>
            <option value="house">House</option>
            <option value="senate">Senate</option>
          </select>
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Party</label>
          <select name="party" defaultValue={params.party ?? ""} className={selectClassName}>
            <option value="">All parties</option>
            <option value="democrat">Democrat</option>
            <option value="republican">Republican</option>
            <option value="independent">Independent</option>
          </select>
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Role</label>
          <input name="role" defaultValue={params.role ?? ""} className={inputClassName} placeholder="CEO" />
        </div>

        <div className="md:col-span-2 xl:col-span-4">
          <button
            type="submit"
            className="inline-flex h-10 items-center justify-center rounded-2xl border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20"
          >
            Apply filters
          </button>
        </div>
      </form>
      <FeedFilterAutoSubmit formId="feed-filters-form" />
      <FeedMinAmountInputEnhancer formId="feed-filters-form" inputName="min_amount" />
    </section>
  );
}
