import { FeedFilterAutoSubmit } from "@/components/feed/FeedFilterAutoSubmit";
import { FeedMemberAutosuggestEnhancer } from "@/components/feed/FeedMemberAutosuggestEnhancer";
import { FeedMinAmountInputEnhancer } from "@/components/feed/FeedMinAmountInputEnhancer";
import { FeedRoleAutosuggestEnhancer } from "@/components/feed/FeedRoleAutosuggestEnhancer";
import { FeedSymbolAutosuggestEnhancer } from "@/components/feed/FeedSymbolAutosuggestEnhancer";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { cardClassName, ghostButtonClassName, inputClassName, selectClassName } from "@/lib/styles";

type FeedMode = "congress" | "insider" | "government_contracts" | "all";

type FeedFiltersServerProps = {
  mode: FeedMode;
  params: {
    symbol?: string;
    min_amount?: string;
    max_amount?: string;
    recent_days?: string;
    member?: string;
    chamber?: string;
    party?: string;
    asset_class?: string;
    trade_type?: string;
    role?: string;
    department?: string;
    filed_after_max?: string;
    pnl_min?: string;
    pnl_max?: string;
    signal_min?: string;
  };
};

const departmentOptions = [
  ["", "Any"],
  ["Department of Defense", "Department of Defense"],
  ["Department of Health and Human Services", "Department of Health and Human Services"],
  ["Department of Agriculture", "Department of Agriculture"],
  ["Department of Energy", "Department of Energy"],
  ["Department of Homeland Security", "Department of Homeland Security"],
  ["Department of Veterans Affairs", "Department of Veterans Affairs"],
  ["National Aeronautics and Space Administration", "National Aeronautics and Space Administration"],
  ["General Services Administration", "General Services Administration"],
  ["Department of Transportation", "Department of Transportation"],
  ["Department of Justice", "Department of Justice"],
  ["Other", "Other"],
] as const;

function modeHref(nextMode: FeedMode, params: FeedFiltersServerProps["params"]) {
  const url = new URLSearchParams();
  url.set("mode", nextMode);
  const keys =
    nextMode === "government_contracts"
      ? (["symbol", "min_amount", "max_amount", "recent_days", "department"] as const)
      : ([
          "symbol",
          "min_amount",
          "max_amount",
          "recent_days",
          "member",
          "chamber",
          "party",
          "asset_class",
          "trade_type",
          "role",
          "department",
          "filed_after_max",
          "pnl_min",
          "pnl_max",
          "signal_min",
        ] as const);
  for (const key of keys) {
    const value = params[key]?.trim();
    if (value) url.set(key, value);
  }
  return `/?${url.toString()}`;
}

function formatAmountDisplay(value?: string): string {
  const digits = (value ?? "").replace(/[^\d]/g, "");
  if (!digits) return "";
  return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export function FeedFiltersServer({ mode, params }: FeedFiltersServerProps) {
  const formKey = JSON.stringify({ mode, ...params });

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

      <SavedViewsBar
        surface="feed"
        restoreOnLoad={true}
        defaultParams={{ mode }}
        paramKeys={[
          "mode",
          "symbol",
          "min_amount",
          "max_amount",
          "recent_days",
          "member",
          "chamber",
          "party",
          "asset_class",
          "trade_type",
          "role",
          "department",
          "filed_after_max",
          "pnl_min",
          "pnl_max",
          "signal_min",
          "ownership",
          "whale",
          "limit",
          "page_size",
        ]}
      />

      <div className="mt-4 flex flex-wrap gap-1">
        {([
          ["all", "All"],
          ["congress", "Congress"],
          ["insider", "Insider"],
          ["government_contracts", "Government Contracts"],
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

      <form key={formKey} id="feed-filters-form" method="GET" action="/" className="mt-4 grid gap-2.5 md:grid-cols-2 lg:grid-cols-5">
        <input type="hidden" name="mode" value={mode} />

        <div className="relative min-w-0 lg:col-start-1 lg:row-start-1">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
          <input id="feed-filter-symbol" name="symbol" defaultValue={params.symbol ?? ""} className={inputClassName} placeholder="NVDA" autoComplete="off" />
          <FeedSymbolAutosuggestEnhancer formId="feed-filters-form" inputName="symbol" mode={mode} />
        </div>

        <div className="min-w-0 lg:col-start-2 lg:row-start-1">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Min amount</label>
          <input
            name="min_amount"
            inputMode="numeric"
            defaultValue={formatAmountDisplay(params.min_amount)}
            className={inputClassName}
            placeholder="250,000"
          />
        </div>

        <div className="min-w-0 lg:col-start-2 lg:row-start-2">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Max amount</label>
          <input
            name="max_amount"
            inputMode="numeric"
            defaultValue={formatAmountDisplay(params.max_amount)}
            className={inputClassName}
            placeholder="5,000,000"
          />
        </div>

        <div className="min-w-0 lg:col-start-3 lg:row-start-1">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent days</label>
          <select name="recent_days" defaultValue={params.recent_days ?? ""} className={selectClassName}>
            <option value="">Anytime</option>
            <option value="1">1 day</option>
            <option value="7">7 days</option>
            <option value="30">30 days</option>
            <option value="90">90 days</option>
          </select>
        </div>

        <div className="min-w-0 lg:col-start-4 lg:row-start-1">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Asset Type</label>
          <select name="asset_class" defaultValue={params.asset_class ?? ""} className={selectClassName}>
            <option value="">All assets</option>
            <option value="equity">Public Equities</option>
            <option value="etf_fund">ETF/Fund</option>
            <option value="treasury">Treasuries</option>
            <option value="crypto">Crypto</option>
            <option value="other">Other</option>
          </select>
        </div>

        <div className="min-w-0 lg:col-start-4 lg:row-start-2">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Trade Type</label>
          <select name="trade_type" defaultValue={params.trade_type ?? ""} className={selectClassName}>
            <option value="">All types</option>
            <option value="purchase">Purchase</option>
            <option value="sale">Sale</option>
          </select>
        </div>

        <div className="relative min-w-0 lg:col-start-5 lg:row-start-1">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Member / Insider</label>
          <input id="feed-filter-member" name="member" defaultValue={params.member ?? ""} className={inputClassName} placeholder="Pelosi" autoComplete="off" />
          <FeedMemberAutosuggestEnhancer formId="feed-filters-form" inputName="member" />
        </div>

        <div className="min-w-0 lg:col-start-1 lg:row-start-2">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Chamber</label>
          <select name="chamber" defaultValue={params.chamber ?? ""} className={selectClassName}>
            <option value="">All chambers</option>
            <option value="house">House</option>
            <option value="senate">Senate</option>
          </select>
        </div>

        <div className="min-w-0 lg:col-start-3 lg:row-start-2">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Party</label>
          <select name="party" defaultValue={params.party ?? ""} className={selectClassName}>
            <option value="">All parties</option>
            <option value="democrat">Democrat</option>
            <option value="republican">Republican</option>
            <option value="independent">Independent</option>
          </select>
        </div>

        <div className="relative min-w-0 lg:col-start-1 lg:row-start-3">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Role</label>
          <input id="feed-filter-role" name="role" defaultValue={params.role ?? ""} className={inputClassName} placeholder="CEO" autoComplete="off" />
          <FeedRoleAutosuggestEnhancer formId="feed-filters-form" inputName="role" />
        </div>

        <div className="min-w-0 lg:col-start-5 lg:row-start-2">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Department</label>
          <select name="department" defaultValue={params.department ?? ""} className={selectClassName}>
            {departmentOptions.map(([value, label]) => (
              <option key={value || "any"} value={value}>
                {label}
              </option>
            ))}
          </select>
        </div>

        <div className="min-w-0 lg:col-start-2 lg:row-start-3">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Filed After Max</label>
          <input
            name="filed_after_max"
            type="number"
            min="0"
            step="1"
            inputMode="numeric"
            defaultValue={params.filed_after_max ?? ""}
            className={inputClassName}
            placeholder="45"
          />
        </div>

        <div className="min-w-0 lg:col-start-3 lg:row-start-3">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">PNL Min</label>
          <input
            name="pnl_min"
            type="number"
            step="0.1"
            inputMode="decimal"
            defaultValue={params.pnl_min ?? ""}
            className={inputClassName}
            placeholder="5"
          />
        </div>

        <div className="min-w-0 lg:col-start-4 lg:row-start-3">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">PNL Max</label>
          <input
            name="pnl_max"
            type="number"
            step="0.1"
            inputMode="decimal"
            defaultValue={params.pnl_max ?? ""}
            className={inputClassName}
            placeholder="25"
          />
        </div>

        <div className="min-w-0 lg:col-start-5 lg:row-start-3">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Signal Min</label>
          <input
            name="signal_min"
            type="number"
            min="0"
            max="100"
            step="1"
            inputMode="numeric"
            defaultValue={params.signal_min ?? ""}
            className={inputClassName}
            placeholder="70"
          />
        </div>

        <div className="md:col-span-2 lg:col-span-5">
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
      <FeedMinAmountInputEnhancer formId="feed-filters-form" inputName="max_amount" />
    </section>
  );
}
