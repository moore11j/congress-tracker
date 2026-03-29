import { cardClassName, ghostButtonClassName, inputClassName, selectClassName } from "@/lib/styles";

type FeedMode = "congress" | "insider" | "all";
type WhaleMode = "off" | "500k" | "1m" | "5m";

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
    whale?: string;
  };
};

function modeHref(nextMode: FeedMode, params: FeedFiltersServerProps["params"]) {
  const url = new URLSearchParams();
  url.set("mode", nextMode);
  const keys = ["symbol", "min_amount", "recent_days", "member", "chamber", "party", "trade_type", "role", "whale"] as const;
  for (const key of keys) {
    const value = params[key]?.trim();
    if (value) url.set(key, value);
  }
  return `/?${url.toString()}`;
}

export function FeedFiltersServer({ mode, params }: FeedFiltersServerProps) {
  const whale = (params.whale === "500k" || params.whale === "1m" || params.whale === "5m" ? params.whale : "off") as WhaleMode;

  return (
    <section className={cardClassName}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Feed mode & filters</h2>
          <p className="text-sm text-slate-400">Server-rendered fallback filters (URL-driven GET navigation).</p>
        </div>
        <a href={`/?mode=${mode}`} className={ghostButtonClassName}>
          Reset
        </a>
      </div>

      <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap gap-2">
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
                className={`relative inline-flex items-center justify-center rounded-full border px-3 py-1.5 text-xs uppercase tracking-wide transition-colors duration-150 ${
                  active
                    ? "border-white/30 bg-white/[0.06] text-white font-medium"
                    : "border-white/10 bg-transparent text-white/60 font-semibold"
                }`}
              >
                {label}
              </a>
            );
          })}
        </div>

        <form method="GET" action="/" className="flex flex-wrap items-center gap-2">
          <input type="hidden" name="mode" value={mode} />
          <input type="hidden" name="symbol" value={params.symbol ?? ""} />
          <input type="hidden" name="min_amount" value={params.min_amount ?? ""} />
          <input type="hidden" name="recent_days" value={params.recent_days ?? ""} />
          <input type="hidden" name="member" value={params.member ?? ""} />
          <input type="hidden" name="chamber" value={params.chamber ?? ""} />
          <input type="hidden" name="party" value={params.party ?? ""} />
          <input type="hidden" name="trade_type" value={params.trade_type ?? ""} />
          <input type="hidden" name="role" value={params.role ?? ""} />
          <span className="text-xs font-semibold uppercase tracking-wide text-slate-400">Whale mode</span>
          <select name="whale" defaultValue={whale} className={selectClassName}>
            <option value="off">Off</option>
            <option value="500k">$500K+</option>
            <option value="1m">$1M+</option>
            <option value="5m">$5M+</option>
          </select>
          <button type="submit" className={ghostButtonClassName}>Apply</button>
        </form>
      </div>

      <form method="GET" action="/" className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <input type="hidden" name="mode" value={mode} />

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
          <input name="symbol" defaultValue={params.symbol ?? ""} className={inputClassName} placeholder="NVDA" />
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Min amount</label>
          <input name="min_amount" defaultValue={params.min_amount ?? ""} className={inputClassName} placeholder="250000" />
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

        <div>
          <input type="hidden" name="whale" value={params.whale ?? "off"} />
          <button type="submit" className={ghostButtonClassName}>Apply filters</button>
        </div>
      </form>
    </section>
  );
}
