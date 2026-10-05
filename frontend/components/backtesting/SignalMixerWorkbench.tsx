"use client";

import { useRef, useState } from "react";
import Link from "next/link";
import { runSignalMixer, type SignalMixerConfig, type SignalMixerResult } from "@/lib/api";
import { inputClassName, selectClassName, subtlePrimaryButtonClassName } from "@/lib/styles";

const confirmationNames = { congress: "Congress purchase disclosed", insider: "Insider purchase filed", government_contract: "Government contract recorded", analyst_upgrade: "Analyst upgrade published" };
const pct = (value: number | null) => value === null ? "—" : `${value.toFixed(1)}%`;

export function SignalMixerWorkbench({ canRun, today }: { canRun: boolean; today: string }) {
  const [trigger, setTrigger] = useState<SignalMixerConfig["trigger"]>("insider");
  const [confirmation, setConfirmation] = useState<SignalMixerConfig["confirmation"]>("analyst_upgrade");
  const [windowDays, setWindowDays] = useState(30);
  const [buyers, setBuyers] = useState(1);
  const [aboveSma, setAboveSma] = useState(false);
  const [startDate, setStartDate] = useState(() => { const day = new Date(`${today}T12:00:00Z`); day.setUTCFullYear(day.getUTCFullYear() - 3); return day.toISOString().slice(0, 10); });
  const [endDate, setEndDate] = useState(today);
  const [fees, setFees] = useState("0");
  const [slippage, setSlippage] = useState("10");
  const [result, setResult] = useState<SignalMixerResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const running = useRef(false);
  const config: SignalMixerConfig = { trigger, confirmation, window_days: windowDays, minimum_buyers: buyers, above_sma50: aboveSma, start_date: startDate, end_date: endDate, fee_bps: Number(fees), slippage_bps: Number(slippage) };
  const changed = result && JSON.stringify(result.config) !== JSON.stringify(config);

  async function run(event: React.FormEvent) {
    event.preventDefault();
    if (!canRun || running.current) return;
    running.current = true;
    setLoading(true); setError(null); setResult(null);
    try { setResult(await runSignalMixer(config)); }
    catch (error) { setError(error instanceof Error ? error.message : "Signal study unavailable. Please try again."); }
    finally { running.current = false; setLoading(false); }
  }

  return <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,.75fr)_minmax(0,1.25fr)]">
    <form onSubmit={run} className="tool-panel tool-panel-accent space-y-5"><div><p className="text-xs font-semibold text-emerald-300">Your signal recipe</p><h2 className="mt-2 text-lg font-semibold text-white">Choose the events to study</h2></div>
      <fieldset disabled={loading} className="grid gap-4 sm:grid-cols-2 disabled:opacity-60">
        <label className="space-y-2 text-xs font-medium text-slate-300"><span className="block text-emerald-300">Later purchase disclosure</span>
          <select className={`${selectClassName} w-full`} value={trigger} onChange={(event) => { const next = event.target.value as typeof trigger; setTrigger(next); if (next === confirmation) setConfirmation(next === "insider" ? "congress" : "insider"); }}>
            <option value="insider">Insider purchase filed</option><option value="congress">Congress purchase disclosed</option>
          </select>
        </label>
        <label className="space-y-2 text-xs font-medium text-slate-300"><span className="block text-sky-300">Earlier company event</span>
          <select className={`${selectClassName} w-full`} value={confirmation} onChange={(event) => setConfirmation(event.target.value as typeof confirmation)}>
            {Object.entries(confirmationNames).filter(([key]) => key !== trigger).map(([key, label]) => <option value={key} key={key}>{label}</option>)}
          </select>
        </label>
        <label className="space-y-2 text-xs font-medium text-slate-300"><span className="block">Within preceding days</span>
          <select className={`${selectClassName} w-full`} value={windowDays} onChange={(event) => setWindowDays(Number(event.target.value))}>{[7, 14, 30, 60, 90].map((days) => <option key={days} value={days}>{days} days</option>)}</select>
        </label>
        <label className="space-y-2 text-xs font-medium text-slate-300"><span className="block">Distinct buyers in trigger window</span>
          <select className={`${selectClassName} w-full`} value={buyers} onChange={(event) => setBuyers(Number(event.target.value))}>{[1, 2, 3, 5].map((count) => <option key={count} value={count}>{count === 1 ? "Any purchase" : `${count}+ distinct buyers`}</option>)}</select>
        </label>
        <label className="space-y-2 text-xs font-medium text-slate-300"><span className="block">Disclosures from</span><input required type="date" max={endDate} value={startDate} onChange={(event) => setStartDate(event.target.value)} className={`${inputClassName} w-full`} /></label>
        <label className="space-y-2 text-xs font-medium text-slate-300"><span className="block">Disclosures through</span><input required type="date" min={startDate} max={today} value={endDate} onChange={(event) => setEndDate(event.target.value)} className={`${inputClassName} w-full`} /></label>
        <label className="space-y-2 text-xs font-medium text-slate-300"><span className="block">Fees per side (basis points)</span><input required type="number" min="0" max="100" step="1" value={fees} onChange={(event) => setFees(event.target.value)} className={`${inputClassName} w-full`} /></label>
        <label className="space-y-2 text-xs font-medium text-slate-300"><span className="block">Slippage per side (basis points)</span><input required type="number" min="0" max="500" step="1" value={slippage} onChange={(event) => setSlippage(event.target.value)} className={`${inputClassName} w-full`} /></label>
        <label className="flex items-center gap-2 text-sm text-slate-300 sm:col-span-2"><input type="checkbox" checked={aboveSma} onChange={(event) => setAboveSma(event.target.checked)} />Require price above its 50-day moving average</label>
      </fieldset>
      <div className="rounded-xl border border-sky-400/15 bg-sky-400/5 p-3 text-xs leading-5 text-slate-400">Dates select purchase disclosures. Returns follow each match for 30, 90 and 365 days using available prices through today. Same-day event pairs are excluded. Contract timing uses Walnut’s first recorded observation, which limits historical coverage. 10 basis points = 0.10% per side.</div>
      <div className="flex flex-wrap items-center gap-3">
        <button type="submit" disabled={!canRun || loading} className={subtlePrimaryButtonClassName}>{loading ? "Finding matches and calculating returns…" : "Run signal study"}</button>
        {!canRun ? <Link href="/pricing" className="text-sm text-emerald-300">Included with Premium →</Link> : <span className="text-xs text-slate-500">Historical research · up to 5 years · no live alerts</span>}
      </div>
    </form>
    <section className="tool-panel tool-results min-w-0"><p className="text-xs font-semibold text-sky-300">Historical outcomes</p><h2 className="mt-2 mb-4 text-lg font-semibold text-white">What happened next?</h2>
    {loading ? <p role="status" className="mb-4 rounded-xl border border-sky-300/20 bg-sky-300/5 p-4 text-sm text-sky-200">Processing all qualifying disclosures in batches. Longer windows can take more time.</p> : null}
    {error ? <p role="alert" className="rounded-lg border border-rose-400/20 bg-rose-400/5 p-3 text-sm text-rose-200">{error}</p> : null}
    <div aria-live="polite">{result ? <div className="space-y-4">
      <div className="flex flex-wrap items-baseline gap-2"><h3 className="font-semibold text-white">{result.matched_setups} matching setups</h3><span className="text-xs text-slate-400">{result.config.start_date} → {result.config.end_date} · outcomes through {result.outcomes_as_of ?? result.config.end_date}</span></div>
      {changed ? <p className="text-xs text-amber-300">Inputs changed. Run again to update these results.</p> : null}
      {result.matched_setups === 0 ? <p className="rounded-xl border border-amber-300/20 bg-amber-300/5 p-4 text-sm leading-6 text-amber-100">The study completed, but no event pairs met this recipe in the stored data. Try a different earlier event or a wider interval between events. Government contracts have limited historical observation coverage.</p> : result.horizons.every(horizon => horizon.sample_size === 0) ? <p className="rounded-xl border border-sky-300/20 bg-sky-300/5 p-4 text-sm leading-6 text-sky-100">Matching events were found, but there are no complete, priced outcomes yet. Recent disclosures need time to reach each horizon; some matches may also lack prices. Choose earlier disclosure dates for mature outcomes.</p> : null}
      <div className="grid gap-3 md:grid-cols-3">{result.horizons.map((horizon) => <section key={horizon.days} className="tool-metric rounded-xl border border-white/10 p-4">
        <div className="flex items-center justify-between"><h4 className="font-semibold text-sky-300">{horizon.days} days</h4><span className="text-xs text-slate-400">n = {horizon.sample_size}</span></div>
        {horizon.sample_size < 30 ? <p className="mt-2 text-xs text-amber-300">{horizon.sample_size === 0 ? "No complete, priced outcomes" : "Small sample · exploratory only"}</p> : null}
        {(horizon.exclusions.pending ?? 0) > 0 ? <p className="mt-2 text-xs text-slate-400">{horizon.exclusions.pending} awaiting this horizon</p> : null}
        <dl className="mt-3 space-y-2 text-sm">{[
          ["Beat SPY", pct(horizon.beat_spy_rate_pct)], ["Positive return", pct(horizon.positive_return_rate_pct)], ["Median net return", pct(horizon.median_net_return_pct)], ["Median excess vs SPY", horizon.median_excess_return_pct === null ? "—" : `${horizon.median_excess_return_pct.toFixed(1)} pp`], ["Worst net outcome", pct(horizon.worst_return_pct)], ["Losing outcomes", String(horizon.loss_count)],
        ].map(([label, value]) => <div key={label} className="flex justify-between gap-3"><dt className="text-slate-400">{label}</dt><dd className="font-semibold tabular-nums text-slate-100">{value}</dd></div>)}</dl>
        <details className="mt-4 border-t border-white/10 pt-3 text-xs text-slate-400"><summary className="cursor-pointer text-slate-300">Coverage and exclusions</summary><ul className="mt-2 space-y-1">{Object.entries(horizon.exclusions).map(([key, value]) => <li key={key}>{key.replaceAll("_", " ")}: {value}</li>)}</ul></details>
        {horizon.examples.length ? <details className="mt-3 text-xs"><summary className="cursor-pointer text-sky-300">Inspect outcomes ({horizon.examples.length} shown)</summary><div className="mt-2 max-h-80 space-y-3 overflow-y-auto">{horizon.examples.map((row) => <div key={`${row.symbol}-${row.signal_date}`} className="border-t border-white/10 pt-2 text-slate-400"><Link className="font-semibold text-sky-300" href={`/ticker/${encodeURIComponent(row.symbol)}`}>{row.symbol}</Link><p>Earlier evidence {row.confirmation_date} → disclosure {row.signal_date}</p><p>{row.entry_date} → {row.exit_date} · {pct(row.net_return_pct)} net · SPY {pct(row.spy_return_pct)}</p><p className="text-[10px] text-slate-500">{row.trigger_id} · {row.confirmation_id}</p></div>)}</div></details> : null}
      </section>)}</div>
      <details className="rounded-xl border border-white/10 p-4 text-xs leading-5 text-slate-400"><summary className="cursor-pointer font-semibold text-slate-300">Methodology and source coverage</summary><ul className="mt-3 list-disc space-y-2 pl-4">{result.assumptions.map((text) => <li key={text}>{text}</li>)}</ul><p className="mt-3">{Object.entries(result.diagnostics).map(([key, value]) => `${key.replaceAll("_", " ")}: ${value}`).join(" · ")}</p><p>{result.methodology_version}</p></details>
    </div> : !loading && !error ? <div className="rounded-xl border border-dashed border-sky-300/20 bg-sky-300/[.025] px-5 py-12 text-center"><div className="mb-5 flex justify-center gap-3">{[30, 90, 365].map(days => <span key={days} className="rounded-xl border border-sky-300/15 bg-sky-300/5 px-4 py-3 text-sm font-semibold text-sky-200">{days}<span className="mt-1 block text-[10px] font-normal text-slate-400">days later</span></span>)}</div><h3 className="text-base font-semibold text-white">Start with a question, then follow the evidence</h3><p className="mx-auto mt-3 max-w-md text-sm leading-6 text-slate-400">Run your recipe to see matching disclosures, returns versus SPY, and how many outcomes have had time to mature.</p></div> : null}</div>
    </section>
  </div>;
}
