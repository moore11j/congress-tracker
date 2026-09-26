"use client";

import { useEffect, useMemo, useRef, useState, type ComponentProps, type ReactNode } from "react";
import { ArrowDownToLine, ChartNoAxesCombined, Table2, TrendingUp, Users } from "lucide-react";
import { WalnutLineChart } from "@/components/charts/WalnutLineChart";
import { WalnutDonutChart } from "@/components/charts/WalnutDonutChart";
import { WalnutChartContainer } from "@/components/charts/WalnutChartContainer";
import { defaultRetirementPlan, projectRetirement, realAnnualReturn, retirementDisplayValue, validateRetirementPlan, type RetirementPerson, type RetirementPlan } from "@/lib/retirementCalculator";
import { RetirementReturnPicker, type ImportedReturn } from "./RetirementReturnPicker";
import { CalculatorNumberInput } from "./CalculatorNumberInput";
import "./retirement.css";

const money = (value: number) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value);
const compact = (value: number) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(value);
const colors = { you: "#6ee7b7", spouse: "#fbbf24", total: "#93c5fd" };

function ResponsiveLineChart(props: ComponentProps<typeof WalnutLineChart>) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(700);
  useEffect(() => {
    if (!ref.current) return;
    const observer = new ResizeObserver(([entry]) => setWidth(Math.max(280, Math.round(entry.contentRect.width))));
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, []);
  return <div ref={ref}><WalnutLineChart {...props} width={width} /></div>;
}

function NumberField({ label, value, onChange, min = 0, max = 1000000, step = 1, suffix }: { label: string; value: number; onChange: (value: number) => void; min?: number; max?: number; step?: number; suffix?: string }) {
  return <label className="block min-w-0 text-xs text-slate-400">{label}<div className="relative"><CalculatorNumberInput className="retirement-input pr-8" value={value} onCommit={onChange} min={min} max={max} integer={step === 1 && suffix !== "$"} />{suffix ? <span className="pointer-events-none absolute right-3 top-3 text-xs text-slate-500">{suffix}</span> : null}</div></label>;
}
function PersonFields({ name, person, onChange, color }: { name: string; person: RetirementPerson; onChange: (person: RetirementPerson) => void; color: string }) {
  const field = (key: keyof RetirementPerson) => (value: number) => onChange({ ...person, [key]: value });
  return <fieldset className="min-w-0"><legend className="mb-3 flex items-center gap-2 text-sm font-semibold text-white"><span className="h-2 w-2 rounded-full" style={{ background: color }} />{name}</legend><div className="grid grid-cols-2 gap-3"><NumberField label="Opening investment balance" value={person.openingBalance} onChange={field("openingBalance")} max={1e9} suffix="$" /><NumberField label="Monthly savings" value={person.monthlySavings} onChange={field("monthlySavings")} suffix="$" /><NumberField label="Current age" value={person.age} onChange={field("age")} min={18} max={100} /><NumberField label="Retirement age" value={person.retirementAge} onChange={field("retirementAge")} min={person.age} max={100} /><div className="col-span-2"><NumberField label="Monthly retirement income · today's dollars, after tax" value={person.monthlyIncome} onChange={field("monthlyIncome")} suffix="$" /></div></div></fieldset>;
}
function Metric({ label, value, detail, color = "text-white" }: { label: string; value: string; detail: string; color?: string }) {
  return <div className="min-w-0 rounded-xl border border-white/10 bg-slate-900/40 p-4"><p className="text-xs text-slate-400">{label}</p><p className={`mt-2 break-words text-2xl font-semibold tracking-tight tabular-nums ${color}`}>{value}</p><p className="mt-2 text-xs leading-5 text-slate-500">{detail}</p></div>;
}
function Panel({ title, children }: { title: string; children: ReactNode }) { return <section className="min-w-0 rounded-2xl border border-white/10 bg-slate-900/30 p-4 sm:p-5"><h2 className="mb-4 text-sm font-semibold text-white">{title}</h2>{children}</section>; }

export function RetirementCalculator({ startYear }: { startYear: number }) {
  const [draft, setDraft] = useState<RetirementPlan>(defaultRetirementPlan);
  const [plan, setPlan] = useState<RetirementPlan>(defaultRetirementPlan);
  const [view, setView] = useState("chart");
  const [era, setEra] = useState("all");
  const [real, setReal] = useState(false);
  const [revision, setRevision] = useState(0);
  const [importOpen, setImportOpen] = useState(false);
  const [savingSource, setSavingSource] = useState<ImportedReturn | null>(null);
  const [retirementSource, setRetirementSource] = useState<ImportedReturn | null>(null);
  const [errors, setErrors] = useState<string[]>([]);
  const dirty = JSON.stringify(draft) !== JSON.stringify(plan);
  const projection = useMemo(() => projectRetirement(plan, startYear), [plan, startYear]);
  const scenarios = useMemo(() => [-2, 0, 2].map((difference) => ({ difference, result: projectRetirement({ ...plan, annualReturn: Math.max(-99, Math.min(100, plan.annualReturn + difference)), retirementReturn: Math.max(-99, Math.min(100, plan.retirementReturn + difference)) }, startYear) })), [plan, startYear]);
  const adjust = (value: number, elapsed: number) => retirementDisplayValue(value, elapsed, plan.inflation, real);
  const displayedSavingReturn = real ? realAnnualReturn(plan.annualReturn, plan.inflation) : plan.annualReturn;
  const displayedRetirementReturn = real ? realAnnualReturn(plan.retirementReturn, plan.inflation) : plan.retirementReturn;
  const rows = projection.rows.filter((row) => era === "saving" ? row.year <= startYear + projection.bothYear : era === "retirement" ? row.year >= startYear + projection.firstYear : true);
  const final = projection.rows[projection.rows.length - 1];
  const both = projection.bothRetired;
  const firstShortfall = projection.rows.find((row) => row.shortfall > 0.01);
  const contributions = projection.rows.slice(0, projection.bothYear + 1).reduce((sum, row) => sum + row.contributions, 0);
  const initial = plan.you.openingBalance + (plan.includeSpouse ? plan.spouse.openingBalance : 0);
  const earned = projection.rows.slice(0, projection.bothYear + 1).reduce((sum, row) => sum + row.growth, 0);
  const change = (key: keyof RetirementPlan, value: number | boolean) => updateDraft({ ...draft, [key]: value });

  function updateDraft(next: RetirementPlan) {
    setDraft(next);
    const nextErrors = validateRetirementPlan(next);
    setErrors(nextErrors);
    if (nextErrors.length) return;
    setPlan(next); setRevision((v) => v + 1);
  }
  function applySource(source: ImportedReturn, target: "saving" | "retirement" | "both") {
    updateDraft({ ...draft, ...(target !== "retirement" ? { annualReturn: Number(source.rate.toFixed(4)) } : {}), ...(target !== "saving" ? { retirementReturn: Number(source.rate.toFixed(4)) } : {}) });
    if (target !== "retirement") setSavingSource(source);
    if (target !== "saving") setRetirementSource(source);
    setImportOpen(false);
  }
  function exportCsv() {
    const header = ["Year", "Your age", ...(plan.includeSpouse ? ["Spouse age"] : []), "Phase at year end", "Your balance", ...(plan.includeSpouse ? ["Spouse balance"] : []), "Total balance", "Contributions", "Investment growth", "Gross withdrawals", "After-tax income", "Income shortfall"];
    const values = rows.map((r) => [r.year, r.youAge, ...(plan.includeSpouse ? [r.spouseAge] : []), r.phase, ...[r.you, ...(plan.includeSpouse ? [r.spouse] : []), r.total, r.contributions, r.growth, r.withdrawals, r.income, r.shortfall].map((n) => adjust(n, r.year - startYear).toFixed(2))]);
    const csv = [header, ...values].map((row) => row.join(",")).join("\r\n");
    const url = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8;" }));
    const a = document.createElement("a"); a.href = url; a.download = `walnut-retirement-${real ? "today-dollars" : "nominal"}-${era}.csv`; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  return <div className="retirement-calculator space-y-6">
    <div className="grid items-start gap-6 xl:grid-cols-[370px_minmax(0,1fr)]">
      <form onSubmit={(e) => { e.preventDefault(); updateDraft(draft); }} className="space-y-4" noValidate>
        <Panel title="Build your retirement plan"><div className="space-y-5"><PersonFields name="You" person={draft.you} color={colors.you} onChange={(you) => updateDraft({ ...draft, you })} /><div className="border-t border-white/10 pt-4"><label className="flex items-center gap-2 text-sm text-slate-200"><input type="checkbox" className="h-4 w-4 accent-emerald-300" checked={draft.includeSpouse} onChange={(e) => change("includeSpouse", e.target.checked)} /><Users size={15} /> Include spouse or partner</label></div>{draft.includeSpouse ? <PersonFields name="Spouse / partner" color={colors.spouse} person={draft.spouse} onChange={(spouse) => updateDraft({ ...draft, spouse })} /> : null}</div></Panel>
        <Panel title="Return & withdrawal assumptions"><p className="-mt-2 mb-4 text-xs leading-5 text-slate-400">Enter nominal annual returns. The real view adjusts returns and balances for your inflation assumption.</p><div className="grid grid-cols-2 gap-3"><NumberField label="Annual return · saving" value={draft.annualReturn} onChange={(v) => { change("annualReturn", v); setSavingSource(null); }} min={-99} max={100} step={0.1} suffix="%" /><NumberField label="Annual return · retired" value={draft.retirementReturn} onChange={(v) => { change("retirementReturn", v); setRetirementSource(null); }} min={-99} max={100} step={0.1} suffix="%" /><NumberField label="Annual inflation" value={draft.inflation} onChange={(v) => change("inflation", v)} max={20} step={0.1} suffix="%" /><NumberField label="Withdrawal tax rate" value={draft.taxRate} onChange={(v) => change("taxRate", v)} max={60} step={0.1} suffix="%" /><div className="col-span-2"><NumberField label={draft.includeSpouse ? "Years to project after both retire" : "Years to project after retirement"} value={draft.retirementYears} onChange={(v) => change("retirementYears", v)} min={1} max={60} /></div></div>
          <button type="button" aria-expanded={importOpen} onClick={() => setImportOpen(!importOpen)} className="mt-4 flex w-full items-center justify-center gap-2 rounded-lg border border-emerald-300/25 bg-emerald-300/5 px-3 py-2.5 text-xs font-semibold text-emerald-200 hover:bg-emerald-300/10"><TrendingUp size={15} />Import a historical annual return</button>
          {importOpen ? <RetirementReturnPicker onApply={applySource} /> : null}
          {savingSource || retirementSource ? <div className="mt-3 space-y-1 text-xs leading-5 text-slate-400">{savingSource ? <p>Saving: {savingSource.name} · {savingSource.rate.toFixed(2)}% · {savingSource.period}</p> : null}{retirementSource ? <p>Retired: {retirementSource.name} · {retirementSource.rate.toFixed(2)}% · {retirementSource.period}</p> : null}</div> : null}
        </Panel>
        {errors.length ? <div role="alert" className="rounded-xl border border-amber-300/20 bg-amber-300/5 p-3 text-xs leading-5 text-amber-200">{errors.map((error) => <p key={error}>{error}</p>)}</div> : null}
        <button type="submit" className="flex w-full items-center justify-center gap-2 rounded-xl bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200"><ChartNoAxesCombined size={18} />Calculate my projection</button>
        <p className="text-center text-xs text-slate-500">Updates automatically when you Tab or click away from a number. Calculations run in your browser.</p>
      </form>

      <div className="min-w-0 space-y-5">
        <div className="space-y-3"><div className="flex flex-wrap items-center justify-between gap-3"><p role="status" className={`text-xs ${dirty ? "text-amber-200" : "text-slate-400"}`}>{dirty ? "Check the highlighted inputs. Results show the last valid projection." : "Projection updated"}</p><div role="group" aria-label="Nominal or real projection" className="flex flex-wrap gap-1 rounded-lg border border-white/10 bg-slate-950/50 p-1">{[{ value: false, label: "Nominal · future dollars" }, { value: true, label: "Real · today's dollars" }].map((option) => <button key={option.label} type="button" aria-pressed={real === option.value} onClick={() => setReal(option.value)} className={`rounded-md px-3 py-2 text-xs transition ${real === option.value ? "bg-emerald-300/15 text-emerald-200" : "text-slate-400 hover:text-white"}`}>{option.label}</button>)}</div></div><p aria-live="polite" className="text-xs leading-5 text-slate-400">{real ? "Real annual returns" : "Nominal annual returns"}: <strong className="text-slate-200">{displayedSavingReturn.toFixed(2)}% saving / {displayedRetirementReturn.toFixed(2)}% retired</strong>{real ? ` · adjusted for ${plan.inflation}% inflation. Balances, chart, table, and CSV show today's purchasing power.` : " · before inflation. Balances, chart, table, and CSV show future dollars."}</p></div>
        <div className={`grid gap-3 ${plan.includeSpouse ? "sm:grid-cols-3" : "sm:grid-cols-2"}`} aria-live="polite">
          <Metric label="At your retirement" value={money(adjust(projection.atRetirement[0], plan.you.retirementAge - plan.you.age))} detail={`Your balance · age ${plan.you.retirementAge} · ${startYear + plan.you.retirementAge - plan.you.age}`} color="text-emerald-200" />
          {plan.includeSpouse ? <Metric label="At spouse's retirement" value={money(adjust(projection.atRetirement[1], plan.spouse.retirementAge - plan.spouse.age))} detail={`Spouse's balance · age ${plan.spouse.retirementAge} · ${startYear + plan.spouse.retirementAge - plan.spouse.age}`} color="text-amber-200" /> : null}
          <Metric label={plan.includeSpouse ? "When both are retired" : "At end of projection"} value={money(adjust(plan.includeSpouse ? both.total : final.total, plan.includeSpouse ? projection.bothYear : final.year - startYear))} detail={plan.includeSpouse ? `Combined balance in ${both.year}, including earlier withdrawals` : `Remaining balance in ${final.year}`} color="text-blue-200" />
        </div>

        <section className="min-w-0 overflow-hidden rounded-2xl border border-white/10 bg-slate-900/30">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 p-4 sm:p-5"><div><h2 className="text-base font-semibold text-white">Your investment journey</h2><p className="mt-1 text-xs text-slate-400">{real ? "Inflation-adjusted, today's dollars" : "Future dollars"} · monthly compounding · annual snapshots</p></div><div className="flex rounded-lg border border-white/10 bg-slate-950/50 p-1" aria-label="Projection view">{[["chart", "Chart", ChartNoAxesCombined], ["table", "Table", Table2]].map(([key, label, Icon]) => { const Glyph = Icon as typeof Table2; return <button key={key as string} type="button" aria-pressed={view === key} onClick={() => setView(key as string)} className={`flex items-center gap-1.5 rounded-md px-3 py-2 text-xs ${view === key ? "bg-white/10 text-white" : "text-slate-400"}`}><Glyph size={14} />{label as string}</button>; })}</div></div>
          <div className="flex flex-wrap items-center justify-between gap-3 px-4 pt-4"><div className="flex flex-wrap gap-1" aria-label="Projection period">{[["all", "Full journey"], ["saving", "Building wealth"], ["retirement", "Retirement income"]].map(([key, label]) => <button key={key} type="button" aria-pressed={era === key} onClick={() => setEra(key)} className={`rounded-full px-3 py-1.5 text-xs transition ${era === key ? "bg-emerald-300/10 text-emerald-200" : "text-slate-400 hover:text-white"}`}>{label}</button>)}</div><button type="button" onClick={exportCsv} className="flex items-center gap-1 text-xs text-slate-400 hover:text-white"><ArrowDownToLine size={14} />CSV</button></div>
          <div className="p-2 sm:p-4"><WalnutChartContainer label="Retirement projection">
            {view === "chart" ? <div key={`${revision}-${era}-${real}`} className="retirement-reveal"><div className="retirement-plot"><ResponsiveLineChart data={rows.map((r) => ({ label: String(r.year) }))} minValue={0} height={340} ariaLabel="Projected investment balances for you, spouse, and household through saving and withdrawals" formatValue={compact} series={[{ key: "total", label: "Combined", color: colors.total, values: rows.map((r) => adjust(r.total, r.year - startYear)), areaGradient: { top: "rgba(147,197,253,.16)", bottom: "rgba(147,197,253,0)" } }, { key: "you", label: "You", color: colors.you, values: rows.map((r) => adjust(r.you, r.year - startYear)) }, ...(plan.includeSpouse ? [{ key: "spouse", label: "Spouse", color: colors.spouse, values: rows.map((r) => adjust(r.spouse, r.year - startYear)) }] : [])]} renderTooltip={(index) => { const row = rows[index]; return <div className="space-y-2 text-xs"><p className="font-semibold text-white">{row.year} · {row.phase}</p><p className="text-slate-400">Age {row.youAge}{plan.includeSpouse ? ` / ${row.spouseAge}` : ""}</p><p style={{ color: colors.you }}>You <span className="float-right">{money(adjust(row.you, row.year - startYear))}</span></p>{plan.includeSpouse ? <p style={{ color: colors.spouse }}>Spouse <span className="float-right">{money(adjust(row.spouse, row.year - startYear))}</span></p> : null}<p style={{ color: colors.total }}>Total <span className="float-right">{money(adjust(row.total, row.year - startYear))}</span></p><p className="border-t border-white/10 pt-2 text-slate-400">Annual withdrawals <span className="float-right">{money(adjust(row.withdrawals, row.year - startYear))}</span></p></div>; }} /></div><div className="flex justify-center gap-5 text-xs text-slate-300">{[["You", colors.you], ...(plan.includeSpouse ? [["Spouse", colors.spouse]] : []), ["Combined", colors.total]].map(([label, color]) => <span key={label} className="flex items-center gap-2"><i className="h-1 w-4 rounded" style={{ background: color }} />{label}</span>)}</div></div> : <div className="max-h-[440px] overflow-auto rounded-xl" tabIndex={0} aria-label="Scrollable yearly projection"><table className="w-full whitespace-nowrap text-right text-xs tabular-nums"><caption className="sr-only">Annual projection in {real ? "today's" : "future"} dollars. Flows cover the preceding year; first row is the opening snapshot.</caption><thead className="sticky top-0 bg-slate-900 text-slate-400"><tr>{["Year / age", "Phase at year end", "You", ...(plan.includeSpouse ? ["Spouse"] : []), "Combined", "Saved", "Growth", "Withdrawn", "Net income", "Income shortfall"].map((h) => <th key={h} scope="col" className="px-3 py-3 font-medium">{h}</th>)}</tr></thead><tbody className="divide-y divide-white/5">{rows.map((row) => <tr key={row.year} className="text-slate-300 hover:bg-white/5"><th scope="row" className="px-3 py-3 font-medium text-white">{row.year} <span className="text-slate-500">/ {row.youAge}{plan.includeSpouse ? ` · ${row.spouseAge}` : ""}</span></th><td className="px-3 py-3 text-slate-500">{row.phase}</td>{[row.you, ...(plan.includeSpouse ? [row.spouse] : []), row.total, row.contributions, row.growth, row.withdrawals, row.income, row.shortfall].map((value, i) => <td key={i} className="px-3 py-3">{money(adjust(value, row.year - startYear))}</td>)}</tr>)}</tbody></table></div>}
          </WalnutChartContainer></div>
          <div className="grid gap-2 border-t border-white/10 bg-white/[0.015] p-4 sm:grid-cols-3"><p className="text-xs text-slate-400"><span className="mb-1 block text-emerald-200">Your retirement</span>{startYear + plan.you.retirementAge - plan.you.age} · age {plan.you.retirementAge}</p>{plan.includeSpouse ? <p className="text-xs text-slate-400"><span className="mb-1 block text-amber-200">Spouse's retirement</span>{startYear + plan.spouse.retirementAge - plan.spouse.age} · age {plan.spouse.retirementAge}</p> : null}<p className="text-xs text-slate-400"><span className="mb-1 block text-blue-200">Projection ends</span>{final.year} · your age {final.youAge}</p></div>
        </section>

        <div className="grid gap-4 md:grid-cols-2">
          <Panel title={plan.includeSpouse ? "Household balance when both retire" : "Balance at retirement"}><div className="flex flex-wrap items-center gap-4"><WalnutDonutChart size={150} ariaLabel="Share of household investments at retirement" segments={[{ label: "You", value: both.you, color: colors.you }, ...(plan.includeSpouse ? [{ label: "Spouse", value: both.spouse, color: colors.spouse }] : [])]} value={compact(adjust(both.total, projection.bothYear))} label={String(both.year)} /><div className="min-w-0 flex-1 space-y-3 text-xs"><p className="flex justify-between gap-2 text-emerald-200"><span>You</span><b>{money(adjust(both.you, projection.bothYear))}</b></p>{plan.includeSpouse ? <p className="flex justify-between gap-2 text-amber-200"><span>Spouse</span><b>{money(adjust(both.spouse, projection.bothYear))}</b></p> : null}<p className="leading-5 text-slate-500">Balances at the same date, after any earlier retirement withdrawals.</p></div></div></Panel>
          <Panel title="Retirement income outlook"><p className="text-3xl font-semibold text-white">{money((plan.you.monthlyIncome + (plan.includeSpouse ? plan.spouse.monthlyIncome : 0)) * 12)}<span className="ml-1 text-xs font-normal text-slate-400">/ year</span></p><p className="mt-2 text-xs leading-5 text-slate-400">Combined target after tax, in today's dollars. Each person's withdrawals start at their own retirement.</p><div className={`mt-4 rounded-lg p-3 text-xs leading-5 ${firstShortfall ? "bg-amber-300/5 text-amber-200" : "bg-emerald-300/5 text-emerald-200"}`}>{firstShortfall ? `First income shortfall: ${firstShortfall.year}. At least one account cannot cover its planned withdrawal. Accounts do not automatically fund each other.` : `Planned income is covered through ${final.year} under these assumptions.`}</div><p className="mt-3 text-xs text-slate-400">Ending balance <strong className="float-right text-white">{money(adjust(final.total, final.year - startYear))}</strong></p></Panel>
        </div>

        <Panel title="What if returns change?"><p className="-mt-2 mb-4 text-xs leading-5 text-slate-400">A two-percentage-point change to both return assumptions. These are illustrative scenarios, not probabilities or confidence bounds.</p><div className="grid gap-3 sm:grid-cols-3">{scenarios.map(({ difference, result }) => <div key={difference} className={`rounded-xl border p-3 ${difference === 0 ? "border-emerald-300/25 bg-emerald-300/5" : "border-white/10"}`}><p className="text-xs text-slate-400">{difference === 0 ? "Your assumptions" : difference < 0 ? "Lower returns · −2 pts" : "Higher returns · +2 pts"}</p><p className="mt-2 text-lg font-semibold text-white">{compact(adjust(result.bothRetired.total, result.bothYear))}</p><p className="mt-1 text-xs text-slate-500">{plan.includeSpouse ? "When both retire" : "At retirement"}</p><p className="mt-3 text-xs text-slate-400">{result.rows.some((r) => r.shortfall > 0.01) ? `Income shortfall by ${result.rows.find((r) => r.shortfall > 0.01)!.year}` : `Income covered to ${final.year}`}</p></div>)}</div></Panel>
        <Panel title="Where the retirement balance comes from"><div className="grid gap-4 sm:grid-cols-3">{[["Opening investments", initial], ["Added savings", contributions], ["Investment growth", earned]].map(([label, value]) => <div key={label as string}><p className="text-xs text-slate-400">{label as string}</p><p className="mt-2 text-lg font-semibold text-white">{money(value as number)}</p></div>)}</div><p className="mt-3 text-xs leading-5 text-slate-500">Cumulative future-dollar amounts through {both.year}. Subtract withdrawals before that date to reconcile with the combined balance.</p></Panel>
      </div>
    </div>
    <section className="rounded-2xl border border-white/10 bg-slate-900/20 p-5 text-sm leading-6 text-slate-400"><h2 className="mb-2 font-semibold text-white">How this retirement calculator works</h2><p>Annual returns are effective rates converted into monthly compound growth. Savings are added at month end until each person's retirement. From the following month, their account uses the retirement return and pays the requested income, adjusted for inflation from today and increased to cover the withdrawal tax. Contributions stay fixed in dollar terms. Each account is modeled separately and cannot fall below zero.</p><p className="mt-3">The table records annual ending balances and the preceding year's savings, growth, gross withdrawals, after-tax income, and unmet income. In today's dollars mode, amounts are discounted using inflation at each row's year end. The projection continues for the selected number of years after the later retirement date.</p><p className="mt-3 text-xs text-slate-500">Illustrative planning only, not a forecast or personalized investment advice. Constant returns do not model market volatility or sequence-of-returns risk. Taxes use a simplified flat withdrawal rate; fees, taxes on investment growth, account limits, pensions, and Social Security are not included. Imported historical performance does not guarantee future results.</p></section>
  </div>;
}
