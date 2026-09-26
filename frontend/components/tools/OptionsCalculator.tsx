"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { ArrowDownRight, ArrowUpRight, ArrowLeftRight, Waves, Plus, Trash2, Download, SlidersHorizontal } from "lucide-react";
import { WalnutLineChart } from "@/components/charts/WalnutLineChart";
import { WalnutChartContainer, WalnutChartSkeleton } from "@/components/charts/WalnutChartContainer";
import { ApiError, getCalculatorClose, getCalculatorExpirations, getCalculatorOptionContracts, getCalculatorOptionPrices, type CalculatorOptionChain, type CalculatorOptionContract } from "@/lib/api";
import { applyOptionClose, optionCloseLabel, createStrategy, matchListedPosition, daysUntil, entryCost, entryFees, expirationRisk, optionValue, positionGreeks, profitAt, strategyTemplates, type ModelInputs, type OptionLeg, type Position } from "@/lib/optionsCalculator";
import { loadOptionsData, optionPriceBatch, waitForOptions } from "@/lib/optionsMarketData";
import { OptionsTickerInput } from "./OptionsTickerInput";
import { CalculatorNumberInput } from "./CalculatorNumberInput";
import "./options.css";

const money = (n: number) => Number.isFinite(n) ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(n) : "Unlimited";
const compact = (n: number) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(n);
const pnlClass = (n: number) => n >= 0 ? "text-emerald-300" : "text-rose-300";
// Chart geometry must be deterministic across Node and browser Math implementations.
// Display cash flows to cents; the pricing/risk engine retains full precision.
const chartCash = (n: number) => Math.round(n * 100) / 100;
function Field({ label, value, onChange, min = 0, max = 1000000, step = .01 }: { label: string; value: number; onChange: (value: number) => void; min?: number; max?: number; step?: number }) {
  return <label>{label}<CalculatorNumberInput value={value} onCommit={onChange} min={min} max={max} integer={step === 1} /></label>;
}
function Metric({ label, value, note, tone = "text-white" }: { label: string; value: string; note: string; tone?: string }) {
  return <div className="options-panel"><p className="text-xs text-slate-400">{label}</p><p className={`mt-2 break-words text-2xl font-semibold tracking-tight tabular-nums ${tone}`}>{value}</p><p className="mt-2 text-xs leading-5 text-slate-500">{note}</p></div>;
}
const outlooks = [
  { id: "bullish", label: "Price goes up", icon: ArrowUpRight }, { id: "bearish", label: "Price goes down", icon: ArrowDownRight },
  { id: "range", label: "Stays in a range", icon: ArrowLeftRight }, { id: "move", label: "Makes a big move", icon: Waves },
  { id: "vol-up", label: "Volatility rises", icon: Waves }, { id: "vol-down", label: "Volatility falls", icon: SlidersHorizontal },
];

export function OptionsCalculator({ today, initialExpiry }: { today: string; initialExpiry: string }) {
  const [symbol, setSymbol] = useState("SPY"), [expiration, setExpiration] = useState(initialExpiry);
  const [spot, setSpot] = useState(100), [spotSource, setSpotSource] = useState("Illustrative price · enter or load a close");
  const [volatility, setVolatility] = useState(30), [rate, setRate] = useState(4), [dividend, setDividend] = useState(0);
  const days = daysUntil(today, expiration);
  const model: ModelInputs = { spot, days, volatility, rate, dividend };
  const positionRevision = useRef(0);
  const [position, writePosition] = useState<Position>(() => createStrategy("bull-call", { spot: 100, days: daysUntil(today, initialExpiry), volatility: 30, rate: 4, dividend: 0 }));
  const setPosition: typeof writePosition = action => { positionRevision.current++; writePosition(action); };
  const latestModel = useRef(model); latestModel.current = model;
  const [outlook, setOutlook] = useState("bullish"), [strategy, setStrategy] = useState("bull-call");
  const [move, setMove] = useState(10), [moveUnit, setMoveUnit] = useState<"%" | "$">("%");
  const [elapsed, setElapsed] = useState(15), [scenarioVol, setScenarioVol] = useState(30);
  const [view, setView] = useState("Chart"), [chain, setChain] = useState<CalculatorOptionChain | null>(null);
  const [allStrikes, setAllStrikes] = useState(false);
  const [busy, setBusy] = useState(""), [notice, setNotice] = useState("");
  const [listedDates, setListedDates] = useState<string[]>([]), [dateCursor, setDateCursor] = useState<string | null>(null);
  const [fullPreload, setFullPreload] = useState(false), [priceStatus, setPriceStatus] = useState("");
  const [pricePaused, setPricePaused] = useState(false), [hidden, setHidden] = useState(false), [retry, setRetry] = useState(0);
  const nextPriceAt = useRef(0), marketAbort = useRef<AbortController | null>(null);
  const generation = useRef(0), legId = useRef(10);
  const chartRef = useRef<HTMLDivElement>(null), [chartWidth, setChartWidth] = useState(700);
  useEffect(() => { if (!chartRef.current) return; const observer = new ResizeObserver(([entry]) => setChartWidth(Math.max(280, Math.floor(entry.contentRect.width)))); observer.observe(chartRef.current); return () => observer.disconnect(); }, [view]);
  useEffect(() => () => { generation.current++; marketAbort.current?.abort(); }, []);
  const resetDataContext = () => { generation.current++; marketAbort.current?.abort(); setBusy(""); setChain(null); setNotice(""); setFullPreload(false); setPricePaused(false); setPriceStatus(""); setPosition(p => ({ ...p, legs: p.legs.map(l => ({ ...l, contract: undefined, source: "Carried entry · review price" })) })); };
  const request = async (label: string, action: (isCurrent: () => boolean, signal: AbortSignal) => Promise<void>) => {
    marketAbort.current?.abort(); const controller = new AbortController(); marketAbort.current = controller;
    const id = ++generation.current; setBusy(label); setNotice("");
    try { await action(() => id === generation.current && !controller.signal.aborted, controller.signal); } catch (error) { if (id === generation.current && !controller.signal.aborted) setNotice(error instanceof Error ? error.message : "Data is unavailable. Enter prices manually."); }
    finally { if (id === generation.current) setBusy(""); }
  };
  const updateLeg = (id: string, patch: Partial<OptionLeg>) => { generation.current++; marketAbort.current?.abort(); setBusy(""); setStrategy("custom"); setPosition(p => ({ ...p, legs: p.legs.map(l => l.id === id ? { ...l, ...patch } : l) })); };
  const chooseStrategy = (id: string) => { generation.current++; marketAbort.current?.abort(); setBusy(""); setStrategy(id); setPosition(p => ({ ...matchListedPosition(createStrategy(id, model), chain?.contracts ?? [], model), fee: p.fee })); };
  const loadMarket = (ticker: string, selectedExpiry?: string) => request("market", async (current, signal) => {
    const wait = () => { if (current()) setNotice("Waiting for the shared free data allowance; loading resumes automatically. You can still edit manually."); };
    const revision = positionRevision.current;
    let marketSpot = spot, date = selectedExpiry ?? expiration;
    if (!selectedExpiry) {
      const stock = await loadOptionsData(() => getCalculatorClose(ticker, signal), signal, wait);
      if (!current()) return;
      marketSpot = stock.price; setSpot(stock.price); setSpotSource(optionCloseLabel(stock));
      const dates = await loadOptionsData(() => getCalculatorExpirations(ticker, marketSpot, undefined, signal), signal, wait);
      if (!current()) return;
      const available = dates.expirations.filter(d => d >= today);
      setListedDates(available); setDateCursor(dates.next_cursor ?? null);
      date = [...available].sort((a, b) => Math.abs(daysUntil(today, a) - daysUntil(today, expiration)) - Math.abs(daysUntil(today, b) - daysUntil(today, expiration)))[0] ?? expiration;
      setExpiration(date);
    }
    const result = await loadOptionsData(() => getCalculatorOptionContracts(ticker, date, undefined, signal), signal, wait);
    if (!current()) return;
    setChain(result); setPricePaused(false); setFullPreload(result.price_provider === "alpaca"); nextPriceAt.current = 0;
    const inputs = { ...latestModel.current, spot: marketSpot, days: daysUntil(today, date) };
    if (revision === positionRevision.current) writePosition(p => strategy === "custom" ? p : { ...matchListedPosition(createStrategy(strategy, inputs), result.contracts, inputs), fee: p.fee });
    setNotice(result.contracts.length ? (result.price_provider === "alpaca" ? "Listed contracts loaded. Historical prices load automatically in batches of up to 100." : "Listed contracts loaded. Closing prices for modeled strategy legs load automatically.") : "No standard contracts found. Choose another listed expiration or use a manual date.");
  });
  useEffect(() => { void loadMarket("SPY"); /* Initial data only; selections call loadMarket explicitly. */
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  useEffect(() => { const update = () => setHidden(document.hidden); update(); document.addEventListener("visibilitychange", update); return () => document.removeEventListener("visibilitychange", update); }, []);
  // Selected legs take priority. Whole-chain requests are paced and stop off-screen or on context changes.
  useEffect(() => {
    if (!chain || busy || hidden || pricePaused) return;
    const selected = position.legs.filter(l => l.contract && l.source === "Modeled entry");
    const cached = selected.map(l => chain.contracts.find(c => c.ticker === l.contract)).find(c => c?.close);
    if (cached?.close) { const close = cached.close; writePosition(p => applyOptionClose(p, close)); return; }
    const batch = optionPriceBatch(chain, selected.map(l => l.contract!), spot, fullPreload);
    const contract = batch[0];
    const pageCursor = !contract && fullPreload ? chain.next_cursor : null;
    if (!contract && !pageCursor) { setPriceStatus(!chain.next_cursor && chain.contracts.every(c => c.close || c.no_trade) ? "All listed contracts checked." : "Selected entry prices checked."); setFullPreload(false); return; }
    const controller = new AbortController(), { signal } = controller;
    void (async () => {
      try {
        setPriceStatus(pageCursor ? "Loading more listed contracts…" : chain.price_provider === "alpaca" ? `Loading historical prices · ${batch.length} contracts…` : `Loading close · ${contract!.ticker}`);
        await waitForOptions(Math.max(0, nextPriceAt.current - Date.now()), signal);
        nextPriceAt.current = Date.now() + (chain.price_provider === "alpaca" && !pageCursor ? 500 : 13000);
        if (pageCursor) {
          const page = await getCalculatorOptionContracts(chain.symbol, chain.expiration, pageCursor, signal);
          if (!signal.aborted) setChain(c => c ? { ...page, excluded: c.excluded + page.excluded, contracts: [...new Map([...c.contracts, ...page.contracts].map(item => [item.ticker, item])).values()] } : page);
          return;
        }
        if (!contract) return;
        if (chain.price_provider === "alpaca") {
          const result = await getCalculatorOptionPrices(batch.map(c => c.ticker), signal);
          if (signal.aborted) return;
          const closes = new Map(result.closes.map(close => [close.ticker, close])), missing = new Set(result.no_trade);
          setChain(c => c ? { ...c, contracts: c.contracts.map(item => closes.has(item.ticker) ? { ...item, close: closes.get(item.ticker), no_trade: false } : missing.has(item.ticker) ? { ...item, no_trade: true } : item) } : c);
          writePosition(p => result.closes.reduce(applyOptionClose, p));
          return;
        }
        const close = await getCalculatorClose(contract.ticker, signal);
        if (signal.aborted) return;
        setChain(c => c ? { ...c, contracts: c.contracts.map(item => item.ticker === close.ticker ? { ...item, close } : item) } : c);
        writePosition(p => applyOptionClose(p, close));
      } catch (error) {
        if (signal.aborted) return;
        if (error instanceof ApiError && error.status === 429) {
          nextPriceAt.current = Date.now() + 61000; setPriceStatus("Free allowance busy · retrying in one minute.");
          try { await waitForOptions(61000, signal); setRetry(n => n + 1); } catch { /* cancelled */ }
        } else if (contract && error instanceof ApiError && error.status === 404) {
          setChain(c => c ? { ...c, contracts: c.contracts.map(item => item.ticker === contract.ticker ? { ...item, no_trade: true } : item) } : c);
        } else { setPricePaused(true); setPriceStatus(error instanceof Error ? error.message : "Price loading paused. Retry when ready."); }
      }
    })();
    return () => controller.abort();
  }, [chain, position.legs, fullPreload, busy, hidden, pricePaused, retry, spot]);
  const addContract = (contract?: CalculatorOptionContract) => {
    if (position.legs.length >= 6) return;
    const kind = contract?.kind ?? "call", strike = contract?.strike ?? spot;
    setStrategy("custom");
    setPosition(p => ({ ...p, legs: [...p.legs, { id: `leg-${legId.current++}`, kind, strike, side: 1, premium: contract?.close?.price ?? Math.round(optionValue(kind, strike, model) * 100) / 100, quantity: 1, source: contract?.close ? optionCloseLabel(contract.close) : "Modeled entry", contract: contract?.ticker }] }));
  };
  const target = Math.max(0, moveUnit === "%" ? spot * (1 + move / 100) : spot + move);
  const horizon = Math.min(days, elapsed), remaining = Math.max(0, days - horizon);
  const scenario = { ...model, days: remaining, volatility: scenarioVol };
  const risk = expirationRisk(position), debit = entryCost(position) + entryFees(position), greeks = positionGreeks(position, model);
  const targetPnl = profitAt(position, { ...scenario, spot: target });
  const plot = useMemo(() => {
    const low = Math.max(0, Math.min(spot * .6, target * .9, ...position.legs.map(l => l.strike * .85)));
    const high = Math.max(spot * 1.4, target * 1.1, ...position.legs.map(l => l.strike * 1.15), low + 1);
    return Array.from({ length: 81 }, (_, i) => { const price = low + (high - low) * i / 80; return { price, label: money(price), expiry: chartCash(profitAt(position, { ...model, spot: price, days: 0 })), scenario: chartCash(profitAt(position, { ...scenario, spot: price })) }; });
  // Every input is represented explicitly so the chart updates with the model.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [position, spot, days, volatility, rate, dividend, target, remaining, scenarioVol]);
  const group = outlook === "vol-up" ? "move" : outlook === "vol-down" ? "range" : outlook;
  const templates = strategyTemplates.filter(t => t.outlook === group);
  const rows = chain && chain.symbol === symbol && chain.expiration === expiration ? [...new Set(chain.contracts.map(c => c.strike))].sort((a, b) => Math.abs(a - spot) - Math.abs(b - spot)).slice(0, allStrikes ? undefined : 21).sort((a, b) => a - b) : [];
  const exportCsv = () => {
    const content = [`Stock price,Expiration P/L,Scenario P/L (${horizon} days elapsed; ${scenarioVol}% volatility)`, ...plot.map(p => [p.price.toFixed(2), p.expiry.toFixed(2), p.scenario.toFixed(2)].join(","))].join("\r\n");
    const url = URL.createObjectURL(new Blob([content], { type: "text/csv;charset=utf-8" })); const link = document.createElement("a"); link.href = url; link.download = `options-scenarios-${symbol || "custom"}.csv`; link.click(); URL.revokeObjectURL(url);
  };
  return <div className="options-calculator space-y-5">
    <section className="options-panel">
      <div className="flex flex-wrap items-center justify-between gap-3"><h2>01 <span className="ml-2">Build your outlook</span></h2><span className="options-tag">Standard US equity options · 100 shares per contract</span></div>
      <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <OptionsTickerInput value={symbol} onSelect={ticker => { resetDataContext(); setSymbol(ticker); setListedDates([]); setDateCursor(null); setSpotSource("Carried stock price · review for this ticker"); void loadMarket(ticker); }} />
        <Field label="Stock price ($)" value={spot} min={.01} onChange={v => { generation.current++; marketAbort.current?.abort(); setBusy(""); setSpot(v); setSpotSource("Manual stock price"); }} />
        <div><label>Listed expiration<select aria-label="Listed expiration" value={listedDates.includes(expiration) ? expiration : "manual"} onChange={e => { if (e.target.value === "manual") return; resetDataContext(); setExpiration(e.target.value); void loadMarket(symbol, e.target.value); }}><option value="manual">Choose a date manually</option>{listedDates.map(date => <option key={date} value={date}>{date}</option>)}</select></label><details className="mt-2 text-xs text-slate-400"><summary className="cursor-pointer">Use a custom date</summary><label className="mt-2">Expiration · all legs<input type="date" min={today} max={`${Number(today.slice(0, 4)) + 5}${today.slice(4)}`} value={expiration} onChange={e => { if (e.target.value >= today && e.target.value <= `${Number(today.slice(0, 4)) + 5}${today.slice(4)}` && /^\d{4}-\d{2}-\d{2}$/.test(e.target.value)) { resetDataContext(); setExpiration(e.target.value); void loadMarket(symbol, e.target.value); } }} /></label></details>{dateCursor ? <button className="options-button mt-2" disabled={!!busy} onClick={() => request("dates", async (current, signal) => { const result = await loadOptionsData(() => getCalculatorExpirations(symbol, spot, dateCursor, signal), signal, () => setNotice("Waiting for the free allowance to load more dates…")); if (current()) { setListedDates(dates => [...new Set([...dates, ...result.expirations.filter(date => date >= today)])].sort()); setDateCursor(result.next_cursor ?? null); } })}>More listed dates</button> : null}</div>
        <div className="flex items-end"><button className="options-button w-full" disabled={!!busy} onClick={() => { resetDataContext(); void loadMarket(symbol); }}>{busy === "market" ? "Loading market data…" : "Reload market data"}</button></div>
      </div>
      <p role="status" className="mt-3 text-xs text-emerald-200">{busy === "market" ? "Loading listed dates, strikes, and starting prices…" : priceStatus}{hidden ? " · Price loading paused while this tab is hidden." : ""}</p>
      <p className="mt-3 text-xs text-slate-500">{spotSource} · {days} calendar days to expiration · no live quotes</p>
      <p className="mt-2 text-xs text-slate-400">Chart and results update when you Tab or click away from a number. Press Enter to apply an edit.</p>
      <div className="mt-5 grid grid-cols-2 gap-2 lg:grid-cols-6">{outlooks.map(({ id, label, icon: Icon }) => <button className="options-button flex min-h-16 flex-col items-start justify-center gap-2 text-left" key={id} aria-pressed={outlook === id} onClick={() => { setOutlook(id); setMove(id === "bearish" ? -10 : id === "bullish" || id === "move" ? 10 : 0); if (id === "vol-up") setScenarioVol(volatility + 10); else if (id === "vol-down") setScenarioVol(Math.max(0, volatility - 10)); else setScenarioVol(volatility); }}><Icon size={17} />{label}</button>)}</div>
      <div className="mt-5 grid gap-3 md:grid-cols-3">{templates.map(t => <button key={t.id} className="options-button p-4 text-left" aria-pressed={strategy === t.id} onClick={() => chooseStrategy(t.id)}><span className="block text-sm font-semibold">{t.name}</span><span className="mt-2 block text-xs font-normal leading-5 text-slate-400">{t.description}</span><span className="mt-3 block text-xs text-emerald-300">Build strategy →</span></button>)}</div>
      <p className="mt-3 text-xs text-slate-500">Examples matching your outlook, not recommendations. Selecting a strategy uses listed strikes and cached closes when available; remaining modeled entries load EOD prices automatically.</p>
    </section>

    <div className="grid items-start gap-5 xl:grid-cols-[340px_minmax(0,1fr)]">
      <div className="space-y-5">
        <section className="options-panel"><h2>02 <span className="ml-2">Set the assumptions</span></h2><div className="mt-4 grid grid-cols-2 gap-3">
          <Field label="Entry volatility (%)" value={volatility} max={500} onChange={setVolatility} />
          <Field label="Risk-free rate (%)" value={rate} min={-10} max={30} onChange={setRate} />
          <Field label="Dividend yield (%)" value={dividend} max={50} onChange={setDividend} />
          <Field label="Entry fee / contract ($)" value={position.fee} max={100} onChange={fee => setPosition(p => ({ ...p, fee }))} />
        </div><p className="mt-3 text-xs leading-5 text-slate-500">Volatility is an assumption, not market implied volatility. Entry prices stay fixed when you change the scenario.</p>
        <button className="options-button mt-3 w-full" onClick={() => setPosition(p => ({ ...p, legs: p.legs.map(l => l.source === "Modeled entry" ? { ...l, premium: Math.round(optionValue(l.kind, l.strike, model) * 100) / 100 } : l) }))}>Reprice modeled entries</button></section>
        <section className="options-panel"><h2>03 <span className="ml-2">Explore a scenario</span></h2>
          <div className="mt-4 flex items-end gap-2"><div className="flex-1"><Field label={`Expected price change (${moveUnit})`} value={move} min={moveUnit === "%" ? -100 : -spot} max={moveUnit === "%" ? 1000 : 1000000} onChange={setMove} /></div><button className="options-button mb-1" onClick={() => { setMove(moveUnit === "%" ? spot * move / 100 : move / spot * 100); setMoveUnit(moveUnit === "%" ? "$" : "%"); }} aria-label="Switch forecast between percent and dollars">{moveUnit === "%" ? "Use $" : "Use %"}</button></div>
          <p className="mt-3 text-sm">Target price <strong className="float-right text-white">{money(target)}</strong></p>
          <label className="mt-5">Evaluate after {horizon} of {days} days<input className="mt-3 w-full accent-emerald-300" type="range" min={0} max={days} value={horizon} onChange={e => setElapsed(Number(e.target.value))} /></label>
          <div className="mt-3"><Field label="Scenario volatility (%)" value={scenarioVol} max={500} onChange={setScenarioVol} /></div>
          <div className="mt-5 rounded-xl border border-emerald-300/15 bg-emerald-300/5 p-4"><p className="text-xs text-slate-400">{remaining ? "Modeled P/L at your target" : "Expiration P/L at your target"}</p><p className={`mt-2 text-3xl font-semibold tabular-nums ${pnlClass(targetPnl)}`}>{money(targetPnl)}</p><p className="mt-2 text-xs text-slate-500">{remaining} days remaining · includes entry fees</p></div>
        </section>
      </div>
      <section className="options-panel">
        <div className="flex flex-wrap items-center justify-between gap-3"><div><h2>Your strategy, at a glance</h2><p className="mt-1 text-xs text-slate-500">{strategyTemplates.find(t => t.id === strategy)?.name ?? "Custom strategy"} · {expiration}</p></div><div className="flex gap-1" role="group" aria-label="Results view">{["Chart", "Table", "Heatmap"].map(v => <button className="options-button" key={v} aria-pressed={view === v} onClick={() => setView(v)}>{v}</button>)}</div></div>
        <div className="mt-5 flex flex-wrap gap-4 text-xs"><span className="text-emerald-300">● At expiration</span><span className="text-sky-300">┄ Your scenario · day {horizon}</span><span className="text-slate-500">P/L in USD · stock price on horizontal axis</span></div>
        {view === "Chart" ? <div ref={chartRef} className="mt-3 min-w-0"><WalnutChartContainer label="Options profit and loss"><WalnutLineChart draggableTooltip alignEdgeLabels width={chartWidth} height={360} data={plot} series={[{ key: "zero", label: "Break-even", color: "#475569", dashed: true, values: plot.map(() => 0) }, { key: "expiration", label: "At expiration", color: "#6ee7b7", values: plot.map(p => p.expiry) }, { key: "scenario", label: "Scenario estimate", color: "#7dd3fc", dashed: true, values: plot.map(p => p.scenario) }]} ariaLabel="Options profit or loss by underlying stock price" formatValue={compact} renderTooltip={i => <div className="text-xs"><p className="font-semibold">Stock {plot[i].label}</p><p className="mt-1 text-emerald-300">Expiration: {money(plot[i].expiry)}</p><p className="text-sky-300">Scenario: {money(plot[i].scenario)}</p></div>} /></WalnutChartContainer></div> : view === "Table" ? <div className="options-reveal mt-4 max-h-[380px] overflow-auto"><table className="options-table"><caption className="sr-only">Profit or loss by stock price</caption><thead><tr><th>Stock price</th><th>At expiration</th><th>Scenario · day {horizon}</th></tr></thead><tbody>{plot.filter((_, i) => i % 4 === 0).map(p => <tr key={p.price}><td>{money(p.price)}</td><td className={pnlClass(p.expiry)}>{money(p.expiry)}</td><td className={pnlClass(p.scenario)}>{money(p.scenario)}</td></tr>)}</tbody></table></div> : <div className="options-reveal mt-4 overflow-auto"><table className="options-table"><caption className="mb-3 text-left text-xs text-slate-400">Price × time · estimated P/L at {scenarioVol}% volatility. Columns show days elapsed.</caption><thead><tr><th>Stock price</th>{Array.from({ length: 6 }, (_, i) => <th key={i}>Day {Math.round(days * i / 5)}</th>)}</tr></thead><tbody>{Array.from({ length: 9 }, (_, i) => spot * (1.2 - i * .05)).map(price => <tr key={price}><td>{money(price)}</td>{Array.from({ length: 6 }, (_, i) => { const elapsedDay = Math.round(days * i / 5), value = profitAt(position, { ...scenario, spot: price, days: days - elapsedDay }); return <td key={i} style={{ background: value >= 0 ? `rgba(16,185,129,${Math.min(.4, .06 + Math.abs(value) / Math.max(100, risk.maxLoss === Infinity ? 10000 : risk.maxLoss) * .2)})` : "rgba(244,63,94,.15)" }} className={pnlClass(value)}>{money(value)}</td>; })}</tr>)}</tbody></table></div>}
        <div className="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-white/10 pt-4"><p className="text-xs text-slate-500">Explore beyond the chart: maximum risk uses all prices from $0 to infinity.</p><button className="options-button flex items-center gap-2" onClick={exportCsv}><Download size={13} />Export scenarios</button></div>
        <div className="mt-5 grid grid-cols-2 gap-3 lg:grid-cols-4">{Object.entries(greeks).map(([name, value]) => <div key={name} className="rounded-lg bg-slate-950/60 p-3"><p className="text-xs capitalize text-slate-400">{name}</p><p className="mt-1 text-lg font-medium tabular-nums text-slate-100">{value.toFixed(2)}</p><p className="mt-1 text-[10px] text-slate-500">{name === "delta" ? "Share equivalent" : name === "gamma" ? "Delta change / $1" : name === "theta" ? "P/L / day forward" : "P/L / 1 vol point"}</p></div>)}</div><p className="mt-2 text-xs text-slate-500">Estimated position Greeks at entry assumptions. At expiration, sensitivities at strikes are discontinuous.</p>
      </section>
    </div>
    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4"><Metric label={debit >= 0 ? "Net entry debit" : "Net entry credit"} value={money(Math.abs(debit))} note="Includes stock and entry commissions; not margin required." /><Metric label="Maximum profit at expiration" value={money(risk.maxProfit)} note="All positions held to the same expiration." tone="text-emerald-300" /><Metric label="Maximum loss at expiration" value={money(risk.maxLoss)} note="Includes entry commissions. No exit or assignment fees." tone="text-rose-300" /><Metric label="Break-even stock price" value={risk.breakEvens.length ? risk.breakEvens.map(money).join(" / ") : "None"} note="At expiration, after entry fees; flat zero-P/L regions may extend beyond these boundaries." /></div>

    <section className="options-panel"><div className="flex flex-wrap items-center justify-between gap-3"><h2>Edit your position</h2><button className="options-button flex items-center gap-2" disabled={position.legs.length >= 6} onClick={() => addContract()}><Plus size={14} />Add option leg</button></div>
      <p className="mt-2 text-xs text-slate-500">Premiums are per share. Each contract represents 100 shares. Up to six legs, one underlying and one expiration.</p>
      <div className="mt-4 space-y-3">{position.legs.map((leg, index) => <div key={leg.id} className="rounded-xl border border-white/10 bg-slate-950/40 p-3"><div className="grid grid-cols-2 items-end gap-3 sm:grid-cols-3 lg:grid-cols-[.8fr_.8fr_1fr_1fr_.7fr_auto]">
        <label>Leg {index + 1} · action<select value={leg.side} onChange={e => updateLeg(leg.id, { side: Number(e.target.value) as 1 | -1 })}><option value={1}>Buy</option><option value={-1}>Sell</option></select></label>
        <label>Leg {index + 1} · type<select value={leg.kind} onChange={e => updateLeg(leg.id, { kind: e.target.value as "call" | "put", contract: undefined, source: "Manual entry · review price" })}><option value="call">Call</option><option value="put">Put</option></select></label>
        <div>{chain?.contracts.length ? <label>Leg {index + 1} listed strike<select value={leg.contract ?? "manual"} onChange={e => { const contract = chain.contracts.find(c => c.ticker === e.target.value); if (contract) updateLeg(leg.id, { strike: contract.strike, contract: contract.ticker, premium: contract.close?.price ?? Math.round(optionValue(contract.kind, contract.strike, model) * 100) / 100, source: contract.close ? optionCloseLabel(contract.close) : "Modeled entry" }); else updateLeg(leg.id, { contract: undefined, source: "Manual entry · review price" }); }}><option value="manual">Custom strike</option>{chain.contracts.filter(c => c.kind === leg.kind).map(c => <option key={c.ticker} value={c.ticker}>{money(c.strike)}</option>)}</select></label> : null}{!leg.contract ? <Field label={`Leg ${index + 1} strike ($)`} value={leg.strike} min={.01} onChange={strike => updateLeg(leg.id, { strike, contract: undefined, source: "Manual entry · review price" })} /> : null}</div>
        <Field label={`Leg ${index + 1} premium ($/share)`} value={leg.premium} onChange={premium => updateLeg(leg.id, { premium, source: "Manual entry" })} />
        <Field label={`Leg ${index + 1} contracts`} value={leg.quantity} min={1} max={1000} step={1} onChange={quantity => updateLeg(leg.id, { quantity })} />
        <button className="options-button mb-1 justify-self-start" aria-label={`Remove leg ${index + 1}`} onClick={() => { setStrategy("custom"); setPosition(p => ({ ...p, legs: p.legs.filter(l => l.id !== leg.id) })); }}><Trash2 size={15} /></button>
      </div><div className="mt-3 flex flex-wrap items-center gap-3"><span className="options-tag">{leg.source}{leg.source === "Modeled entry" && chain?.contracts.find(c => c.ticker === leg.contract)?.no_trade ? " · no previous trade" : ""}</span><span className="text-xs text-slate-500">{leg.contract ?? "Custom strike · availability not verified"}</span>{leg.contract ? <button className="options-button" disabled={!!busy} onClick={() => request(leg.id, async current => { const data = await getCalculatorClose(leg.contract!); if (current()) setPosition(p => ({ ...p, legs: p.legs.map(l => l.id === leg.id && l.contract === data.ticker ? { ...l, premium: data.price, source: optionCloseLabel(data) } : l) })); })}>{busy === leg.id ? "Loading…" : "Use EOD premium"}</button> : null}</div></div>)}</div>
      <div className="mt-4 grid max-w-lg grid-cols-2 gap-3"><Field label="Underlying shares held" value={position.shares} step={1} max={1000000} onChange={shares => { setStrategy("custom"); setPosition(p => ({ ...p, shares })); }} /><Field label="Stock entry price ($)" value={position.stockEntry} min={.01} onChange={stockEntry => setPosition(p => ({ ...p, stockEntry }))} /></div>
    </section>
    <section className="options-panel"><div className="flex flex-wrap items-center justify-between gap-3"><div><h2>Find listed contracts</h2><p className="mt-2 text-xs text-slate-500">Massive reference data · {symbol || "Choose a ticker"} · {expiration}</p></div><button className="options-button options-primary" disabled={!!busy || !/^[A-Z][A-Z0-9.\-]{0,9}$/.test(symbol)} onClick={() => request("chain", async current => { const result = await getCalculatorOptionContracts(symbol, expiration); if (current()) { setChain(result); setPricePaused(false); setFullPreload(result.price_provider === "alpaca"); nextPriceAt.current = 0; } })}>{busy === "chain" ? "Finding contracts…" : "Load contracts for this expiration"}</button></div>
      <p className="mt-3 text-xs leading-5 text-slate-400">Listed dates and strikes load automatically. Dated EOD prices are actual historical trade closes (some may be older than the previous session); estimates use your assumptions. Selected legs load first. When batch data is available, the full chain preloads automatically, starting near the stock price. No bid/ask, live prices, or market IV.</p>
      {chain ? <div className="mt-3 flex flex-wrap items-center gap-3"><button className="options-button" aria-pressed={fullPreload && !pricePaused} disabled={!!busy} onClick={() => { if (fullPreload && !pricePaused) { setPricePaused(true); setFullPreload(false); setPriceStatus("Price preload paused."); } else { setPricePaused(false); setFullPreload(true); } }}>{fullPreload && !pricePaused ? "Pause price preload" : "Preload all chain prices"}</button>{pricePaused ? <button className="options-button" onClick={() => { setPricePaused(false); setRetry(n => n + 1); }}>Resume selected prices</button> : null}<span className="text-xs text-slate-400">{chain.contracts.filter(c => c.close).length} / {chain.contracts.length} EOD prices · {chain.contracts.filter(c => c.no_trade).length} without a recent historical trade</span><p className="w-full text-xs text-slate-500">{chain.price_provider === "alpaca" ? `Alpaca historical closes · up to 100 contracts per batch · ${Math.ceil(chain.contracts.filter(c => !c.close && !c.no_trade).length / 100)} batches remaining. Contracts without trades in the past seven days remain labeled estimates.` : `Massive historical closes · about ${Math.ceil(chain.contracts.filter(c => !c.close && !c.no_trade).length * 13 / 60)} minutes to preload uncached prices at the free-plan pace.`} Loading pauses while this tab is hidden. Shared allowances can add a wait.</p>{chain.next_cursor ? <button className="options-button" disabled={!!busy} onClick={() => request("chain-page", async (current, signal) => { const result = await loadOptionsData(() => getCalculatorOptionContracts(symbol, expiration, chain.next_cursor!, signal), signal, () => setNotice("Waiting to load the next contracts page…")); if (current()) setChain(c => c ? { ...result, excluded: c.excluded + result.excluded, contracts: [...new Map([...c.contracts, ...result.contracts].map(item => [item.ticker, item])).values()] } : result); })}>Load remaining contracts</button> : null}</div> : null}
      {busy === "chain" ? <WalnutChartSkeleton label="Loading option contracts" heightClassName="h-48" /> : chain ? <div className="options-reveal mt-4 max-h-[520px] overflow-auto"><button className="options-button mb-3" aria-pressed={allStrikes} onClick={() => setAllStrikes(v => !v)}>{allStrikes ? "Show nearest 21 strikes" : "Show all strikes"}</button><p className="mb-2 text-xs text-slate-500">{chain.contracts.length} contracts found · {allStrikes ? "showing all returned strikes" : "showing up to 21 strikes nearest your stock price"}.{chain.excluded ? ` ${chain.excluded} adjusted or nonstandard contracts excluded.` : ""}{chain.truncated ? " More contracts are available; load the next page above." : ""}</p>{rows.length ? <table className="options-table"><thead><tr><th>Call · price/share</th><th>Add call</th><th>Strike</th><th>Add put</th><th>Put · price/share</th></tr></thead><tbody>{rows.map(strike => { const call = chain.contracts.find(c => c.strike === strike && c.kind === "call"), put = chain.contracts.find(c => c.strike === strike && c.kind === "put"); return <tr key={strike}><td className="text-emerald-200">{call ? <>{money(call.close?.price ?? optionValue("call", strike, model))}<span className="mt-1 block text-[10px] text-slate-400">{call.close ? optionCloseLabel(call.close) : call.no_trade ? "Estimate · no previous trade" : "Estimate · close not loaded"}</span></> : "Not listed"}</td><td><button className="options-button" disabled={!call || position.legs.length >= 6} onClick={() => addContract(call)} aria-label={`Add ${strike} call`}>+ Call</button></td><td className="bg-white/5 font-semibold text-white">{money(strike)}</td><td><button className="options-button" disabled={!put || position.legs.length >= 6} onClick={() => addContract(put)} aria-label={`Add ${strike} put`}>+ Put</button></td><td className="text-sky-200">{put ? <>{money(put.close?.price ?? optionValue("put", strike, model))}<span className="mt-1 block text-[10px] text-slate-400">{put.close ? optionCloseLabel(put.close) : put.no_trade ? "Estimate · no previous trade" : "Estimate · close not loaded"}</span></> : "Not listed"}</td></tr>; })}</tbody></table> : <p className="py-5 text-sm text-slate-400">No standard contracts found on this date. Try another expiration, or build with manual strikes.</p>}</div> : null}
      <p role="status" aria-live="polite" className={`mt-3 text-sm ${notice ? "rounded-lg border border-amber-300/20 bg-amber-300/5 p-3 text-amber-200" : "text-slate-500"}`}>{notice || (busy ? "Loading market data…" : "Listed dates and strikes share Massive’s 5-request-per-minute allowance. Historical prices are cached for one hour. Manual entry prices are never overwritten by preload.")}</p>
    </section>
    <details className="options-panel text-sm leading-6"><summary className="cursor-pointer font-semibold text-white">How the calculations work</summary><div className="mt-4 space-y-3 text-slate-400"><p>Expiration P/L = Σ [direction × contracts × 100 × (intrinsic value − entry premium)] + shares × (final stock price − stock entry price) − entry fees. Direction is +1 for buys and −1 for sells. Call intrinsic value = max(stock − strike, 0); put intrinsic value = max(strike − stock, 0).</p><p>Before expiration, replace intrinsic value with the European Black–Scholes value, using calendar days / 365, annual volatility, a continuously compounded risk-free rate, and a continuous dividend yield. Each leg uses the same volatility assumption. Greeks are numerical sensitivities of the full position; theta is a one-day forward change.</p><p>Maximum profit, loss, and break-even prices use the piecewise-linear expiration payoff across all nonnegative stock prices. Premiums stay fixed until edited, reloaded, or explicitly repriced. Only entry commissions are included.</p><p>US equity options are usually American-style: early exercise and assignment, dividends received on shares, financing, slippage, exit fees, taxes, and broker margin are not modeled. EOD closes can be stale or untradeable. This tool explores hypothetical outcomes; it does not place trades or estimate your probability of profit.</p><p><a className="text-emerald-200 underline" href="https://www.optionseducation.org/advancedconcepts/black-scholes-formula" target="_blank" rel="noreferrer">Options Industry Council: pricing models</a> · <a className="text-emerald-200 underline" href="https://massive.com/docs/rest/options/contracts/all-contracts" target="_blank" rel="noreferrer">Massive contract reference</a></p></div></details>
  </div>;
}
