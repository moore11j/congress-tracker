"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useRef, useState, useTransition, type FormEvent, type ReactNode } from "react";
import { ArrowUp, ArrowUpRight, BarChart3, ChevronDown, Menu, MessageSquare, Plus, Search, X } from "lucide-react";
import { ResearchCreditBalance } from "@/components/chat/ResearchCreditBalance";
import { AccountNav } from "@/components/auth/AccountNav";
import { DiscoveryStocks } from "@/components/discover/DiscoveryStocks";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { searchSuggest, type SearchSuggestResult } from "@/lib/api";
import { chatIntent, chatResearchHref, type ChatSection } from "@/lib/chatNavigation";
import { tickerHref, tickerRouteSymbol } from "@/lib/ticker";
import { ResearchCreditQuote } from "@/components/chat/ResearchCreditQuote";
import type { ResearchMode } from "@/lib/researchCredits";

type Turn = { role: "user" | "walnut"; text: string };
const tools = [
  ["Screener", "/screener"], ["Compare companies", "/compare"], ["Signals", "/signals"],
  ["Market Maps", "/market-pressure"], ["Strategies", "/strategies"], ["Backtesting", "/backtesting"],
  ["Signal Mixer", "/signal-mixer"], ["Retirement calculator", "/retirement-calculator"], ["Options calculator", "/options-calculator"],
] as const;
const sources = [
  ["Congress", "/members"], ["Insiders", "/insiders"], ["Institutions", "/institutions"],
  ["Government contracts", "/government-contracts"], ["Departments", "/departments"], ["Research briefs", "/research"],
] as const;

export function WalnutChatWorkspace({ symbol, rankings, children }: { symbol: string | null; rankings: boolean; children: ReactNode }) {
  const router = useRouter();
  const [input, setInput] = useState("");
  const [mode, setMode] = useState<ResearchMode>("browse");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [choices, setChoices] = useState<SearchSuggestResult[]>([]);
  const [recent, setRecent] = useState<string[]>([]);
  const [searching, setSearching] = useState(false);
  const [pending, startTransition] = useTransition();
  const controller = useRef<AbortController | null>(null);
  const requestId = useRef(0);
  const drawer = useRef<HTMLDialogElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const replyAnchor = useRef<HTMLDivElement>(null);
  const hasResearch = Boolean(symbol || rankings);
  const empty = !hasResearch && turns.length === 0;

  useEffect(() => () => { requestId.current++; controller.current?.abort(); }, []);
  useEffect(() => { if (symbol) setRecent(previous => [symbol, ...previous.filter(value => value !== symbol)].slice(0, 8)); }, [symbol]);
  useEffect(() => { if (turns.length) replyAnchor.current?.scrollIntoView({ block: "start" }); }, [turns.length]);

  function say(text: string) { setTurns(previous => [...previous, { role: "walnut", text }]); }
  function navigateTicker(value: string) {
    const href = chatResearchHref(value);
    if (!href) return;
    setChoices([]);
    drawer.current?.close();
    say(`Opening ${tickerRouteSymbol(value)} research below. You can explore its chart, financials, news and existing evidence here.`);
    startTransition(() => router.push(href, { scroll: false }));
  }
  function showRankings() {
    drawer.current?.close();
    setChoices([]);
    say("Here are Walnut’s current ranked stocks. Your account’s existing access applies.");
    startTransition(() => router.push("/ask?view=rankings", { scroll: false }));
  }
  function showSection(section: ChatSection, label: string) {
    say(`Showing ${label.toLowerCase()} for ${symbol} in the research panel below.`);
    window.location.hash = section;
    // Native hashchange is consumed by the existing ticker tab component.
    document.querySelector("[data-chat-research]")?.scrollIntoView({ block: "start" });
  }
  async function submitText(raw: string, navigationOnly = false) {
    const value = raw.trim().slice(0, 600);
    if (!value || searching || pending) return;
    if (mode !== "browse" && !navigationOnly) return; // No paid request without a connected, authorized AI runner.
    if (navigationOnly) setMode("browse");
    setInput(""); setChoices([]);
    setTurns(previous => [...previous, { role: "user", text: value }]);
    const intent = chatIntent(value, Boolean(symbol));
    if (intent.kind === "rankings") { showRankings(); return; }
    if (intent.kind === "section") { showSection(intent.section, intent.label); return; }
    if (!intent.query || intent.query.length > 120) {
      say("This preview can open company research and its sections. Open-ended AI answers are not available yet. Try a company name or ticker."); return;
    }
    const id = ++requestId.current;
    const active = new AbortController(); controller.current?.abort(); controller.current = active;
    setSearching(true);
    const deadline = setTimeout(() => active.abort(), 12_000);
    try {
      const response = await searchSuggest(intent.query, 8, { signal: active.signal, source: "WalnutChatSearch", sameOrigin: true });
      if (requestId.current !== id) return;
      if (active.signal.aborted) throw new Error("Search timed out");
      const seen = new Set<string>();
      const matches = response.items.filter(item => {
        const key = item.kind === "ticker" ? tickerRouteSymbol(item.symbol) : null;
        if (!key || seen.has(key)) return false;
        seen.add(key); return true;
      });
      const exact = matches.filter(item => item.symbol?.toLowerCase() === intent.query.replace(/^\$/, "").toLowerCase() || item.label.toLowerCase() === intent.query.toLowerCase());
      if (exact.length === 1) navigateTicker(exact[0].symbol!);
      else if (matches.length) { setChoices(matches); say("Choose the company you want to research."); }
      else say("I couldn’t match that to a company. Try its name or ticker. This preview supports research navigation; open-ended AI answers are still to come.");
    } catch {
      if (requestId.current === id) say("Company search is temporarily unavailable. Please try again. Your current research is still here.");
    } finally {
      clearTimeout(deadline);
      if (requestId.current === id) { setSearching(false); controller.current = null; }
    }
  }
  function newChat() {
    requestId.current++; controller.current?.abort(); controller.current = null;
    setInput(""); setMode("browse"); setTurns([]); setChoices([]); setSearching(false); drawer.current?.close();
    startTransition(() => router.push("/ask", { scroll: false }));
    inputRef.current?.focus();
  }
  function submit(event: FormEvent) { event.preventDefault(); void submitText(input); }

  const sidebar = <>
    <Link href="/ask" onClick={event => { if (!event.metaKey && !event.ctrlKey) { event.preventDefault(); newChat(); } }} className="flex items-center gap-2.5 px-2 py-2 text-lg font-semibold tracking-tight text-white">
      <WalnutBrandMark className="flex h-8 w-8 items-center justify-center rounded-lg border border-emerald-300/20 bg-emerald-300/5" svgClassName="h-5 w-5" />Walnut
    </Link>
    <button type="button" onClick={newChat} className="mt-6 flex w-full items-center gap-3 rounded-lg border border-white/10 px-3 py-3 text-sm text-slate-200 transition hover:bg-white/5"><Plus className="h-4 w-4" />New research</button>
    <nav aria-label="Workspace navigation" className="mt-5 space-y-1 text-sm">
      <button type="button" onClick={() => { drawer.current?.close(); inputRef.current?.focus(); }} className="flex w-full items-center gap-3 rounded-lg bg-white/[0.06] px-3 py-2.5 text-white"><MessageSquare className="h-4 w-4 text-emerald-300" />Ask Walnut</button>
      <Link href="/my-stocks" prefetch={false} className="flex items-center gap-3 rounded-lg px-3 py-2.5 text-slate-400 hover:bg-white/5 hover:text-white"><Search className="h-4 w-4" />My Stocks</Link>
      <button type="button" onClick={showRankings} className="flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-slate-400 hover:bg-white/5 hover:text-white"><BarChart3 className="h-4 w-4" />Leaderboards</button>
      {process.env.NEXT_PUBLIC_OUTCOMES_LEDGER_ENABLED !== "0" && <Link href="/outcomes" prefetch={false} className="block rounded-lg px-3 py-2.5 text-slate-400 hover:bg-white/5 hover:text-white">Outcomes</Link>}
      <Link href="/?mode=all" prefetch={false} className="block rounded-lg px-3 py-2.5 text-slate-400 hover:bg-white/5 hover:text-white">Activity Feed</Link>
      <Link href="/insights" prefetch={false} className="block rounded-lg px-3 py-2.5 text-slate-400 hover:bg-white/5 hover:text-white">Insights</Link>
      <details className="group"><summary className="flex cursor-pointer list-none items-center justify-between rounded-lg px-3 py-2.5 text-slate-400 hover:bg-white/5 hover:text-white">Tools <ChevronDown className="h-3.5 w-3.5 transition group-open:rotate-180" /></summary><div className="ml-3 border-l border-white/10 pl-3">{tools.filter(([, href]) => href !== "/strategies" || process.env.NEXT_PUBLIC_STRATEGIES_ENABLED !== "0").map(([label, href]) => <Link key={href} href={href} prefetch={false} className="block py-2.5 text-xs text-slate-400 hover:text-white">{label}</Link>)}</div></details>
      <details className="group"><summary className="flex cursor-pointer list-none items-center justify-between rounded-lg px-3 py-2.5 text-slate-400 hover:bg-white/5 hover:text-white">Sources & research <ChevronDown className="h-3.5 w-3.5 transition group-open:rotate-180" /></summary><div className="ml-3 border-l border-white/10 pl-3">{sources.map(([label, href]) => <Link key={href} href={href} prefetch={false} className="block py-2.5 text-xs text-slate-400 hover:text-white">{label}</Link>)}</div></details>
    </nav>
    {!!recent.length && <section className="mt-8"><h2 className="px-3 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">In this session</h2><div className="mt-2">{recent.map(value => <button key={value} type="button" onClick={() => navigateTicker(value)} className="block w-full truncate rounded-lg px-3 py-2.5 text-left text-sm text-slate-400 hover:bg-white/5 hover:text-white">{value} research</button>)}</div></section>}
    <div className="mt-auto space-y-3 border-t border-white/10 px-2 pb-1 pt-5 text-xs"><Link href="/ask/credits" className="flex items-center justify-between text-slate-300">Plans & credits <ArrowUpRight className="h-3.5 w-3.5" /></Link><div className="flex gap-4 text-slate-500"><Link href="/privacy">Privacy</Link><Link href="/terms">Terms</Link></div></div>
  </>;

  return <div className="flex h-[100dvh] min-h-0 min-w-0 bg-[#151c23] text-slate-200">
    <aside className="hidden w-[244px] shrink-0 flex-col gap-1 overflow-y-auto border-r border-white/[0.06] bg-[#0d141b] p-4 lg:flex">{sidebar}</aside>
    <dialog ref={drawer} aria-label="Workspace menu" className="fixed inset-y-0 left-0 right-auto m-0 h-[100dvh] max-h-none w-[280px] max-w-[85vw] border-r border-white/10 bg-[#0d141b] p-4 text-slate-200 backdrop:bg-black/60"><button type="button" aria-label="Close workspace menu" onClick={() => drawer.current?.close()} className="absolute right-3 top-4 rounded-md p-2 hover:bg-white/10"><X className="h-5 w-5" /></button><div className="flex min-h-full flex-col pb-2">{sidebar}</div></dialog>
    <div className="flex min-w-0 flex-1 flex-col">
      <header className="flex min-h-16 shrink-0 flex-wrap items-center justify-between gap-2 px-4 py-2 sm:px-6">
        <div className="flex min-w-0 items-center gap-3"><button type="button" aria-label="Open workspace menu" onClick={() => drawer.current?.showModal()} className="-ml-2 rounded-md p-2 text-slate-400 lg:hidden"><Menu className="h-5 w-5" /></button><span className="truncate text-sm font-medium text-slate-300">Ask Walnut</span><span className="rounded border border-white/10 px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-slate-500">Preview</span></div>
        <div className="flex min-w-0 items-center gap-2"><ResearchCreditBalance compact /><AccountNav /></div>
      </header>
      <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-4 sm:px-6">
        <div className={`mx-auto flex min-h-full w-full flex-col ${hasResearch ? "max-w-6xl" : "max-w-3xl"}`}>
          {empty ? <div className="my-auto py-10 sm:py-16">
            <WalnutBrandMark className="mx-auto flex h-12 w-12 items-center justify-center rounded-2xl border border-emerald-300/20 bg-emerald-300/[0.04]" svgClassName="h-7 w-7" />
            <h1 className="mt-6 text-center text-[28px] font-medium tracking-tight text-white sm:text-4xl">What are we researching?</h1>
            <p className="mx-auto mt-3 max-w-md text-center text-sm leading-6 text-slate-400">Start with a company. Explore the evidence.<br className="hidden sm:block" /> Keep your next question in the same conversation.</p>
            <div className="mx-auto mt-9 grid max-w-xl gap-2 sm:grid-cols-2">{["Research NBIS", "Research NVDA", "Show ranked stocks", "Research Microsoft"].map(prompt => <button key={prompt} type="button" onClick={() => void submitText(prompt, true)} disabled={searching || pending} className="flex items-center justify-between rounded-xl border border-white/10 px-4 py-4 text-left text-sm text-slate-300 transition hover:border-white/20 hover:bg-white/[0.035] disabled:opacity-50">{prompt}<ArrowUpRight className="h-4 w-4 text-slate-500" /></button>)}</div>
          </div> : <div className="py-5 sm:py-8">
            <div className="mx-auto max-w-3xl space-y-6">{turns.map((turn, index) => <div key={index} className={turn.role === "user" ? "ml-auto w-fit max-w-[90%] rounded-2xl bg-white/[0.07] px-4 py-3 text-sm leading-6 text-white" : "flex gap-3 text-sm leading-6 text-slate-300"}>{turn.role === "walnut" && <WalnutBrandMark className="mt-1 flex h-6 w-6 shrink-0 items-center justify-center text-emerald-300" svgClassName="h-5 w-5" />}<p className="min-w-0 whitespace-pre-wrap break-words">{turn.text}</p></div>)}</div>
            <div ref={replyAnchor} />
            {!!choices.length && <div className="mx-auto mt-5 max-w-3xl divide-y divide-white/10 rounded-xl border border-white/10">{choices.map(item => <button key={item.symbol} type="button" onClick={() => navigateTicker(item.symbol!)} className="flex w-full items-center justify-between gap-4 px-4 py-4 text-left hover:bg-white/5"><span className="min-w-0"><span className="block text-sm font-semibold text-white">{item.symbol}</span><span className="block break-words text-xs text-slate-400">{item.label}{item.subtitle ? ` · ${item.subtitle}` : ""}</span></span><ArrowUpRight className="h-4 w-4 shrink-0 text-slate-500" /></button>)}</div>}
            {(searching || pending) && <p role="status" className="mx-auto mt-6 max-w-3xl text-sm text-slate-400">{searching ? "Finding the company…" : "Opening research…"}</p>}
            {hasResearch && <section data-chat-research aria-label={symbol ? `${symbol} research in conversation` : "Ranked stocks in conversation"} className="mt-6 min-w-0 overflow-hidden rounded-xl border border-white/10 bg-[#08131f]">
              <div className="flex flex-wrap items-center justify-between gap-2 border-b border-white/10 px-4 py-3"><span className="text-xs font-medium text-slate-400">{symbol ? `${symbol} · Ticker research` : "Walnut · Ranked stocks"}</span><Link href={symbol ? tickerHref(symbol)! : "/leaderboards"} prefetch={false} className="inline-flex items-center gap-1 text-xs text-emerald-200">Open full page <ArrowUpRight className="h-3.5 w-3.5" /></Link></div>
              <div className="min-w-0 p-3 sm:p-5">{rankings ? <DiscoveryStocks onResearch={navigateTicker} /> : children}</div>
            </section>}
            {symbol && <div className="mt-4 flex flex-wrap gap-2">{["Show financials", "Show the chart", "Show news", "What are the risks?"].map(prompt => <button key={prompt} type="button" onClick={() => void submitText(prompt, true)} disabled={searching || pending} className="rounded-full border border-white/10 px-3 py-2 text-xs text-slate-400 hover:border-emerald-300/30 hover:text-emerald-200 disabled:opacity-50">{prompt}</button>)}</div>}
          </div>}
        </div>
      </div>
      <footer className="shrink-0 px-4 pb-3 pt-3 sm:px-6 sm:pb-4">
        <form onSubmit={submit} className="mx-auto max-w-3xl rounded-2xl border border-white/15 bg-[#202a33] p-3 shadow-lg shadow-black/10 focus-within:border-emerald-300/30">
          <ResearchCreditQuote mode={mode} onModeChange={setMode} />
          <label htmlFor="walnut-question" className="sr-only">Ask Walnut or search a company</label>
          <textarea ref={inputRef} id="walnut-question" value={input} onChange={event => setInput(event.target.value)} onKeyDown={event => { if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); void submitText(input); } }} maxLength={600} rows={2} placeholder={symbol ? `Ask about ${symbol}, or research another company…` : "Ask Walnut or search a company…"} className="block max-h-40 min-h-[56px] w-full resize-none bg-transparent px-1 py-1 text-base text-white placeholder:text-slate-500 focus:outline-none" />
          <div className="mt-1 flex items-center justify-between gap-3"><span className="truncate text-[11px] text-slate-500">{symbol ? `Researching ${symbol}` : "Company research & tools"}</span><button type="submit" aria-label="Send question" disabled={mode !== "browse" || !input.trim() || searching || pending} className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-emerald-300 text-slate-950 transition hover:bg-emerald-200 disabled:bg-white/10 disabled:text-slate-500"><ArrowUp className="h-4 w-4" /></button></div>
        </form>
        <p className="mx-auto mt-2 max-w-3xl text-center text-[10px] leading-4 text-slate-500">Preview: company research and section navigation work. Open-ended AI answers are coming next. Premium and Pro access still applies.</p>
      </footer>
    </div>
  </div>;
}
