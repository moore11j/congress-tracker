import type { Metadata } from "next";
import type { ReactNode } from "react";

export const metadata: Metadata = {
  metadataBase: new URL("https://walnut-intel.com"),
  title: "Walnut Intel | Market Intelligence from Political Trades and Insider Activity",
  description: "Track congressional trades, insider transactions, ticker intelligence, signal scores, and cross-source market confirmation.",
  alternates: {
    canonical: "/",
  },
};

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnut-intel.com").replace(/\/+$/, "");
const loginUrl = `${appUrl}/login`;
const pricingUrl = `${appUrl}/pricing`;

const navLinks = [
  ["Signals", "#signals-preview"],
  ["Congress Trades", "#congress-preview"],
  ["Insider Trades", "#insider-preview"],
  ["Screener", "#screener"],
  ["Pricing", "#pricing"],
] as const;

const signalCards = [
  {
    title: "Congressional trading disclosures",
    body: "Monitor House and Senate activity with ticker, filing, party, chamber, and trade context.",
    label: "Political tape",
  },
  {
    title: "Insider transactions",
    body: "Track executive and director purchases, sales, ownership changes, and role-weighted activity.",
    label: "SEC Form 4",
  },
  {
    title: "Ticker intelligence",
    body: "Unify political, insider, financial, and event-level context around a single public-market name.",
    label: "Ticker lens",
  },
  {
    title: "Signal Conviction Score",
    body: "Rank names by cross-source confirmation instead of treating each disclosure as an isolated datapoint.",
    label: "Confirmation",
  },
  {
    title: "Watchlists and alerts",
    body: "Keep priority tickers close and prepare for premium monitoring workflows as new signals land.",
    label: "Monitoring",
  },
  {
    title: "Screener and saved views",
    body: "Turn recurring research patterns into repeatable screens across market and intelligence filters.",
    label: "Research ops",
  },
  {
    title: "Technical indicator filters",
    body: "Screen for RSI, relative volume, price momentum, MACD state, trend state, beta, and liquidity conditions.",
    label: "Technicals",
  },
  {
    title: "Fundamental indicator filters",
    body: "Filter by valuation, margins, growth, leverage, cash flow, earnings yield, ROE, ROIC, and balance-sheet quality.",
    label: "Fundamentals",
  },
] as const;

const proofCards = [
  {
    ticker: "NVDA",
    title: "Insider sale + congressional interest",
    meta: "Example signal card",
    score: "82",
    direction: "Mixed",
    detail: "Large-cap technology name with fresh disclosure activity and elevated market attention.",
  },
  {
    ticker: "LMT",
    title: "Defense ticker confirmation",
    meta: "Example signal card",
    score: "76",
    direction: "Bullish",
    detail: "Political exposure, government-contract context, and ticker-level monitoring in one view.",
  },
  {
    ticker: "AAPL",
    title: "Ticker intelligence watch",
    meta: "Example signal card",
    score: "64",
    direction: "Neutral",
    detail: "Watchlist-ready profile combining disclosure tape, ownership context, and price/liquidity filters.",
  },
  {
    ticker: "PLTR",
    title: "Cross-source activity pulse",
    meta: "Example signal card",
    score: "71",
    direction: "Bullish",
    detail: "Emerging confirmation across alternative datasets, screener presets, and monitoring workflows.",
  },
] as const;

const whyWalnut = [
  "Less dashboard sprawl: one terminal for political, insider, ticker, and confirmation context.",
  "More signal confirmation: prioritize repeatable patterns over isolated headlines.",
  "Political plus insider plus ticker context: see who acted, what moved, and why it matters.",
  "Designed for fast research: compact surfaces for scanning, comparison, and follow-up.",
] as const;

const availableNow = [
  "Congress trades",
  "Insider trades",
  "Ticker intelligence",
  "Signal scores",
  "Government contracts",
  "Watchlists",
  "Screener",
  "Member/insider performance",
] as const;

const comingSoon = [
  "AI analyst briefs",
  "Options flow",
  "Institutional activity",
  "Earnings and event calendar overlays",
  "Social sentiment overlays",
  "Advanced alerts and exports",
] as const;

function WalnutMark() {
  return (
    <span className="flex h-9 w-9 items-center justify-center rounded-lg border border-emerald-300/35 bg-slate-950 shadow-[0_0_28px_rgba(16,185,129,0.18)]">
      <svg viewBox="0 0 48 48" aria-hidden="true" className="h-6 w-6">
        <path
          d="M24 7c-4.5 0-7.8 3.2-8.1 7.5-4.2.5-7.3 3.9-7.3 8.1 0 1.6.4 3 1.2 4.3-2 1.6-3.1 3.9-3.1 6.5 0 4.7 3.8 8.6 8.5 8.6 2.6 0 4.8-1.1 6.4-2.9.7.2 1.5.3 2.4.3s1.7-.1 2.4-.3c1.6 1.8 3.8 2.9 6.4 2.9 4.7 0 8.5-3.9 8.5-8.6 0-2.6-1.1-4.9-3.1-6.5.8-1.3 1.2-2.7 1.2-4.3 0-4.2-3.1-7.6-7.3-8.1C31.8 10.2 28.5 7 24 7Z"
          fill="#020617"
          stroke="#34d399"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="3"
        />
        <path
          d="M24 8.5v30M16 16c3.2 2.4 5.4 5.5 6.4 9M32 16c-3.2 2.4-5.4 5.5-6.4 9M10.5 27c4.1 1.5 7.1 3.9 9.1 7.4M37.5 27c-4.1 1.5-7.1 3.9-9.1 7.4"
          fill="none"
          stroke="#ccfbf1"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="2.4"
        />
      </svg>
    </span>
  );
}

function SectionEyebrow({ children }: { children: ReactNode }) {
  return <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{children}</p>;
}

export default function LandingPage() {
  return (
    <main className="min-h-screen overflow-hidden bg-[#030712] text-slate-100">
      <div className="absolute inset-0 -z-10 bg-[linear-gradient(90deg,rgba(148,163,184,0.05)_1px,transparent_1px),linear-gradient(180deg,rgba(148,163,184,0.04)_1px,transparent_1px)] bg-[size:56px_56px]" />
      <header className="sticky top-0 z-40 border-b border-white/10 bg-slate-950/88 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-4 sm:px-6 lg:px-8">
          <a href="/" className="flex min-w-0 items-center gap-3" aria-label="Walnut Intel home">
            <WalnutMark />
            <span className="leading-none">
              <span className="block whitespace-nowrap text-base font-semibold text-white">Walnut Intel</span>
              <span className="mt-1 block whitespace-nowrap text-[11px] font-medium text-slate-400">by Walnut Intelligence Inc.</span>
            </span>
          </a>
          <nav className="hidden items-center gap-5 text-sm font-medium text-slate-300 lg:flex">
            {navLinks.map(([label, href]) => (
              <a key={label} href={href} className="transition hover:text-white">
                {label}
              </a>
            ))}
          </nav>
          <div className="flex shrink-0 items-center gap-2">
            <a
              href={loginUrl}
              className="hidden rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/25 hover:text-white md:inline-flex"
            >
              Login / Register
            </a>
            <a
              href={appUrl}
              className="rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200"
            >
              Launch Terminal -&gt;
            </a>
          </div>
        </div>
      </header>

      <section className="relative border-b border-white/10">
        <div className="mx-auto grid min-h-[calc(100vh-73px)] max-w-7xl items-center gap-10 px-4 py-16 sm:px-6 lg:grid-cols-[1.02fr_0.98fr] lg:px-8 lg:py-20">
          <div className="max-w-3xl">
            <SectionEyebrow>Market intelligence terminal</SectionEyebrow>
            <h1 className="mt-5 max-w-4xl text-4xl font-semibold leading-[1.04] text-white sm:text-5xl lg:text-6xl">
              Market intelligence from political trades, insider activity, and cross-source signals.
            </h1>
            <p className="mt-6 max-w-2xl text-base leading-7 text-slate-300 sm:text-lg">
              Walnut Intel helps investors monitor congressional disclosures, insider transactions, ticker intelligence, and confirmation signals in one clean market terminal.
            </p>
            <div className="mt-8 flex flex-col gap-3 sm:flex-row">
              <a
                href={appUrl}
                className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200"
              >
                Launch Terminal
              </a>
              <a
                href={loginUrl}
                className="inline-flex items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-5 py-3 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:bg-white/[0.06]"
              >
                Login / Register
              </a>
            </div>
            <p className="mt-5 text-xs leading-5 text-slate-500">Built for research and monitoring. Not investment advice.</p>
          </div>

          <div className="relative">
            <div className="rounded-lg border border-white/10 bg-slate-950/90 shadow-2xl shadow-black/40">
              <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Walnut Market Terminal</p>
                  <p className="mt-1 text-sm font-semibold text-white">Cross-source signal monitor</p>
                </div>
                <span className="rounded border border-emerald-300/30 bg-emerald-300/10 px-2 py-1 text-xs font-semibold text-emerald-100">Live app</span>
              </div>
              <div className="grid grid-cols-3 border-b border-white/10 text-xs text-slate-400">
                <div className="border-r border-white/10 p-4">
                  <p className="text-slate-500">Signals</p>
                  <p className="mt-2 text-2xl font-semibold text-white">438</p>
                </div>
                <div className="border-r border-white/10 p-4">
                  <p className="text-slate-500">Confirmed</p>
                  <p className="mt-2 text-2xl font-semibold text-emerald-200">72</p>
                </div>
                <div className="p-4">
                  <p className="text-slate-500">Watchlist</p>
                  <p className="mt-2 text-2xl font-semibold text-cyan-200">31</p>
                </div>
              </div>
              <div className="space-y-3 p-4">
                {proofCards.slice(0, 3).map((card) => (
                  <div key={card.ticker} className="grid gap-3 rounded-lg border border-white/10 bg-white/[0.035] p-3 sm:grid-cols-[4rem_1fr_auto]">
                    <div className="font-mono text-lg font-semibold text-emerald-200">{card.ticker}</div>
                    <div>
                      <p className="text-sm font-semibold text-white">{card.title}</p>
                      <p className="mt-1 text-xs leading-5 text-slate-400">{card.detail}</p>
                    </div>
                    <div className="text-left sm:text-right">
                      <p className="text-xs text-slate-500">Conviction</p>
                      <p className="mt-1 font-mono text-lg font-semibold text-white">{card.score}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-3xl">
            <SectionEyebrow>Signal stack</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">A cleaner way to confirm market activity.</h2>
          </div>
          <div className="mt-8 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            {signalCards.map((card) => (
              <article key={card.title} className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">{card.label}</p>
                <h3 className="mt-4 text-lg font-semibold text-white">{card.title}</h3>
                <p className="mt-3 text-sm leading-6 text-slate-400">{card.body}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section id="signals-preview" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="flex flex-col justify-between gap-4 md:flex-row md:items-end">
            <div>
              <SectionEyebrow>Data preview</SectionEyebrow>
              <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Example cards from a market-intelligence workflow.</h2>
            </div>
            <p className="max-w-md text-sm leading-6 text-slate-500">Static examples for this public page. The terminal contains the live product experience.</p>
          </div>
          <div className="mt-8 grid gap-4 lg:grid-cols-4">
            {proofCards.map((card) => (
              <article key={card.ticker} className="rounded-lg border border-white/10 bg-slate-950/80 p-5">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <p className="font-mono text-xl font-semibold text-emerald-200">{card.ticker}</p>
                    <p className="mt-1 text-xs uppercase tracking-[0.16em] text-slate-500">{card.meta}</p>
                  </div>
                  <div className="text-right">
                    <p className="font-mono text-xl font-semibold text-white">{card.score}</p>
                    <p className="text-xs text-slate-500">{card.direction}</p>
                  </div>
                </div>
                <h3 className="mt-5 text-base font-semibold text-white">{card.title}</h3>
                <p className="mt-3 text-sm leading-6 text-slate-400">{card.detail}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-3xl">
            <SectionEyebrow>Terminal previews</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">See the research surfaces before you launch the terminal.</h2>
            <p className="mt-4 text-sm leading-6 text-slate-500">Static example previews. Live pages, portfolio simulations, and insider charts are available in the app after registration.</p>
          </div>

          <div className="mt-8 grid gap-5 lg:grid-cols-2">
            <article id="congress-preview" className="rounded-lg border border-white/10 bg-slate-950/85 p-5 shadow-2xl shadow-black/25">
              <div className="flex flex-wrap items-start justify-between gap-4 border-b border-white/10 pb-4">
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">Congress portfolio simulation</p>
                  <h3 className="mt-2 text-2xl font-semibold text-white">Nancy Pelosi disclosure portfolio</h3>
                  <p className="mt-2 max-w-xl text-sm leading-6 text-slate-400">Example view combining recent disclosures, simulated holdings, benchmark comparison, and trade outcome context.</p>
                </div>
                <span className="rounded border border-emerald-300/30 bg-emerald-300/10 px-2 py-1 text-xs font-semibold text-emerald-100">Example</span>
              </div>
              <div className="mt-5 grid gap-3 sm:grid-cols-3">
                {[
                  ["Simulated return", "+42.8%"],
                  ["Benchmark", "S&P 500"],
                  ["Lookback", "3Y"],
                ].map(([label, value]) => (
                  <div key={label} className="rounded-lg border border-white/10 bg-white/[0.035] p-4">
                    <p className="text-xs text-slate-500">{label}</p>
                    <p className="mt-2 font-mono text-xl font-semibold text-white">{value}</p>
                  </div>
                ))}
              </div>
              <div className="mt-5 rounded-lg border border-white/10 bg-[#050b18] p-4">
                <div className="flex h-36 items-end gap-2 border-b border-l border-white/10 px-3 pb-3">
                  {[22, 38, 34, 50, 46, 62, 58, 76, 71, 88, 84, 96].map((height, index) => (
                    <div key={index} className="flex-1 rounded-t bg-emerald-300/70" style={{ height: `${height}%` }} />
                  ))}
                </div>
                <div className="mt-3 grid gap-2 text-xs text-slate-400 sm:grid-cols-3">
                  <span>NVDA purchase disclosed</span>
                  <span>Benchmark comparison</span>
                  <span>Outcome markers on trade dates</span>
                </div>
              </div>
            </article>

            <article id="insider-preview" className="rounded-lg border border-white/10 bg-slate-950/85 p-5 shadow-2xl shadow-black/25">
              <div className="flex flex-wrap items-start justify-between gap-4 border-b border-white/10 pb-4">
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300">Insider profile with ticker chart</p>
                  <h3 className="mt-2 text-2xl font-semibold text-white">Tim Cook insider activity profile</h3>
                  <p className="mt-2 max-w-xl text-sm leading-6 text-slate-400">Example view showing insider transactions alongside the selected ticker chart, issuer context, and performance readouts.</p>
                </div>
                <span className="rounded border border-cyan-300/30 bg-cyan-300/10 px-2 py-1 text-xs font-semibold text-cyan-100">AAPL chart</span>
              </div>
              <div className="mt-5 grid gap-4 lg:grid-cols-[0.72fr_1.28fr]">
                <div className="space-y-3">
                  {[
                    ["Role", "CEO"],
                    ["Issuer", "Apple Inc."],
                    ["Mode", "Ticker chart selected"],
                  ].map(([label, value]) => (
                    <div key={label} className="rounded-lg border border-white/10 bg-white/[0.035] p-4">
                      <p className="text-xs text-slate-500">{label}</p>
                      <p className="mt-2 text-sm font-semibold text-white">{value}</p>
                    </div>
                  ))}
                </div>
                <div className="rounded-lg border border-white/10 bg-[#050b18] p-4">
                  <div className="flex h-44 items-end gap-1.5 border-b border-l border-white/10 px-3 pb-3">
                    {[34, 31, 42, 45, 39, 52, 57, 61, 54, 69, 73, 67, 82, 78].map((height, index) => (
                      <div
                        key={index}
                        className={`flex-1 rounded-t ${index === 6 || index === 11 ? "bg-amber-300/80" : "bg-cyan-300/65"}`}
                        style={{ height: `${height}%` }}
                      />
                    ))}
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2 text-xs">
                    <span className="rounded border border-cyan-300/25 bg-cyan-300/10 px-2 py-1 text-cyan-100">Price series</span>
                    <span className="rounded border border-amber-300/25 bg-amber-300/10 px-2 py-1 text-amber-100">Insider markers</span>
                    <span className="rounded border border-white/10 bg-white/[0.04] px-2 py-1 text-slate-300">Transaction table</span>
                  </div>
                </div>
              </div>
            </article>
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-10 lg:grid-cols-[0.85fr_1.15fr]">
          <div>
            <SectionEyebrow>Why Walnut</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Built for research speed, not raw data dumping.</h2>
            <p className="mt-5 text-base leading-7 text-slate-400">
              Walnut Market Terminal brings political disclosures, insider transactions, ticker context, and signal confirmation into a compact workflow for investors who need to move quickly.
            </p>
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
            {whyWalnut.map((item) => (
              <div key={item} className="rounded-lg border border-white/10 bg-white/[0.035] p-5 text-sm leading-6 text-slate-300">
                {item}
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <SectionEyebrow>Dataset roadmap</SectionEyebrow>
          <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Available now, with new market-intelligence datasets coming next.</h2>
          <div className="mt-8 grid gap-4 lg:grid-cols-2">
            <div className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.04] p-6">
              <h3 className="text-lg font-semibold text-white">Available Now</h3>
              <div className="mt-5 grid gap-3 sm:grid-cols-2">
                {availableNow.map((item) => (
                  <div key={item} className="rounded-lg border border-white/10 bg-slate-950/70 px-4 py-3 text-sm font-medium text-slate-200">
                    {item}
                  </div>
                ))}
              </div>
            </div>
            <div className="rounded-lg border border-cyan-300/20 bg-cyan-300/[0.035] p-6">
              <h3 className="text-lg font-semibold text-white">Coming Soon</h3>
              <div className="mt-5 grid gap-3 sm:grid-cols-2">
                {comingSoon.map((item) => (
                  <div key={item} className="flex items-center justify-between gap-3 rounded-lg border border-white/10 bg-slate-950/70 px-4 py-3 text-sm font-medium text-slate-200">
                    <span>{item}</span>
                    <span className="shrink-0 rounded border border-cyan-300/30 bg-cyan-300/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-cyan-100">
                      Coming Soon
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="screener" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl rounded-lg border border-white/10 bg-white/[0.035] p-6 sm:p-8">
          <div className="grid gap-8 lg:grid-cols-[0.75fr_1.25fr] lg:items-center">
            <div>
              <SectionEyebrow>Screener</SectionEyebrow>
              <h2 className="mt-3 text-3xl font-semibold text-white">An advanced stock screener built for signal confirmation.</h2>
              <p className="mt-5 text-sm leading-6 text-slate-400">
                Screen across disclosure activity, government contracts, technical indicators, fundamentals, liquidity, valuation, trend, quality, and confirmation signals from the same terminal experience.
              </p>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
              {[
                "Political activity",
                "Insider activity",
                "Government contracts",
                "Confirmation score",
                "RSI and relative volume",
                "MACD and trend state",
                "Valuation multiples",
                "Margins and growth",
                "ROE, ROIC, cash flow",
              ].map((item) => (
                <div key={item} className="rounded-lg border border-white/10 bg-slate-950/70 p-4">
                  <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Filter</p>
                  <p className="mt-3 text-sm font-semibold text-white">{item}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section id="pricing" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <SectionEyebrow>Pricing</SectionEyebrow>
          <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Start free. Upgrade to Premium or Pro when you need deeper monitoring.</h2>
          <div className="mt-8 grid gap-4 lg:grid-cols-3">
            <article className="rounded-lg border border-white/10 bg-white/[0.035] p-6">
              <h3 className="text-xl font-semibold text-white">Free</h3>
              <p className="mt-3 text-sm leading-6 text-slate-400">Basic monitoring and public market intelligence for disclosure research.</p>
            </article>
            <article className="rounded-lg border border-emerald-300/25 bg-emerald-300/[0.04] p-6">
              <div className="flex items-center justify-between gap-3">
                <h3 className="text-xl font-semibold text-white">Premium</h3>
                <span className="rounded border border-emerald-300/35 bg-emerald-300/10 px-2 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-emerald-100">
                  Popular
                </span>
              </div>
              <p className="mt-3 text-sm leading-6 text-slate-400">
                Advanced screeners, monitoring, saved views, exports, alerts, and higher workflow capacity.
              </p>
            </article>
            <article className="rounded-lg border border-cyan-300/25 bg-cyan-300/[0.035] p-6">
              <div className="flex items-center justify-between gap-3">
                <h3 className="text-xl font-semibold text-white">Pro</h3>
                <span className="rounded border border-cyan-300/35 bg-cyan-300/10 px-2 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-cyan-100">
                  Highest limits
                </span>
              </div>
              <p className="mt-3 text-sm leading-6 text-slate-400">
                More capacity for watchlists, saved views, monitoring sources, screeners, and power-user research workflows.
              </p>
            </article>
          </div>
          <div className="mt-8 flex flex-col gap-3 sm:flex-row">
            <a
              href={pricingUrl}
              className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200"
            >
              Compare Plans
            </a>
            <a
              href={loginUrl}
              className="inline-flex items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-5 py-3 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:bg-white/[0.06]"
            >
              Login / Register
            </a>
          </div>
        </div>
      </section>

      <footer className="px-4 py-10 sm:px-6 lg:px-8">
        <div className="mx-auto flex max-w-7xl flex-col gap-6 text-sm text-slate-400 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="font-semibold text-white">Walnut Intel</p>
            <p className="mt-1">by Walnut Intelligence Inc.</p>
            <p className="mt-3 max-w-2xl text-xs leading-5 text-slate-500">
              Walnut Intel is for informational and research purposes only and does not provide investment advice.
            </p>
          </div>
          <nav className="flex flex-wrap gap-4">
            <a href={appUrl} className="hover:text-white">
              App
            </a>
            <a href={pricingUrl} className="hover:text-white">
              Pricing
            </a>
            <a href={loginUrl} className="hover:text-white">
              Login / Register
            </a>
            <a href="mailto:contact@walnut-intel.com" className="hover:text-white">
              Contact
            </a>
            <a href={`${appUrl}/terms`} className="hover:text-white">
              Terms
            </a>
            <a href={`${appUrl}/privacy`} className="hover:text-white">
              Privacy
            </a>
          </nav>
        </div>
      </footer>
    </main>
  );
}
