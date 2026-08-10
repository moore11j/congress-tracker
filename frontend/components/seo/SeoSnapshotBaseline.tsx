import Link from "next/link";
import type { ReactNode } from "react";
import type { SeoEntitySnapshot } from "@/lib/api";

type Field = { label: string; value: string };
type ActivityRow = Record<string, unknown>;

const appCardClassName =
  "rounded-lg border border-white/10 bg-[#0a1726]/95 shadow-[0_14px_34px_rgba(0,0,0,0.22)]";
const moduleCardClassName =
  "rounded-lg border border-white/10 bg-slate-900/70 shadow-[0_18px_60px_-42px_rgba(20,184,166,0.8)] ring-1 ring-white/[0.025]";
const actionClassName =
  "inline-flex h-9 min-w-0 items-center justify-center rounded-lg border border-white/10 bg-slate-950/20 px-4 text-xs font-semibold text-slate-100 transition hover:border-white/25 hover:bg-white/[0.04] sm:text-sm";
const primaryActionClassName =
  "inline-flex h-9 min-w-0 items-center justify-center rounded-lg border border-emerald-400/35 bg-emerald-500/10 px-4 text-xs font-semibold text-emerald-100 transition hover:bg-emerald-500/18 sm:text-sm";

const navItemsByType: Record<SeoEntitySnapshot["entity_type"], Array<{ label: string; href: string }>> = {
  ticker: [
    { label: "Overview", href: "#overview" },
    { label: "Price / Volume", href: "#price-volume" },
    { label: "Congress", href: "#source-grid" },
    { label: "Insiders", href: "#source-grid" },
    { label: "Ownership", href: "#source-grid" },
    { label: "Financials", href: "#source-grid" },
    { label: "Analysts", href: "#source-grid" },
  ],
  member: [
    { label: "Overview", href: "#overview" },
    { label: "Trades", href: "#recent-activity" },
    { label: "Performance", href: "#locked-workflows" },
    { label: "Holdings", href: "#locked-workflows" },
    { label: "Activity", href: "#recent-activity" },
    { label: "Committees", href: "#locked-workflows" },
  ],
  insider: [
    { label: "Overview", href: "#overview" },
    { label: "Transactions", href: "#recent-activity" },
    { label: "Ownership", href: "#locked-workflows" },
    { label: "Performance", href: "#locked-workflows" },
    { label: "Filings", href: "#recent-activity" },
  ],
};

function textValue(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function numberValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() && Number.isFinite(Number(value))) return Number(value);
  return null;
}

function formatDate(value: unknown): string {
  const text = textValue(value);
  if (!text) return "";
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return text;
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function formatCurrency(value: unknown): string {
  const number = numberValue(value);
  if (number === null) return "";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: number >= 100 ? 0 : 2,
  }).format(number);
}

function formatActivityValue(key: string, value: unknown): string {
  if (value === null || value === undefined) return "";
  if (key.includes("date") || key === "ts") return formatDate(value);
  if (key.includes("price") || key.includes("value") || key.includes("amount")) return formatCurrency(value) || textValue(value);
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "";
  if (typeof value === "string") return value.trim();
  return "";
}

function cleanLabel(value: string): string {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .replace(/\bCik\b/g, "CIK")
    .replace(/\bTs\b/g, "Date");
}

function cleanBody(value: unknown): string {
  const text = textValue(value);
  if (!text) return "";
  if (/snapshot|stored|provider refresh|seo/i.test(text)) return "";
  return text;
}

function dataAsOf(snapshot: SeoEntitySnapshot): string {
  return formatDate(snapshot.data_as_of ?? snapshot.updated_at ?? snapshot.generated_at) || "recent data";
}

function entityTitle(snapshot: SeoEntitySnapshot): string {
  const payload = snapshot.payload ?? {};
  return (
    textValue(payload.company_name) ||
    textValue(payload.member_name) ||
    textValue(payload.insider_name) ||
    textValue(payload.holder_name) ||
    snapshot.entity_key
  );
}

function primaryCode(snapshot: SeoEntitySnapshot): string {
  const payload = snapshot.payload ?? {};
  if (snapshot.entity_type === "ticker") return textValue(payload.symbol) || snapshot.entity_key;
  if (snapshot.entity_type === "insider") return textValue(payload.primary_symbol) || textValue(payload.reporting_cik);
  return textValue(payload.bioguide_id) || textValue(payload.state);
}

function tickerHeaderMetadata(snapshot: SeoEntitySnapshot): string[] {
  const payload = snapshot.payload ?? {};
  return [payload.sector, payload.industry, payload.country]
    .map(textValue)
    .filter(Boolean);
}

function initialsFor(title: string) {
  const parts = title.split(/\s+/).filter(Boolean);
  const first = parts[0]?.[0] ?? "W";
  const last = parts.length > 1 ? parts[parts.length - 1]?.[0] : parts[0]?.[1];
  return `${first}${last ?? ""}`.toUpperCase();
}

function routeSummary(snapshot: SeoEntitySnapshot): string {
  const payload = snapshot.payload ?? {};
  const title = entityTitle(snapshot);
  if (snapshot.entity_type === "ticker") {
    const symbol = primaryCode(snapshot);
    return `${symbol}${title && title.toUpperCase() !== symbol.toUpperCase() ? ` / ${title}` : ""}`;
  }
  if (snapshot.entity_type === "member") {
    const chamber = textValue(payload.chamber);
    const party = textValue(payload.party);
    const state = textValue(payload.state);
    return `${chamber ? `U.S. ${chamber}` : "U.S. Congress"}${party ? ` - ${party}` : ""}${state ? ` - ${state}` : ""}`;
  }
  const role = textValue(payload.primary_role);
  const company = textValue(payload.primary_company_name);
  const symbol = textValue(payload.primary_symbol);
  return [role, company, symbol].filter(Boolean).join(" - ") || "Form 4 activity profile";
}

function overviewCards(snapshot: SeoEntitySnapshot): Array<{ label: string; title: string; body: string }> {
  const payload = snapshot.payload ?? {};
  const sections = Array.isArray(payload.sections) ? payload.sections : [];
  const cleanSections = sections
    .map((section) => ({
      label: textValue(section.heading) || "Overview",
      title: textValue(section.heading) || "Overview",
      body: cleanBody(section.body),
    }))
    .filter((section) => section.body);
  if (cleanSections.length) return cleanSections.slice(0, snapshot.entity_type === "ticker" ? 3 : 2);
  if (snapshot.entity_type === "ticker") {
    return [
      {
        label: "Confirmation",
        title: "Evidence alignment",
        body: "Walnut brings price, volume, fundamentals, Congress activity, insider filings, and market context into the same ticker workflow.",
      },
      {
        label: "Research",
        title: "What changed recently",
        body: "Use the terminal to inspect recent activity, source movement, and confirmation details behind the public baseline.",
      },
      {
        label: "Risk",
        title: "What can weaken the setup",
        body: "Deeper source detail stays gated by plan while the page keeps the same research structure.",
      },
    ];
  }
  if (snapshot.entity_type === "member") {
    return [
      {
        label: "Disclosures",
        title: "Congress trading context",
        body: "Review recent public disclosures, timing, traded securities, and related ticker links in the same Walnut profile structure.",
      },
      {
        label: "Performance",
        title: "Trade outcome workflow",
        body: "Portfolio and outcome modules remain visible in the product shell and unlock according to the current account plan.",
      },
    ];
  }
  return [
    {
      label: "Form 4",
      title: "Insider activity context",
      body: "Review public Form 4 activity, transaction direction, issuer relationship, and related stock research from one Walnut profile.",
    },
    {
      label: "Performance",
      title: "Transaction outcome workflow",
      body: "Deeper performance, ownership, and monitoring workflows remain visible as product modules with gated detail where required.",
    },
  ];
}

function metricCards(snapshot: SeoEntitySnapshot): Field[] {
  const payload = snapshot.payload ?? {};
  const recentActivity = Array.isArray(payload.recent_activity) ? payload.recent_activity : [];
  if (snapshot.entity_type === "ticker") {
    return [
      { label: "Price", value: formatCurrency(payload.price) || "Available in terminal" },
      { label: "Price Date", value: formatDate(payload.price_date) || dataAsOf(snapshot) },
      { label: "Confirmation", value: textValue(payload.confirmation_score) || "Locked" },
      { label: "Activity", value: recentActivity.length ? `${recentActivity.length} recent item${recentActivity.length === 1 ? "" : "s"}` : "Source context" },
    ];
  }
  if (snapshot.entity_type === "member") {
    return [
      { label: "Recent Disclosures", value: `${recentActivity.length}` },
      { label: "Chamber", value: textValue(payload.chamber) || "Congress" },
      { label: "State", value: textValue(payload.state) || "Profile" },
      { label: "Updated", value: dataAsOf(snapshot) },
    ];
  }
  const buyCount = recentActivity.filter((row) => /buy|purchase|^p$/i.test(textValue(row.transaction_type))).length;
  const sellCount = recentActivity.filter((row) => /sell|sale|^s$/i.test(textValue(row.transaction_type))).length;
  return [
    { label: "Recent Filings", value: `${recentActivity.length}` },
    { label: "Buy Signals", value: `${buyCount}` },
    { label: "Sell Signals", value: `${sellCount}` },
    { label: "Updated", value: dataAsOf(snapshot) },
  ];
}

function sourceModules(snapshot: SeoEntitySnapshot): Array<{ label: string; title: string; body: string; locked?: boolean }> {
  if (snapshot.entity_type === "ticker") {
    return [
      { label: "Source", title: "Congress Activity", body: "Recent public disclosures tied to the ticker.", locked: false },
      { label: "Source", title: "Insider Activity", body: "Form 4 activity and insider transaction direction.", locked: false },
      { label: "Pro", title: "Institutional Activity", body: "Reported holder activity and position changes.", locked: true },
      { label: "Public Awards", title: "Government Contracts", body: "Contract awards and modifications connected to the company.", locked: false },
      { label: "Premium", title: "Technical Indicators", body: "Trend, momentum, relative volume, beta, and liquidity context.", locked: true },
      { label: "Premium", title: "Fundamental Indicators", body: "Valuation, margins, leverage, cash flow, and balance-sheet quality.", locked: true },
    ];
  }
  if (snapshot.entity_type === "member") {
    return [
      { label: "Portfolio", title: "Portfolio Simulation", body: "Return and alpha views for disclosed trades.", locked: true },
      { label: "Holdings", title: "Current Exposure", body: "Position context built from available disclosure history.", locked: true },
      { label: "Monitoring", title: "Follow Member", body: "Watch new disclosures and related ticker activity.", locked: true },
      { label: "Committees", title: "Official Profile Context", body: "Committee and jurisdiction context alongside market activity.", locked: true },
    ];
  }
  return [
    { label: "Performance", title: "Transaction Outcomes", body: "Measure Form 4 activity against issuer price movement.", locked: true },
    { label: "Ownership", title: "Ownership & Performance", body: "Ownership and issuer relationship views.", locked: true },
    { label: "Monitoring", title: "Follow Insider", body: "Watch new filings and related ticker activity.", locked: true },
    { label: "Issuer", title: "Related Stock Research", body: "Open connected ticker research workflows.", locked: false },
  ];
}

function ActivityTable({ rows, entityType }: { rows: ActivityRow[]; entityType: SeoEntitySnapshot["entity_type"] }) {
  const title = entityType === "ticker" ? "Recent Market Activity" : entityType === "member" ? "Recent Disclosures" : "Recent Form 4 Activity";
  if (!rows.length) {
    return (
      <section id="recent-activity" className={`${appCardClassName} p-4`}>
        <h2 className="text-[11px] font-semibold uppercase leading-none tracking-[0.14em] text-slate-200">{title}</h2>
        <p className="mt-3 text-sm leading-6 text-slate-400">Public activity detail is not available for this profile yet.</p>
      </section>
    );
  }
  const preferredKeysByType: Record<SeoEntitySnapshot["entity_type"], string[]> = {
    ticker: ["event_type", "title", "symbol", "ts"],
    member: ["transaction_type", "description", "trade_date", "report_date"],
    insider: ["transaction_type", "symbol", "transaction_date", "filing_date"],
  };
  const preferredKeys = preferredKeysByType[entityType];
  return (
    <section id="recent-activity" className={`${appCardClassName} overflow-hidden`}>
      <div className="border-b border-white/10 px-4 py-3">
        <h2 className="text-[11px] font-semibold uppercase leading-none tracking-[0.14em] text-slate-200">{title}</h2>
      </div>
      <div className="divide-y divide-white/10">
        {rows.slice(0, 8).map((row, index) => {
          const values = preferredKeys
            .map((key) => [key, formatActivityValue(key, row[key])] as const)
            .filter(([, value]) => value);
          return (
            <div key={index} className="grid gap-3 px-4 py-3 text-sm sm:grid-cols-4">
              {values.map(([key, value]) => (
                <p key={key} className="min-w-0">
                  <span className="block text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">{cleanLabel(key)}</span>
                  <span className="mt-1 block truncate text-slate-200">{value}</span>
                </p>
              ))}
            </div>
          );
        })}
      </div>
    </section>
  );
}

function LockedOverlay({ children, label = "Upgrade required" }: { children: ReactNode; label?: string }) {
  return (
    <div className="relative overflow-hidden rounded-lg border border-white/10 bg-slate-950/30">
      <div className="pointer-events-none select-none opacity-70 blur-[2.5px]" aria-hidden="true">
        {children}
      </div>
      <div className="absolute inset-0 grid place-items-center bg-slate-950/50 px-4 text-center backdrop-blur-[1px]">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-200">{label}</p>
          <Link href="/pricing" className="mt-3 inline-flex h-9 items-center justify-center rounded-lg border border-emerald-300/35 bg-emerald-300/10 px-3 text-xs font-semibold text-emerald-100">
            View plans
          </Link>
        </div>
      </div>
    </div>
  );
}

function SourceModuleCard({ module }: { module: ReturnType<typeof sourceModules>[number] }) {
  const content = (
    <article className="min-h-[8.5rem] p-4">
      <div className="flex items-start justify-between gap-3">
        <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-emerald-300">{module.label}</p>
        {module.locked ? <span className="rounded border border-amber-300/25 bg-amber-300/10 px-1.5 py-0.5 text-[10px] font-semibold text-amber-100">Locked</span> : null}
      </div>
      <h3 className="mt-3 text-base font-semibold text-white">{module.title}</h3>
      <p className="mt-3 text-sm leading-6 text-slate-400">{module.body}</p>
    </article>
  );
  return module.locked ? <LockedOverlay label="Plan gated">{content}</LockedOverlay> : <div className={moduleCardClassName}>{content}</div>;
}

function TickerHeader({ snapshot, title, code }: { snapshot: SeoEntitySnapshot; title: string; code: string }) {
  const payload = snapshot.payload ?? {};
  const exchange = textValue(payload.exchange);
  const metadata = tickerHeaderMetadata(snapshot);
  return (
    <section className="flex flex-wrap items-center justify-between gap-4">
      <div className="min-w-0 basis-full max-w-[calc(100vw-2rem)] lg:basis-auto lg:max-w-full">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Ticker intelligence</p>
        <h1 className="max-w-full break-words text-2xl font-semibold text-white [overflow-wrap:anywhere] sm:text-3xl">
          <span>{code}</span>
          {title && title.toUpperCase() !== code.toUpperCase() ? <span className="text-slate-400"> / {title}</span> : null}
          <span className="ml-2 align-middle text-xl font-normal text-slate-500">☆</span>
        </h1>
        <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1">
          {exchange ? <span className="rounded-md border border-white/10 bg-slate-900/45 px-2.5 py-1 text-[11px] font-semibold uppercase text-slate-300">{exchange}</span> : null}
          {metadata.length ? (
            <p className="min-w-0 rounded-md bg-slate-900/45 px-3 py-1 text-[11px] font-medium tracking-[0.02em] text-slate-400 sm:max-w-[44rem] sm:truncate">
              {metadata.join(" / ")}
            </p>
          ) : null}
        </div>
        <p className="mt-3 text-xs text-slate-500">Data as of {dataAsOf(snapshot)}</p>
      </div>
      <div className="grid w-[calc(100vw-2rem)] flex-none grid-cols-2 gap-2 [&>*]:w-full [&>a]:justify-center sm:flex sm:w-auto sm:flex-initial sm:flex-wrap sm:items-center sm:justify-end sm:[&>*]:w-auto">
        <Link href="/login" className={actionClassName}>Login / Register</Link>
        <Link href={`/compare/${encodeURIComponent(code)}/_`} className={actionClassName}>Compare</Link>
        <Link href="/?mode=all" className={actionClassName}>Back to feed</Link>
      </div>
    </section>
  );
}

function ProfileHeader({ snapshot, title, code }: { snapshot: SeoEntitySnapshot; title: string; code: string }) {
  const isMember = snapshot.entity_type === "member";
  const profileLabel = isMember ? "Congress Trading Profile" : "Insider Trading Profile";
  return (
    <section className="relative overflow-hidden rounded-lg border border-white/10 bg-[linear-gradient(135deg,rgba(9,20,35,0.98),rgba(4,10,20,0.98))] px-4 pt-3 shadow-[0_18px_48px_rgba(0,0,0,0.32)] sm:px-5">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <Link href={isMember ? "/?mode=congress" : "/?mode=insider"} className="text-[10px] font-semibold uppercase tracking-[0.22em] text-emerald-300/80 hover:text-emerald-200">
          {profileLabel}
        </Link>
        <div className="grid w-full grid-cols-2 gap-2 sm:flex sm:w-auto sm:flex-wrap sm:justify-end lg:absolute lg:right-5 lg:top-3">
          <Link href="/login" className={actionClassName}>{isMember ? "Follow Member" : "Follow Insider"}</Link>
          <Link href="/login" className={actionClassName}>Share</Link>
          <Link href="/pricing" className={primaryActionClassName}>{isMember ? "Backtest this Member" : "Backtest this Insider"}</Link>
        </div>
      </div>
      <div className="mt-3 flex min-w-0 gap-4 pb-2 lg:pr-[28rem]">
        <div className="grid h-20 w-20 shrink-0 place-items-center rounded-full border border-white/15 bg-slate-950/70 text-2xl font-semibold text-emerald-100 shadow-inner">
          {initialsFor(title)}
        </div>
        <div className="min-w-0 pt-0.5">
          <div className="mt-1.5 flex flex-wrap items-center gap-2">
            <h1 className="truncate text-2xl font-semibold leading-tight text-white sm:text-3xl">{title}</h1>
            <span className="grid h-4 w-4 place-items-center rounded-full bg-sky-500 text-white shadow-[0_0_12px_rgba(14,165,233,0.35)]">
              <svg viewBox="0 0 12 12" aria-hidden="true" className="h-2.5 w-2.5" fill="none">
                <path d="M3 6.2 5 8l4-4.5" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" />
              </svg>
            </span>
          </div>
          <p className="mt-2 text-sm text-slate-300">{routeSummary(snapshot)}</p>
          {code ? <p className="mt-1 text-xs font-medium text-slate-500">{isMember ? "Bioguide ID" : "Reporting CIK"}: {code}</p> : null}
          <div className="mt-2 flex flex-wrap gap-1.5 text-[10px] text-slate-400">
            <span className="inline-flex items-center rounded-md border border-emerald-300/20 bg-emerald-300/10 px-2 py-0.5 text-[10px] font-semibold text-emerald-100">
              {isMember ? "Activity profile" : "Ownership context"}
            </span>
          </div>
        </div>
      </div>
      <nav className="flex gap-7 overflow-x-auto border-t border-white/10 pt-2 text-sm font-medium text-slate-400" aria-label={`${title} sections`}>
        {navItemsByType[snapshot.entity_type].map((item, index) => (
          <a key={item.label} href={item.href} className={`shrink-0 border-b-2 pb-2 ${index === 0 ? "border-amber-300 text-amber-200" : "border-transparent hover:text-white"}`}>
            {item.label}
          </a>
        ))}
      </nav>
    </section>
  );
}

function TickerOverview({ snapshot }: { snapshot: SeoEntitySnapshot }) {
  return (
    <>
      <nav className="flex gap-7 overflow-x-auto border-y border-white/10 py-2 text-sm font-medium text-slate-400" aria-label={`${entityTitle(snapshot)} sections`}>
        {navItemsByType.ticker.map((item, index) => (
          <a key={item.label} href={item.href} className={`shrink-0 border-b-2 pb-2 ${index === 0 ? "border-amber-300 text-amber-200" : "border-transparent hover:text-white"}`}>
            {item.label}
          </a>
        ))}
      </nav>
      <section id="overview" className="grid gap-4 lg:grid-cols-[minmax(0,1.05fr)_minmax(20rem,0.95fr)]">
        <LockedOverlay label="Premium signal">
          <div className="p-5">
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Confirmation Score</p>
            <div className="mt-4 flex items-end justify-between gap-4">
              <div>
                <p className="text-4xl font-semibold tabular-nums text-white">72</p>
                <p className="mt-2 text-sm text-emerald-200">Bullish alignment</p>
              </div>
              <p className="max-w-xs text-right text-sm leading-6 text-slate-400">Active-source alignment, freshness, and opposing evidence are available with Premium or Pro.</p>
            </div>
          </div>
        </LockedOverlay>
        <div id="price-volume" className={`${moduleCardClassName} p-5`}>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Price / Volume</p>
          <div className="mt-4 grid gap-3 sm:grid-cols-2">
            {metricCards(snapshot).map((metric) => (
              <div key={metric.label} className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
                <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">{metric.label}</p>
                <p className="mt-2 text-lg font-semibold text-white">{metric.value}</p>
              </div>
            ))}
          </div>
        </div>
      </section>
    </>
  );
}

export function SeoSnapshotBaseline({ snapshot }: { snapshot: SeoEntitySnapshot }) {
  const payload = snapshot.payload ?? {};
  const recentActivity = Array.isArray(payload.recent_activity) ? payload.recent_activity : [];
  const links = Array.isArray(payload.links) ? payload.links : [];
  const title = entityTitle(snapshot);
  const code = primaryCode(snapshot);
  const canonicalUrl = `https://app.walnutmarkets.com${snapshot.canonical_path}`;
  const jsonLd = [
    {
      "@context": "https://schema.org",
      "@type": "WebPage",
      name: snapshot.title,
      description: snapshot.meta_description,
      url: canonicalUrl,
      dateModified: snapshot.updated_at ?? snapshot.generated_at ?? undefined,
    },
    {
      "@context": "https://schema.org",
      "@type": "BreadcrumbList",
      itemListElement: [
        { "@type": "ListItem", position: 1, name: "Walnut Markets", item: "https://app.walnutmarkets.com" },
        { "@type": "ListItem", position: 2, name: title, item: canonicalUrl },
      ],
    },
  ];

  return (
    <div className="space-y-6">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd).replace(/</g, "\\u003c") }} />

      {snapshot.entity_type === "ticker" ? (
        <TickerHeader snapshot={snapshot} title={title} code={code} />
      ) : (
        <ProfileHeader snapshot={snapshot} title={title} code={code} />
      )}

      {snapshot.entity_type === "ticker" ? <TickerOverview snapshot={snapshot} /> : null}

      <section id={snapshot.entity_type === "ticker" ? "research-context" : "overview"} className={snapshot.entity_type === "ticker" ? "grid gap-4 lg:grid-cols-3" : "grid gap-4 sm:grid-cols-2 lg:grid-cols-4"}>
        {snapshot.entity_type === "ticker"
          ? overviewCards(snapshot).map((section) => (
              <article key={`${section.label}-${section.title}`} className={`${moduleCardClassName} p-5`}>
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">{section.label}</p>
                <h2 className="mt-3 text-lg font-semibold text-white">{section.title}</h2>
                <p className="mt-3 text-sm leading-6 text-slate-300">{section.body}</p>
              </article>
            ))
          : metricCards(snapshot).map((metric) => (
              <article key={metric.label} className={`${appCardClassName} p-4`}>
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{metric.label}</p>
                <p className="mt-2 text-xl font-semibold text-white">{metric.value}</p>
              </article>
            ))}
      </section>

      {snapshot.entity_type !== "ticker" ? (
        <section className="grid gap-4 lg:grid-cols-2">
          {overviewCards(snapshot).map((section) => (
            <article key={`${section.label}-${section.title}`} className={`${appCardClassName} p-5`}>
              <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">{section.label}</p>
              <h2 className="mt-3 text-lg font-semibold text-white">{section.title}</h2>
              <p className="mt-3 text-sm leading-6 text-slate-300">{section.body}</p>
            </article>
          ))}
        </section>
      ) : null}

      <section id="source-grid" className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {sourceModules(snapshot).map((module) => (
          <SourceModuleCard key={`${module.label}-${module.title}`} module={module} />
        ))}
      </section>

      <ActivityTable rows={recentActivity} entityType={snapshot.entity_type} />

      <section id="locked-workflows" className="grid gap-4 lg:grid-cols-2">
        {snapshot.entity_type === "ticker" ? (
          <>
            <LockedOverlay label="Pro workflow">
              <div className="p-5">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Ownership</p>
                <h2 className="mt-3 text-lg font-semibold text-white">Institutional holder activity</h2>
                <p className="mt-3 text-sm leading-6 text-slate-400">Track filings, holder concentration, and position changes inside the terminal.</p>
              </div>
            </LockedOverlay>
            <LockedOverlay label="Premium workflow">
              <div className="p-5">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Analysts</p>
                <h2 className="mt-3 text-lg font-semibold text-white">Valuation and price targets</h2>
                <p className="mt-3 text-sm leading-6 text-slate-400">Compare analyst context, valuation ranges, and estimate movement.</p>
              </div>
            </LockedOverlay>
          </>
        ) : null}
      </section>

      {links.length > 0 ? (
        <nav className={`${appCardClassName} flex flex-wrap gap-3 p-4`} aria-label="Related Walnut research links">
          {links.map((link) => (
            <Link key={link.href} href={link.href} className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-emerald-200 transition hover:border-emerald-300/50 hover:text-emerald-100">
              {link.label}
            </Link>
          ))}
        </nav>
      ) : null}

      <section className={`${appCardClassName} flex flex-col gap-3 p-4 sm:flex-row sm:items-center sm:justify-between`}>
        <div>
          <p className="text-sm font-semibold text-white">Open the live terminal for the full workflow.</p>
          <p className="mt-1 text-sm text-slate-400">Public pages keep the product structure visible while source detail unlocks by account plan.</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Link href="/login" className={primaryActionClassName}>Login / Register</Link>
          <Link href="/pricing" className={actionClassName}>Pricing</Link>
        </div>
      </section>
    </div>
  );
}
