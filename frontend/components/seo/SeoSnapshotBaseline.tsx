import Link from "next/link";
import type { SeoEntitySnapshot } from "@/lib/api";

type Field = { label: string; value: string };
type ActivityRow = Record<string, unknown>;

const cardClassName = "rounded-lg border border-white/10 bg-slate-900/70 shadow-[0_18px_60px_-42px_rgba(20,184,166,0.8)] ring-1 ring-white/[0.025]";
const subtleCardClassName = "rounded-lg border border-white/10 bg-slate-950/45";
const navItemsByType: Record<SeoEntitySnapshot["entity_type"], string[]> = {
  ticker: ["Overview", "News", "Financials", "Ownership", "Events / Filings", "Macro Positioning", "Valuation", "Analysts"],
  member: ["Overview", "Trades", "Performance", "Holdings", "Activity", "Committees"],
  insider: ["Overview", "Transactions", "Ownership", "Performance", "Filings"],
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
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: number >= 100 ? 0 : 2 }).format(number);
}

function formatActivityValue(key: string, value: unknown): string {
  if (value === null || value === undefined) return "";
  if (key.includes("date")) return formatDate(value);
  if (key.includes("price") || key.includes("value") || key.includes("amount")) return formatCurrency(value) || textValue(value);
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "";
  if (typeof value === "string") return value.trim();
  return "";
}

function cleanLabel(value: string): string {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .replace(/\bCik\b/g, "CIK");
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

function eyebrowFor(snapshot: SeoEntitySnapshot): string {
  if (snapshot.entity_type === "ticker") return "Stock Research";
  if (snapshot.entity_type === "member") return "Congress Trading Profile";
  return "Insider Trading Profile";
}

function primaryCode(snapshot: SeoEntitySnapshot): string {
  const payload = snapshot.payload ?? {};
  if (snapshot.entity_type === "ticker") return textValue(payload.symbol) || snapshot.entity_key;
  if (snapshot.entity_type === "insider") return textValue(payload.primary_symbol) || textValue(payload.reporting_cik);
  return textValue(payload.bioguide_id) || textValue(payload.state);
}

function summaryFor(snapshot: SeoEntitySnapshot): string {
  const payload = snapshot.payload ?? {};
  const title = entityTitle(snapshot);
  if (snapshot.entity_type === "ticker") {
    const symbol = textValue(payload.symbol) || snapshot.entity_key;
    const company = textValue(payload.company_name);
    return `Research ${company && company !== symbol ? `${company} (${symbol})` : symbol} using Walnut's public price, disclosure, ownership, contract, and signal context.`;
  }
  if (snapshot.entity_type === "member") {
    const state = textValue(payload.state);
    const chamber = textValue(payload.chamber);
    return `Review ${title}${state ? ` (${state})` : ""}${chamber ? ` ${chamber}` : ""} disclosure activity, traded securities, and related market context.`;
  }
  const symbol = textValue(payload.primary_symbol);
  const role = textValue(payload.primary_role);
  return `Review ${title}${role ? `, ${role}` : ""}${symbol ? `, and related ${symbol} Form 4 activity` : " Form 4 activity"} in Walnut.`;
}

function fieldsFor(snapshot: SeoEntitySnapshot): Field[] {
  const payload = snapshot.payload ?? {};
  const keysByType: Record<SeoEntitySnapshot["entity_type"], string[]> = {
    ticker: ["symbol", "exchange", "sector", "industry"],
    member: ["chamber", "state", "party", "bioguide_id"],
    insider: ["primary_symbol", "primary_role", "reporting_cik"],
  };
  return keysByType[snapshot.entity_type]
    .map((key) => ({ label: cleanLabel(key), value: textValue(payload[key]) }))
    .filter((field) => field.value);
}

function metricCards(snapshot: SeoEntitySnapshot): Field[] {
  const payload = snapshot.payload ?? {};
  const recentActivity = Array.isArray(payload.recent_activity) ? payload.recent_activity : [];
  if (snapshot.entity_type === "ticker") {
    const price = formatCurrency(payload.price);
    return [
      { label: "Last Price", value: price || "Available in terminal" },
      { label: "Price Date", value: formatDate(payload.price_date) || dataAsOf(snapshot) },
      { label: "Confirmation", value: textValue(payload.confirmation_score) || "Public baseline" },
      { label: "Activity", value: recentActivity.length ? `${recentActivity.length} recent item${recentActivity.length === 1 ? "" : "s"}` : "Disclosure context" },
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
  const buyCount = recentActivity.filter((row) => /buy|purchase|p/i.test(textValue(row.transaction_type))).length;
  const sellCount = recentActivity.filter((row) => /sell|sale|s/i.test(textValue(row.transaction_type))).length;
  return [
    { label: "Recent Filings", value: `${recentActivity.length}` },
    { label: "Buy Signals", value: `${buyCount}` },
    { label: "Sell Signals", value: `${sellCount}` },
    { label: "Updated", value: dataAsOf(snapshot) },
  ];
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
  if (cleanSections.length) return cleanSections.slice(0, 4);
  if (snapshot.entity_type === "ticker") {
    return [
      { label: "Confirmation", title: "Public Research Read", body: "Walnut brings price, volume, fundamentals, Congress activity, insider filings, and market context into one ticker workflow." },
      { label: "What Changed", title: "What changed recently", body: "Use the full terminal to inspect the latest activity, source movement, and confirmation details behind this public baseline." },
      { label: "Risks", title: "What can weaken the setup", body: "The public view preserves the research structure while deeper source detail remains gated by plan." },
      { label: "Next", title: "What to watch next", body: "Track whether disclosure activity, fundamentals, price action, and related catalysts continue to support the thesis." },
    ];
  }
  if (snapshot.entity_type === "member") {
    return [
      { label: "Disclosures", title: "Congress trading context", body: "Review recent public disclosures, timing, traded securities, and related ticker links in the same Walnut profile structure." },
      { label: "Performance", title: "Trade outcome workflow", body: "Portfolio and outcome modules stay in the product shell and unlock according to the current account plan." },
    ];
  }
  return [
    { label: "Form 4", title: "Insider activity context", body: "Review public Form 4 activity, transaction direction, issuer relationship, and related stock research from one Walnut profile." },
    { label: "Performance", title: "Transaction outcome workflow", body: "Deeper performance, ownership, and monitoring workflows stay visible as product modules with gated detail where required." },
  ];
}

function gatedCards(snapshot: SeoEntitySnapshot): Array<{ title: string; body: string; cta: string }> {
  if (snapshot.entity_type === "ticker") {
    return [
      { title: "Institutional Activity", body: "Track reported holders, position changes, and institutional accumulation with Walnut Pro.", cta: "Unlock Pro" },
      { title: "Analyst & Valuation Workflows", body: "Compare valuation, forward estimates, and analyst context inside the full terminal.", cta: "Launch Terminal" },
    ];
  }
  if (snapshot.entity_type === "member") {
    return [
      { title: "Portfolio Simulation", body: "Evaluate disclosure timing, holding periods, and benchmark-relative outcomes.", cta: "Unlock performance" },
      { title: "Member Monitoring", body: "Follow new disclosures and related ticker activity from your Walnut watchlists.", cta: "Sign in to follow" },
    ];
  }
  return [
    { title: "Ownership & Performance", body: "Inspect transaction outcomes, ownership context, and issuer-scoped activity with the full terminal.", cta: "Unlock analysis" },
    { title: "Insider Monitoring", body: "Follow new Form 4 filings and related ticker activity from your Walnut watchlists.", cta: "Sign in to follow" },
  ];
}

function ActivityTable({ rows, entityType }: { rows: ActivityRow[]; entityType: SeoEntitySnapshot["entity_type"] }) {
  if (!rows.length) {
    return (
      <section className={`${cardClassName} p-5`}>
        <h2 className="text-base font-semibold text-white">{entityType === "ticker" ? "Recent Activity" : entityType === "member" ? "Recent Disclosures" : "Recent Form 4 Activity"}</h2>
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
    <section className={`${cardClassName} overflow-hidden`}>
      <div className="border-b border-white/10 px-5 py-4">
        <h2 className="text-base font-semibold text-white">{entityType === "member" ? "Recent Disclosures" : entityType === "insider" ? "Recent Form 4 Activity" : "Recent Market Activity"}</h2>
      </div>
      <div className="divide-y divide-white/10">
        {rows.slice(0, 8).map((row, index) => {
          const values = preferredKeys
            .map((key) => [key, formatActivityValue(key, row[key])] as const)
            .filter(([, value]) => value);
          return (
            <div key={index} className="grid gap-3 px-5 py-4 text-sm sm:grid-cols-4">
              {values.map(([key, value]) => (
                <p key={key} className="min-w-0">
                  <span className="block text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">{cleanLabel(key)}</span>
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

function GatedModule({ title, body, cta }: { title: string; body: string; cta: string }) {
  return (
    <article className={`${subtleCardClassName} p-5`}>
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-semibold text-white">{title}</h3>
          <p className="mt-2 text-sm leading-6 text-slate-400">{body}</p>
        </div>
        <span className="rounded-full border border-emerald-300/30 bg-emerald-300/10 px-2.5 py-1 text-[11px] font-semibold text-emerald-200">Locked</span>
      </div>
      <Link href="/pricing" className="mt-4 inline-flex h-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-3 text-xs font-semibold text-slate-100 transition hover:border-emerald-300/50 hover:text-emerald-100">
        {cta}
      </Link>
    </article>
  );
}

export function SeoSnapshotBaseline({ snapshot }: { snapshot: SeoEntitySnapshot; eyebrow?: string }) {
  const payload = snapshot.payload ?? {};
  const links = Array.isArray(payload.links) ? payload.links : [];
  const recentActivity = Array.isArray(payload.recent_activity) ? payload.recent_activity : [];
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
    <main className="min-h-screen bg-[#050914] text-slate-100">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd).replace(/</g, "\\u003c") }} />
      <section className="border-b border-white/10 bg-slate-950/20">
        <div className="mx-auto grid w-full max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[minmax(0,1fr)_360px] lg:px-8">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-emerald-300">{eyebrowFor(snapshot)}</p>
            <div className="mt-4 flex flex-wrap items-end gap-3">
              <h1 className="max-w-3xl text-3xl font-semibold tracking-normal text-white sm:text-4xl">{title}</h1>
              {code ? <span className="rounded-md border border-white/10 bg-white/[0.03] px-2.5 py-1 text-sm font-semibold text-slate-300">{code}</span> : null}
            </div>
            <p className="mt-4 max-w-3xl text-sm leading-6 text-slate-300">{summaryFor(snapshot)}</p>
            <p className="mt-4 text-xs text-slate-500">Data as of {dataAsOf(snapshot)}</p>
            <div className="mt-6 flex flex-wrap gap-3">
              <Link href="/login" className="inline-flex h-10 items-center justify-center rounded-lg bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
                Sign in to personalize
              </Link>
              <Link href={snapshot.entity_type === "ticker" ? "/screener" : "/feed"} className="inline-flex h-10 items-center justify-center rounded-lg border border-white/10 px-4 text-sm font-semibold text-slate-100 transition hover:border-white/25 hover:bg-white/[0.04]">
                Explore terminal
              </Link>
            </div>
          </div>
          <aside className={`${cardClassName} p-5`}>
            <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-500">Profile</p>
            <dl className="mt-4 space-y-3 text-sm">
              {fieldsFor(snapshot).map((field) => (
                <div key={field.label} className="flex items-start justify-between gap-4">
                  <dt className="text-slate-500">{field.label}</dt>
                  <dd className="text-right font-medium text-slate-200">{field.value}</dd>
                </div>
              ))}
            </dl>
          </aside>
        </div>
        <nav className="mx-auto flex w-full max-w-7xl gap-2 overflow-x-auto px-4 pb-4 sm:px-6 lg:px-8" aria-label={`${title} sections`}>
          {navItemsByType[snapshot.entity_type].map((item, index) => (
            <a key={item} href={index === 0 ? "#overview" : "#gated-modules"} className="whitespace-nowrap rounded-lg border border-white/10 bg-white/[0.02] px-3 py-2 text-xs font-semibold text-slate-300 transition hover:border-white/20 hover:text-white">
              {item}
            </a>
          ))}
        </nav>
      </section>

      <section id="overview" className="mx-auto w-full max-w-7xl space-y-8 px-4 py-8 sm:px-6 lg:px-8">
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {metricCards(snapshot).map((metric) => (
            <article key={metric.label} className={`${cardClassName} p-4`}>
              <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{metric.label}</p>
              <p className="mt-2 text-xl font-semibold text-white">{metric.value}</p>
            </article>
          ))}
        </div>

        <div className="grid gap-4 lg:grid-cols-2">
          {overviewCards(snapshot).map((section) => (
            <article key={`${section.label}-${section.title}`} className={`${cardClassName} p-5`}>
              <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">{section.label}</p>
              <h2 className="mt-3 text-lg font-semibold text-white">{section.title}</h2>
              <p className="mt-3 text-sm leading-6 text-slate-300">{section.body}</p>
            </article>
          ))}
        </div>

        <ActivityTable rows={recentActivity} entityType={snapshot.entity_type} />

        <section id="gated-modules" className="grid gap-4 lg:grid-cols-2">
          {gatedCards(snapshot).map((module) => (
            <GatedModule key={module.title} {...module} />
          ))}
        </section>

        {links.length > 0 ? (
          <nav className={`${cardClassName} flex flex-wrap gap-3 p-5`} aria-label="Related Walnut research links">
            {links.map((link) => (
              <Link key={link.href} href={link.href} className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-emerald-200 transition hover:border-emerald-300/50 hover:text-emerald-100">
                {link.label}
              </Link>
            ))}
          </nav>
        ) : null}
      </section>
    </main>
  );
}
