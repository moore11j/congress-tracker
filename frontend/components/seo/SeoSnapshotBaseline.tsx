import Link from "next/link";
import type { SeoEntitySnapshot } from "@/lib/api";

function textValue(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function formatActivityValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "";
  if (typeof value === "string") return value.trim();
  return "";
}

export function SeoSnapshotBaseline({ snapshot, eyebrow }: { snapshot: SeoEntitySnapshot; eyebrow: string }) {
  const payload = snapshot.payload ?? {};
  const sections = Array.isArray(payload.sections) ? payload.sections : [];
  const links = Array.isArray(payload.links) ? payload.links : [];
  const recentActivity = Array.isArray(payload.recent_activity) ? payload.recent_activity : [];
  const title = textValue(payload.company_name) || textValue(payload.member_name) || textValue(payload.insider_name) || snapshot.entity_key;
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
      <section className="border-b border-white/10">
        <div className="mx-auto grid w-full max-w-6xl gap-8 px-4 py-12 sm:px-6 lg:grid-cols-[minmax(0,1fr)_320px] lg:px-8">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-emerald-300">{eyebrow}</p>
            <h1 className="mt-4 max-w-3xl text-3xl font-semibold tracking-normal text-white sm:text-4xl">{title}</h1>
            <p className="mt-4 max-w-3xl text-sm leading-6 text-slate-300">{snapshot.meta_description}</p>
            <p className="mt-4 text-xs text-slate-500">
              Snapshot generated {snapshot.generated_at ? new Date(snapshot.generated_at).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" }) : "from stored data"}.
            </p>
          </div>
          <aside className="rounded-lg border border-white/10 bg-white/[0.03] p-4">
            <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-500">Stored Snapshot</p>
            <dl className="mt-4 space-y-3 text-sm">
              {["symbol", "exchange", "sector", "industry", "primary_symbol", "chamber", "state", "party"].map((key) => {
                const value = textValue(payload[key]);
                if (!value) return null;
                return (
                  <div key={key} className="flex items-start justify-between gap-4">
                    <dt className="capitalize text-slate-500">{key.replaceAll("_", " ")}</dt>
                    <dd className="text-right font-medium text-slate-200">{value}</dd>
                  </div>
                );
              })}
            </dl>
          </aside>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-4 py-10 sm:px-6 lg:px-8">
        <div className="grid gap-4 md:grid-cols-2">
          {sections.map((section, index) => (
            <article key={`${section.heading}-${index}`} className="rounded-lg border border-white/10 bg-white/[0.03] p-5">
              <h2 className="text-base font-semibold text-white">{section.heading}</h2>
              <p className="mt-3 text-sm leading-6 text-slate-300">{section.body}</p>
            </article>
          ))}
        </div>

        {recentActivity.length > 0 ? (
          <section className="mt-8 rounded-lg border border-white/10 bg-white/[0.03] p-5">
            <h2 className="text-base font-semibold text-white">Recent stored activity</h2>
            <div className="mt-4 divide-y divide-white/10">
              {recentActivity.slice(0, 6).map((item, index) => {
                const values = Object.entries(item)
                  .map(([key, value]) => [key.replaceAll("_", " "), formatActivityValue(value)] as const)
                  .filter(([, value]) => value);
                return (
                  <div key={index} className="grid gap-2 py-3 text-sm sm:grid-cols-4">
                    {values.slice(0, 4).map(([key, value]) => (
                      <p key={key} className="min-w-0">
                        <span className="block text-[11px] uppercase tracking-[0.16em] text-slate-500">{key}</span>
                        <span className="block truncate text-slate-200">{value}</span>
                      </p>
                    ))}
                  </div>
                );
              })}
            </div>
          </section>
        ) : null}

        {links.length > 0 ? (
          <nav className="mt-8 flex flex-wrap gap-3" aria-label="Related Walnut research links">
            {links.map((link) => (
              <Link key={link.href} href={link.href} className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-emerald-200 hover:border-emerald-300/50">
                {link.label}
              </Link>
            ))}
          </nav>
        ) : null}
      </section>
    </main>
  );
}
