export const dynamic = "force-dynamic";

type FeedItem = {
  id: number;
  member: { name: string; chamber: string; party?: string | null; state?: string | null };
  security: { symbol?: string | null; name: string; asset_class: string; sector?: string | null };
  transaction_type: string;
  owner_type: string;
  trade_date?: string | null;
  report_date?: string | null;
  amount_range_min?: number | null;
  amount_range_max?: number | null;
};

export default async function FeedPage() {
  const base = process.env.NEXT_PUBLIC_API_BASE || "";
  const res = await fetch(`${base}/api/feed`, { cache: "no-store" });
  const data = (await res.json()) as { items: FeedItem[] };

  return (
    <main>
      <h2 style={{ marginTop: 0 }}>Latest Trades</h2>

      {!data.items?.length ? (
        <p>No items yet. Try seeding demo data.</p>
      ) : (
        <div style={{ display: "grid", gap: 12 }}>
          {data.items.map((x) => (
            <div key={x.id} style={{ border: "1px solid #ddd", borderRadius: 12, padding: 12 }}>
              <div style={{ fontWeight: 700 }}>
                {x.member.name} ({x.member.chamber.toUpperCase()} {x.member.state || ""})
              </div>
              <div>
                <strong>{x.security.symbol || "—"}</strong> — {x.security.name}
              </div>
              <div style={{ marginTop: 6 }}>
                <span style={{ padding: "2px 8px", borderRadius: 999, border: "1px solid #ccc" }}>
                  {x.transaction_type.toUpperCase()}
                </span>{" "}
                <span style={{ color: "#555" }}>
                  {x.trade_date ? `Trade: ${x.trade_date}` : ""} {x.report_date ? ` • Filed: ${x.report_date}` : ""}
                </span>
              </div>
              <div style={{ marginTop: 6, color: "#333" }}>
                Range: {x.amount_range_min?.toLocaleString() || "?"} – {x.amount_range_max?.toLocaleString() || "?"}
              </div>
            </div>
          ))}
        </div>
      )}
    </main>
  );
}
