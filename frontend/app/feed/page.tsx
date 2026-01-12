import Link from "next/link";

/* =========================
   Types
========================= */
type FeedItem = {
  id: number;
  member: {
    bioguide_id: string;
    name: string;
    chamber: string;
    party?: string | null;
    state?: string | null;
  };
  security: {
    symbol?: string | null;
    name: string;
    asset_class: string;
    sector?: string | null;
  };
  transaction_type: string;
  owner_type: string;
  trade_date: string | null;
  report_date: string | null;
  amount_range_min: number | null;
  amount_range_max: number | null;
};

type FeedResponse = { items: FeedItem[]; next_cursor: string | null };
type MetaResponse = { last_updated_utc: string | null };

/* =========================
   Helpers
========================= */
function buildApiUrl(base: string, path: string, params?: Record<string, string | undefined>) {
  const u = new URL(path, base);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v && v.trim()) u.searchParams.set(k, v.trim());
    });
  }
  return u.toString();
}

function formatLastUpdated(iso: string | null) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function partyAbbrev(p?: string | null) {
  if (!p) return "—";
  const v = p.toLowerCase();
  if (v.startsWith("d")) return "D";
  if (v.startsWith("r")) return "R";
  if (v.includes("ind")) return "I";
  return p.toUpperCase();
}

function partyTone(p?: string | null) {
  if (!p) return "neutral";
  const v = p.toLowerCase();
  if (v.startsWith("d")) return "dem";
  if (v.startsWith("r")) return "rep";
  return "ind";
}

function chamberLabel(c?: string | null) {
  const v = (c ?? "").toLowerCase();
  if (v === "house") return "HOUSE";
  if (v === "senate") return "SENATE";
  return "—";
}

function pill(tone: "dem" | "rep" | "ind" | "house" | "senate" | "neutral") {
  const base: React.CSSProperties = {
    padding: "4px 10px",
    borderRadius: 999,
    fontSize: 12,
    fontWeight: 800,
    letterSpacing: 0.4,
    border: "1px solid rgba(0,0,0,0.15)",
    background: "rgba(0,0,0,0.04)",
  };

  if (tone === "dem") return { ...base, background: "rgba(59,130,246,.12)" };
  if (tone === "rep") return { ...base, background: "rgba(239,68,68,.12)" };
  if (tone === "ind") return { ...base, background: "rgba(16,185,129,.12)" };
  if (tone === "house") return { ...base };
  if (tone === "senate") return { ...base };

  return base;
}

function money(min: number | null, max: number | null) {
  return `${min?.toLocaleString() ?? "—"} – ${max?.toLocaleString() ?? "—"}`;
}

/* =========================
   Page
========================= */
export default async function FeedPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const sp = await searchParams;

  const symbol = typeof sp.symbol === "string" ? sp.symbol : "";
  const member = typeof sp.member === "string" ? sp.member : "";
  const chamber = typeof sp.chamber === "string" ? sp.chamber : "";
  const transaction_type = typeof sp.transaction_type === "string" ? sp.transaction_type : "";
  const min_amount = typeof sp.min_amount === "string" ? sp.min_amount : "";
  const cursor = typeof sp.cursor === "string" ? sp.cursor : "";
  const limit = typeof sp.limit === "string" ? sp.limit : "50";

  const apiBase =
    process.env.API_BASE_URL ??
    process.env.NEXT_PUBLIC_API_BASE_URL ??
    "https://congress-tracker-api.fly.dev";

  const feedUrl = buildApiUrl(apiBase, "/api/feed", {
    symbol,
    member,
    chamber,
    transaction_type,
    min_amount,
    cursor,
    limit,
  });

  const metaUrl = buildApiUrl(apiBase, "/api/meta");

  const [feedRes, metaRes] = await Promise.all([
    fetch(feedUrl, { cache: "no-store" }),
    fetch(metaUrl, { cache: "no-store" }),
  ]);

  if (!feedRes.ok) throw new Error("Feed fetch failed");

  const data = (await feedRes.json()) as FeedResponse;
  const meta = metaRes.ok ? ((await metaRes.json()) as MetaResponse) : { last_updated_utc: null };

  const next = new URLSearchParams();
  if (data.next_cursor) next.set("cursor", data.next_cursor);

  return (
    <div style={{ maxWidth: 1050, margin: "0 auto", padding: 28, fontFamily: "system-ui" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 18 }}>
        <h1 style={{ fontSize: 34, fontWeight: 900 }}>Congress Trades</h1>
        <div style={{ fontSize: 13, opacity: 0.7 }}>
          Updated: <b>{formatLastUpdated(meta.last_updated_utc)}</b>
        </div>
      </div>

      <div style={{ display: "grid", gap: 14 }}>
        {data.items.map((it) => (
          <div
            key={it.id}
            style={{
              background: "white",
              borderRadius: 18,
              padding: 16,
              boxShadow: "0 12px 30px rgba(0,0,0,0.08)",
              border: "1px solid rgba(0,0,0,0.1)",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", gap: 16 }}>
              <div>
                <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
                  <div style={{ fontWeight: 900, fontSize: 16 }}>{it.member.name}</div>
                  <span style={pill(partyTone(it.member.party))}>
                    {partyAbbrev(it.member.party)}-{it.member.state ?? "—"}
                  </span>
                  <span style={pill(it.member.chamber === "house" ? "house" : "senate")}>
                    {chamberLabel(it.member.chamber)}
                  </span>
                </div>

                <div style={{ marginTop: 6, fontSize: 14 }}>
                  <b>{(it.security.symbol ?? "").toUpperCase()}</b> · {it.security.name}
                </div>

                <div style={{ marginTop: 6, fontSize: 13, opacity: 0.75 }}>
                  Trade: {it.trade_date ?? "—"} · Report: {it.report_date ?? "—"}
                </div>
              </div>

              <div style={{ textAlign: "right" }}>
                <div style={{ fontWeight: 900 }}>{it.transaction_type.toUpperCase()}</div>
                <div style={{ marginTop: 6 }}>{money(it.amount_range_min, it.amount_range_max)}</div>
              </div>
            </div>
          </div>
        ))}
      </div>

      <div style={{ marginTop: 20 }}>
        {data.next_cursor ? (
          <Link href={`/feed?${next.toString()}`} style={{ fontWeight: 900 }}>
            Load more →
          </Link>
        ) : (
          <span style={{ opacity: 0.6 }}>No more results</span>
        )}
      </div>
    </div>
  );
}
