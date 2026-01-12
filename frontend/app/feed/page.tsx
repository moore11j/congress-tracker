// frontend/app/feed/page.tsx
import Link from "next/link";

type FeedItem = {
  id: number;
  member: { bioguide_id: string; name: string; chamber: string; party?: string | null; state?: string | null };
  security: { symbol?: string | null; name: string; asset_class: string; sector?: string | null };
  transaction_type: string;
  owner_type: string;
  trade_date: string | null;
  report_date: string | null;
  amount_range_min: number | null;
  amount_range_max: number | null;
};

type FeedResponse = { items: FeedItem[]; next_cursor: string | null };
type MetaResponse = { last_updated_utc: string | null };

function buildApiUrl(base: string, path: string, params?: Record<string, string | undefined>) {
  const u = new URL(path, base);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v && v.trim().length > 0) u.searchParams.set(k, v.trim());
    });
  }
  return u.toString();
}

function formatLastUpdated(iso: string | null) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  // Local time on the viewer’s machine
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatChamber(raw?: string | null) {
  const v = (raw ?? "").toLowerCase();
  if (v === "house") return "HOUSE";
  if (v === "senate") return "SENATE";
  return "—";
}

function formatParty(raw?: string | null) {
  const v = (raw ?? "").toLowerCase();
  if (!v) return null;
  if (v.startsWith("d")) return "Democrat";
  if (v.startsWith("r")) return "Republican";
  if (v.includes("ind")) return "Independent";
  return raw;
}

function formatMemberMeta(member: {
  party?: string | null;
  state?: string | null;
}) {
  const party = formatParty(member.party);
  const state = member.state?.toUpperCase() ?? null;

  if (party && state) return `${party}-${state}`;
  if (party) return party;
  if (state) return state;
  return "Unknown";
}

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

  if (!feedRes.ok) {
    const body = await feedRes.text();
    throw new Error(`Feed fetch failed (${feedRes.status}). URL=${feedUrl}. Body=${body}`);
  }
  const data = (await feedRes.json()) as FeedResponse;

  let meta: MetaResponse = { last_updated_utc: null };
  if (metaRes.ok) {
    meta = (await metaRes.json()) as MetaResponse;
  }

  const nextLinkParams = new URLSearchParams();
  if (symbol) nextLinkParams.set("symbol", symbol);
  if (member) nextLinkParams.set("member", member);
  if (chamber) nextLinkParams.set("chamber", chamber);
  if (transaction_type) nextLinkParams.set("transaction_type", transaction_type);
  if (min_amount) nextLinkParams.set("min_amount", min_amount);
  nextLinkParams.set("limit", limit);
  if (data.next_cursor) nextLinkParams.set("cursor", data.next_cursor);

  return (
    <div style={{ maxWidth: 980, margin: "0 auto", padding: 24, fontFamily: "system-ui, -apple-system, Segoe UI, Roboto" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 12 }}>
        <h1 style={{ fontSize: 28, fontWeight: 700, marginBottom: 4 }}>Congress Trades Feed</h1>
        <div style={{ fontSize: 13, opacity: 0.75 }}>
          Last updated: <span style={{ fontWeight: 600 }}>{formatLastUpdated(meta.last_updated_utc)}</span>
        </div>
      </div>

      {/* Filters (simple GET form updates URL) */}
      <form method="get" style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12, marginBottom: 18 }}>
        <input name="symbol" placeholder="Symbol (e.g., NVDA)" defaultValue={symbol} style={{ padding: 10 }} />
        <input name="member" placeholder="Member name (e.g., Pelosi)" defaultValue={member} style={{ padding: 10 }} />
        <select name="chamber" defaultValue={chamber} style={{ padding: 10 }}>
          <option value="">All chambers</option>
          <option value="house">House</option>
          <option value="senate">Senate</option>
        </select>
        <select name="transaction_type" defaultValue={transaction_type} style={{ padding: 10 }}>
          <option value="">All types</option>
          <option value="purchase">Purchase</option>
          <option value="sale">Sale</option>
          <option value="exchange">Exchange</option>
        </select>

        <input name="min_amount" placeholder="Min amount (e.g., 50000)" defaultValue={min_amount} style={{ padding: 10 }} />
        <input name="limit" placeholder="Limit (max 200)" defaultValue={limit} style={{ padding: 10 }} />
        {/* reset cursor whenever filters change */}
        <input type="hidden" name="cursor" value="" />

        <button type="submit" style={{ padding: 10, fontWeight: 600 }}>Apply</button>
        <a href="/feed" style={{ padding: 10, textAlign: "center" }}>Clear</a>
      </form>

      <div style={{ display: "grid", gap: 10 }}>
        {data.items.map((it) => (
          <div key={it.id} style={{ border: "1px solid #ddd", borderRadius: 10, padding: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <div>
                <div style={{ fontWeight: 700 }}>
                  {it.member.name} ({formatMemberMeta(it.member)}) — {formatChamber(it.member.chamber)}
                </div>
                <div style={{ opacity: 0.85 }}>
                  <span style={{ fontWeight: 600 }}>{(it.security.symbol || "").toUpperCase()}</span>{" "}
                  {it.security.name}
                </div>
              </div>
              <div style={{ textAlign: "right" }}>
                <div style={{ fontWeight: 700 }}>{it.transaction_type}</div>
                <div style={{ opacity: 0.85 }}>
                  {it.amount_range_min?.toLocaleString() ?? "?"} – {it.amount_range_max?.toLocaleString() ?? "?"}
                </div>
              </div>
            </div>

            <div style={{ marginTop: 8, opacity: 0.85 }}>
              Trade date: {it.trade_date ?? "?"} • Report date: {it.report_date ?? "?"}
            </div>
          </div>
        ))}
      </div>

      <div style={{ marginTop: 18, display: "flex", gap: 12, alignItems: "center" }}>
        {data.next_cursor ? (
          <Link href={`/feed?${nextLinkParams.toString()}`} style={{ fontWeight: 700 }}>
            Load next →
          </Link>
        ) : (
          <span style={{ opacity: 0.7 }}>No more results.</span>
        )}
      </div>
    </div>
  );
}
