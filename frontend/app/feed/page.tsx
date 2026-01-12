// frontend/app/feed/page.tsx
import Link from "next/link";

type FeedItem = {
  id: number;
  member: {
    bioguide_id: string;
    name: string;
    chamber: string;
    party?: string | null;
    state?: string | null;
  };
  security: { symbol?: string | null; name: string; asset_class: string; sector?: string | null };
  transaction_type: string;
  owner_type: string;
  trade_date: string | null;
  report_date: string | null;
  amount_range_min: number | null;
  amount_range_max: number | null;
};

type FeedResponse = { items: FeedItem[]; next_cursor: string | null };

function buildApiUrl(base: string, params: Record<string, string | undefined>) {
  const u = new URL("/api/feed", base);
  Object.entries(params).forEach(([k, v]) => {
    if (v && v.trim().length > 0) u.searchParams.set(k, v.trim());
  });
  return u.toString();
}

function chamberLabel(chamber: string | undefined) {
  const c = (chamber || "").toLowerCase();
  if (c === "house") return "HOUSE";
  if (c === "senate") return "SENATE";
  return "UNKNOWN";
}

function partyLabel(party: string | null | undefined) {
  const p = (party || "").trim().toUpperCase();
  if (p === "D" || p === "R" || p === "I") return p;
  if (p.length > 0) return p;
  return "?";
}

function stateLabel(state: string | null | undefined) {
  const s = (state || "").trim().toUpperCase();
  return s.length > 0 ? s : "?";
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

  const apiBase = process.env.NEXT_PUBLIC_API_BASE!;
  const url = buildApiUrl(apiBase, {
    symbol,
    member,
    chamber,
    transaction_type,
    min_amount,
    cursor,
    limit,
  });

  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Feed fetch failed (${res.status}). URL=${url}. Body=${body}`);
  }
  const data = (await res.json()) as FeedResponse;

  const nextLinkParams = new URLSearchParams();
  if (symbol) nextLinkParams.set("symbol", symbol);
  if (member) nextLinkParams.set("member", member);
  if (chamber) nextLinkParams.set("chamber", chamber);
  if (transaction_type) nextLinkParams.set("transaction_type", transaction_type);
  if (min_amount) nextLinkParams.set("min_amount", min_amount);
  nextLinkParams.set("limit", limit);
  if (data.next_cursor) nextLinkParams.set("cursor", data.next_cursor);

  return (
    <div
      style={{
        maxWidth: 980,
        margin: "0 auto",
        padding: 24,
        fontFamily: "system-ui, -apple-system, Segoe UI, Roboto",
      }}
    >
      <h1 style={{ fontSize: 28, fontWeight: 700, marginBottom: 12 }}>Congress Trades Feed</h1>

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

        <button type="submit" style={{ padding: 10, fontWeight: 600 }}>
          Apply
        </button>
        <a href="/feed" style={{ padding: 10, textAlign: "center" }}>
          Clear
        </a>
      </form>

      <div style={{ display: "grid", gap: 10 }}>
        {data.items.map((it) => {
          const chamberText = chamberLabel(it.member.chamber);
          const party = partyLabel(it.member.party);
          const state = stateLabel(it.member.state);

          return (
            <div key={it.id} style={{ border: "1px solid #ddd", borderRadius: 10, padding: 12 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                <div>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
                    <div style={{ fontWeight: 700 }}>
                      {it.member.name} <span style={{ fontWeight: 600, opacity: 0.85 }}>({party}-{state})</span>
                    </div>

                    <span
                      style={{
                        display: "inline-flex",
                        alignItems: "center",
                        border: "1px solid #bbb",
                        borderRadius: 999,
                        padding: "2px 10px",
                        fontSize: 12,
                        fontWeight: 700,
                        letterSpacing: 0.3,
                        opacity: 0.9,
                      }}
                      title={`Chamber: ${it.member.chamber}`}
                    >
                      {chamberText}
                    </span>
                  </div>

                  <div style={{ opacity: 0.85, marginTop: 2 }}>
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
          );
        })}
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
