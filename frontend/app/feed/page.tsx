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
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

type BadgeTone = "neutral" | "dem" | "rep" | "ind" | "house" | "senate" | "pos" | "neg";

function chamberBadge(raw?: string | null) {
  const v = (raw ?? "").toLowerCase();
  if (v === "house") return { text: "HOUSE", tone: "house" as const };
  if (v === "senate") return { text: "SENATE", tone: "senate" as const };
  return { text: "—", tone: "neutral" as const };
}

function partyAbbrev(raw?: string | null) {
  const v = (raw ?? "").trim().toLowerCase();
  if (!v) return null;
  if (v.startsWith("d")) return "D";
  if (v.startsWith("r")) return "R";
  if (v.includes("ind")) return "I";
  // If something unexpected comes through, keep it but shorten a bit
  return raw.trim().slice(0, 6).toUpperCase();
}

function partyTone(raw?: string | null): BadgeTone {
  const v = (raw ?? "").trim().toLowerCase();
  if (!v) return "neutral";
  if (v.startsWith("d")) return "dem";
  if (v.startsWith("r")) return "rep";
  if (v.includes("ind")) return "ind";
  return "neutral";
}

function memberTag(member: { party?: string | null; state?: string | null }) {
  const p = partyAbbrev(member.party);
  const s = member.state?.trim().toUpperCase() ?? null;
  if (p && s) return `${p}-${s}`; // D-IL
  if (s) return s;
  if (p) return p;
  return "Unknown";
}

function titleCase(raw: string) {
  return raw
    .trim()
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((w) => w.slice(0, 1).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

function formatTxnType(raw: string) {
  const v = raw?.toLowerCase?.() ?? "";
  if (!v) return "—";
  if (v === "purchase") return "Purchase";
  if (v === "sale") return "Sale";
  if (v === "exchange") return "Exchange";
  return titleCase(raw);
}

function moneyRange(min: number | null, max: number | null) {
  const a = min?.toLocaleString() ?? "—";
  const b = max?.toLocaleString() ?? "—";
  return `${a} – ${b}`;
}

function symbolText(sym?: string | null) {
  const s = (sym ?? "").trim().toUpperCase();
  return s.length ? s : "—";
}

function pillStyle(tone: BadgeTone): React.CSSProperties {
  const base: React.CSSProperties = {
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
    padding: "4px 10px",
    borderRadius: 999,
    fontSize: 12,
    fontWeight: 800,
    letterSpacing: 0.35,
    border: "1px solid rgba(0,0,0,0.10)",
    background: "rgba(0,0,0,0.03)",
    color: "rgba(0,0,0,0.78)",
    whiteSpace: "nowrap",
  };

  // Keep it tasteful (no loud colors), just subtle tints.
  if (tone === "dem") return { ...base, background: "rgba(59,130,246,0.10)", border: "1px solid rgba(59,130,246,0.25)" };
  if (tone === "rep") return { ...base, background: "rgba(239,68,68,0.10)", border: "1px solid rgba(239,68,68,0.25)" };
  if (tone === "ind") return { ...base, background: "rgba(16,185,129,0.10)", border: "1px solid rgba(16,185,129,0.22)" };

  if (tone === "house") return { ...base, background: "rgba(0,0,0,0.04)", border: "1px solid rgba(0,0,0,0.14)" };
  if (tone === "senate") return { ...base, background: "rgba(0,0,0,0.04)", border: "1px solid rgba(0,0,0,0.14)" };

  if (tone === "pos") return { ...base, background: "rgba(16,185,129,0.10)", border: "1px solid rgba(16,185,129,0.22)" };
  if (tone === "neg") return { ...base, background: "rgba(239,68,68,0.10)", border: "1px solid rgba(239,68,68,0.22)" };

  return base;
}

function inputStyle(): React.CSSProperties {
  return {
    width: "100%",
    padding: "10px 12px",
    borderRadius: 12,
    border: "1px solid rgba(0,0,0,0.12)",
    background: "white",
    outline: "none",
    fontSize: 14,
  };
}

function selectStyle(): React.CSSProperties {
  return {
    ...inputStyle(),
    appearance: "none",
  };
}

function buttonStyle(kind: "primary" | "ghost"): React.CSSProperties {
  if (kind === "ghost") {
    return {
      padding: "10px 12px",
      borderRadius: 12,
      border: "1px solid rgba(0,0,0,0.12)",
      background: "transparent",
      fontWeight: 800,
      cursor: "pointer",
      textAlign: "center",
      textDecoration: "none",
      color: "rgba(0,0,0,0.78)",
    };
  }
  return {
    padding: "10px 12px",
    borderRadius: 12,
    border: "1px solid rgba(0,0,0,0.12)",
    background: "rgba(0,0,0,0.90)",
    color: "white",
    fontWeight: 900,
    cursor: "pointer",
  };
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
  if (metaRes.ok) meta = (await metaRes.json()) as MetaResponse;

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
        minHeight: "100vh",
        background: "linear-gradient(180deg, rgba(0,0,0,0.04), rgba(0,0,0,0.00) 280px)",
        fontFamily: "ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial",
      }}
    >
      <div style={{ maxWidth: 1040, margin: "0 auto", padding: "28px 20px 40px" }}>
        {/* Top bar */}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 22 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div
              style={{
                width: 34,
                height: 34,
                borderRadius: 12,
                background: "rgba(0,0,0,0.9)",
                color: "white",
                display: "grid",
                placeItems: "center",
                fontWeight: 900,
                letterSpacing: 0.5,
              }}
              title="CapitolLedger"
            >
              CL
            </div>
            <div>
              <div style={{ fontWeight: 900, fontSize: 14, letterSpacing: 0.2 }}>CapitolLedger</div>
              <div style={{ fontSize: 12, opacity: 0.7 }}>Congress trades feed</div>
            </div>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div style={{ fontSize: 12, opacity: 0.7 }}>
              Last updated:{" "}
              <span style={{ fontWeight: 800, opacity: 0.9 }}>{formatLastUpdated(meta.last_updated_utc)}</span>
            </div>
            <a href="/feed" style={{ ...buttonStyle("ghost"), padding: "8px 10px" }}>
              Feed
            </a>
          </div>
        </div>

        {/* Page header */}
        <div style={{ marginBottom: 14 }}>
          <h1 style={{ fontSize: 34, lineHeight: 1.1, margin: 0, fontWeight: 950, letterSpacing: -0.6 }}>
            Congress Trades Feed
          </h1>
          <div style={{ marginTop: 8, fontSize: 14, opacity: 0.75 }}>
            Filter by ticker, member, chamber, transaction type, and minimum amount.
          </div>
        </div>

        {/* Filters card */}
        <form
          method="get"
          style={{
            border: "1px solid rgba(0,0,0,0.10)",
            background: "rgba(255,255,255,0.9)",
            borderRadius: 18,
            padding: 16,
            boxShadow: "0 10px 30px rgba(0,0,0,0.06)",
            marginBottom: 16,
          }}
        >
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1.2fr 1.4fr 0.8fr 0.9fr",
              gap: 12,
              alignItems: "end",
            }}
          >
            <div>
              <div style={{ fontSize: 12, fontWeight: 800, opacity: 0.7, marginBottom: 6 }}>Symbol</div>
              <input name="symbol" placeholder="NVDA" defaultValue={symbol} style={inputStyle()} />
            </div>

            <div>
              <div style={{ fontSize: 12, fontWeight: 800, opacity: 0.7, marginBottom: 6 }}>Member</div>
              <input name="member" placeholder="Pelosi" defaultValue={member} style={inputStyle()} />
            </div>

            <div>
              <div style={{ fontSize: 12, fontWeight: 800, opacity: 0.7, marginBottom: 6 }}>Chamber</div>
              <select name="chamber" defaultValue={chamber} style={selectStyle()}>
                <option value="">All chambers</option>
                <option value="house">House</option>
                <option value="senate">Senate</option>
              </select>
            </div>

            <div>
              <div style={{ fontSize: 12, fontWeight: 800, opacity: 0.7, marginBottom: 6 }}>Type</div>
              <select name="transaction_type" defaultValue={transaction_type} style={selectStyle()}>
                <option value="">All types</option>
                <option value="purchase">Purchase</option>
                <option value="sale">Sale</option>
                <option value="exchange">Exchange</option>
              </select>
            </div>

            <div>
              <div style={{ fontSize: 12, fontWeight: 800, opacity: 0.7, marginBottom: 6 }}>Min amount</div>
              <input name="min_amount" placeholder="50000" defaultValue={min_amount} style={inputStyle()} />
            </div>

            <div>
              <div style={{ fontSize: 12, fontWeight: 800, opacity: 0.7, marginBottom: 6 }}>Limit</div>
              <input name="limit" placeholder="50" defaultValue={limit} style={inputStyle()} />
            </div>

            {/* reset cursor whenever filters change */}
            <input type="hidden" name="cursor" value="" />

            <div style={{ display: "flex", gap: 10, justifyContent: "flex-end", gridColumn: "span 2" }}>
              <button type="submit" style={buttonStyle("primary")}>
                Apply
              </button>
              <a href="/feed" style={buttonStyle("ghost")}>
                Clear
              </a>
            </div>
          </div>
        </form>

        {/* Feed list */}
        <div style={{ display: "grid", gap: 12 }}>
          {data.items.length === 0 ? (
            <div
              style={{
                border: "1px dashed rgba(0,0,0,0.18)",
                background: "rgba(255,255,255,0.65)",
                borderRadius: 18,
                padding: 18,
                textAlign: "center",
                opacity: 0.85,
              }}
            >
              <div style={{ fontWeight: 900, fontSize: 14 }}>No results</div>
              <div style={{ marginTop: 6, fontSize: 13 }}>Try clearing filters or reducing min amount.</div>
            </div>
          ) : (
            data.items.map((it) => {
              const ch = chamberBadge(it.member.chamber);
              const pt = partyTone(it.member.party);

              const txn = (it.transaction_type ?? "").toLowerCase();
              const txnTone: BadgeTone = txn === "sale" ? "neg" : txn === "purchase" ? "pos" : "neutral";

              return (
                <div
                  key={it.id}
                  style={{
                    border: "1px solid rgba(0,0,0,0.10)",
                    background: "rgba(255,255,255,0.92)",
                    borderRadius: 18,
                    padding: 16,
                    boxShadow: "0 12px 34px rgba(0,0,0,0.06)",
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 14, alignItems: "flex-start" }}>
                    <div style={{ minWidth: 0 }}>
                      {/* Name + pills */}
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center" }}>
                        <div style={{ fontWeight: 950, fontSize: 16, letterSpacing: -0.2 }}>{it.member.name}</div>
                        <span style={pillStyle(pt)}>{memberTag(it.member)}</span>
                        <span style={pillStyle(ch.tone)}>{ch.text}</span>
                      </div>

                      {/* Security */}
                      <div style={{ marginTop: 8, display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center" }}>
                        <span
                          style={{
                            ...pillStyle("neutral"),
                            fontWeight: 950,
                            fontSize: 12,
                            padding: "4px 10px",
                            background: "rgba(0,0,0,0.06)",
                            border: "1px solid rgba(0,0,0,0.14)",
                          }}
                        >
                          {symbolText(it.security.symbol)}
                        </span>
                        <div style={{ fontSize: 14, opacity: 0.9, minWidth: 0 }}>
                          <span style={{ fontWeight: 800 }}>{it.security.name}</span>
                          <span style={{ opacity: 0.6 }}> · {it.security.asset_class}</span>
                          {it.security.sector ? <span style={{ opacity: 0.6 }}> · {it.security.sector}</span> : null}
                        </div>
                      </div>

                      {/* Dates */}
                      <div style={{ marginTop: 10, fontSize: 13, opacity: 0.75 }}>
                        Trade date: <span style={{ fontWeight: 800 }}>{it.trade_date ?? "—"}</span>
                        <span style={{ opacity: 0.45 }}> · </span>
                        Report date: <span style={{ fontWeight: 800 }}>{it.report_date ?? "—"}</span>
                      </div>
                    </div>

                    {/* Right rail */}
                    <div style={{ textAlign: "right", flexShrink: 0 }}>
                      <div style={{ display: "flex", justifyContent: "flex-end" }}>
                        <span style={pillStyle(txnTone)}>{formatTxnType(it.transaction_type)}</span>
                      </div>
                      <div style={{ marginTop: 10, fontWeight: 950, fontSize: 15 }}>
                        {moneyRange(it.amount_range_min, it.amount_range_max)}
                      </div>
                      <div style={{ marginTop: 6, fontSize: 12, opacity: 0.6 }}>{titleCase(it.owner_type ?? "")}</div>
                    </div>
                  </div>
                </div>
              );
            })
          )}
        </div>

        {/* Pagination */}
        <div style={{ marginTop: 18, display: "flex", gap: 12, alignItems: "center", justifyContent: "space-between" }}>
          <div style={{ fontSize: 12, opacity: 0.7 }}>
            Showing <span style={{ fontWeight: 900 }}>{data.items.length}</span> item(s)
          </div>

          {data.next_cursor ? (
            <Link
              href={`/feed?${nextLinkParams.toString()}`}
              style={{
                ...buttonStyle("primary"),
                textDecoration: "none",
                display: "inline-flex",
                alignItems: "center",
                gap: 8,
              }}
            >
              Load next <span style={{ fontWeight: 900 }}>→</span>
            </Link>
          ) : (
            <span style={{ opacity: 0.65, fontSize: 13 }}>No more results.</span>
          )}
        </div>
      </div>
    </div>
  );
}
