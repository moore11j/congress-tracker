function normalized(value?: string | null) {
  return (value ?? "").trim().toLowerCase();
}

export function chamberTextClassName(chamber?: string | null) {
  const value = normalized(chamber);
  if (value === "house") return "text-sky-300";
  if (value === "senate") return "text-violet-300";
  return "text-slate-400";
}

export function partyTextClassName(party?: string | null) {
  const value = normalized(party);
  if (value.startsWith("d")) return "text-sky-400";
  if (value.startsWith("r")) return "text-rose-400";
  if (value.startsWith("i") || value.includes("ind")) return "text-violet-300";
  return "text-slate-400";
}

export function insiderRoleTextClassName(role?: string | null) {
  const value = normalized(role).toUpperCase();
  if (["CEO", "CFO", "COO", "CTO", "CCO", "CLO", "CAO"].includes(value)) return "text-emerald-300";
  if (["EVP", "SVP", "VP", "PRES"].includes(value)) return "text-violet-300";
  if (value === "DIR") return "text-sky-300";
  if (value === "OFFICER") return "text-amber-300";
  return "text-slate-400";
}

export function tradeTypeTextClassName(tradeType?: string | null) {
  const value = normalized(tradeType);
  if (["purchase", "buy", "p", "p-purchase"].includes(value)) return "text-emerald-300";
  if (["sale", "sell", "s", "s-sale"].includes(value)) return "text-rose-300";
  return "text-slate-300";
}

export function signalWeightTextClassName(band?: string | null) {
  const value = normalized(band);
  if (value.includes("strong")) return "text-emerald-300";
  if (value.includes("notable")) return "text-amber-300";
  if (value.includes("mild")) return "text-orange-300";
  return "text-slate-400";
}

export function TickerActivitySignalScore({
  score,
  band,
  unlocked,
}: {
  score?: number | null;
  band?: string | null;
  unlocked: boolean;
}) {
  const scoreLabel = typeof score === "number" && Number.isFinite(score) ? Math.round(score).toString() : "—";
  return (
    <span
      className={`font-mono text-xs font-semibold tabular-nums ${unlocked ? signalWeightTextClassName(band) : "text-slate-500"}`}
      title={unlocked ? undefined : "Signal score requires Premium"}
    >
      {unlocked ? scoreLabel : "Locked"}
    </span>
  );
}
