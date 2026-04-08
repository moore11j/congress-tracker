function parseNumeric(value: unknown): number | null {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const cleaned = value.replace(/[$,% ,]/g, "").trim();
    if (!cleaned) return null;
    const parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseText(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.trim();
  return cleaned ? cleaned : null;
}

function titleCase(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1).toLowerCase();
}

export type SmartSignalValue = {
  score: number | null;
  band: string | null;
};

export type SmartSignalPillModel = SmartSignalValue & {
  compactLabel: string;
  fullLabel: string;
  className: string;
  dotClassName: string;
};

export function resolveSmartSignalValue(
  record: Record<string, unknown>,
  options?: {
    scoreKeys?: string[];
    bandKeys?: string[];
  },
): SmartSignalValue {
  const scoreKeys = options?.scoreKeys ?? ["smart_score", "smartScore"];
  const bandKeys = options?.bandKeys ?? ["smart_band", "smartBand"];

  let score: number | null = null;
  for (const key of scoreKeys) {
    score = parseNumeric(record[key]);
    if (score !== null) break;
  }

  let band: string | null = null;
  for (const key of bandKeys) {
    const value = parseText(record[key]);
    if (value) {
      band = value.toLowerCase();
      break;
    }
  }

  return { score, band };
}

export function smartSignalPillClasses(band?: string | null): string {
  const normalized = (band ?? "").toLowerCase();
  if (normalized === "strong") return "border-emerald-500/30 bg-emerald-500/10 text-emerald-200";
  if (normalized === "notable") return "border-amber-500/30 bg-amber-500/10 text-amber-200";
  if (normalized === "mild") return "border-orange-500/30 bg-orange-500/10 text-orange-200";
  return "border-slate-700 bg-slate-900/30 text-slate-300";
}

export function smartSignalDotClasses(band?: string | null): string {
  const normalized = (band ?? "").toLowerCase();
  if (normalized === "strong") return "bg-emerald-400";
  if (normalized === "notable") return "bg-amber-400";
  if (normalized === "mild") return "bg-orange-400";
  return "bg-slate-500";
}

export function buildSmartSignalPillModel(signal: SmartSignalValue): SmartSignalPillModel | null {
  const band = signal.band?.toLowerCase() ?? null;
  if (signal.score === null && !band) return null;

  const roundedScore = signal.score !== null ? Math.round(signal.score) : null;
  const bandLabel = band ? titleCase(band) : null;

  return {
    score: roundedScore,
    band,
    compactLabel: roundedScore !== null ? String(roundedScore) : (bandLabel ?? "—"),
    fullLabel: roundedScore !== null ? `Smart ${roundedScore}` : (bandLabel ?? "Smart"),
    className: smartSignalPillClasses(band),
    dotClassName: smartSignalDotClasses(band),
  };
}
