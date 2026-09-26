// Methodology rollout dates; daily history uses the rollout's UTC date.
// Daily score history uses UTC dates; keep these annotations on that same axis.
export const confirmationRecalibrations = [
  {
    version: "confirmation-v6-weighted-coverage",
    effectiveAt: "2026-09-26T00:00:00Z",
    description: "Confirmation now uses a fixed weighted 100-point model. Quiet, mixed and missing sources earn no points; full confirmation requires all sources. Historical scores are unchanged.",
  },
  {
    version: "confirmation-v4-source-priorities",
    effectiveAt: "2026-09-16T17:40:48.293741Z",
    description: "Source weights were recalibrated, including less weight for government contracts and more weight for insider buying than selling.",
  },
  {
    version: "confirmation-v5-net-evidence",
    effectiveAt: "2026-09-17T00:06:47.016497Z",
    description: "Opposing evidence is now deducted directly from confirmation. Separate activity bonuses were removed.",
  },
] as const;

export function scoreRecalibrationsInRange(points: readonly { date: string }[]) {
  if (points.length < 2) return [];
  const dates = points.map((point) => Date.parse(point.date.slice(0, 10)));
  if (dates.some((date, index) => !Number.isFinite(date) || (index > 0 && date <= dates[index - 1]))) return [];
  return confirmationRecalibrations.flatMap((event) => {
    const date = event.effectiveAt.slice(0, 10);
    const timestamp = Date.parse(date);
    if (timestamp < dates[0] || timestamp > dates[dates.length - 1]) return [];
    const right = dates.findIndex((value) => value >= timestamp);
    // The chart spaces observations evenly, so interpolate within that segment,
    // rather than treating the whole chart as a uniformly spaced time axis.
    const index = right === 0 ? 0 : right - 1 + (timestamp - dates[right - 1]) / (dates[right] - dates[right - 1]);
    return [{ ...event, date, index, inspectionIndex: right }];
  });
}
