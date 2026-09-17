/** Display strength and direction without calling a directional lean inactive. */
export function confirmationLabel(score: number | null | undefined, direction?: string | null, band?: string | null): string {
  if (score == null || !Number.isFinite(score)) return "Unavailable";
  const normalized = (direction ?? "neutral").toLowerCase();
  if (normalized === "unavailable") return "Unavailable";
  if (normalized === "mixed" || normalized === "conflicted") return "Conflicted confirmation";
  if (normalized !== "bullish" && normalized !== "bearish") return score <= 19 ? "Inactive" : "No clear direction";
  if (score <= 19) return `Weak ${normalized} lean`;
  const strength = band || (score <= 39 ? "weak" : score <= 59 ? "moderate" : score <= 79 ? "strong" : "exceptional");
  return `${strength.charAt(0).toUpperCase()}${strength.slice(1)} ${normalized.charAt(0).toUpperCase()}${normalized.slice(1)}`;
}
