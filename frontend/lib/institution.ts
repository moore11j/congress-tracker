const CIK_PATTERN = /^\d{1,10}$/;

export function normalizeInstitutionCik(value?: string | number | null): string | null {
  if (typeof value === "number") return String(value).padStart(10, "0");
  if (typeof value !== "string") return null;
  const cleaned = value.trim();
  if (!cleaned || !CIK_PATTERN.test(cleaned)) return null;
  return cleaned.padStart(10, "0");
}

export function institutionHref(cik?: string | number | null): string | null {
  const normalized = normalizeInstitutionCik(cik);
  return normalized ? `/institution/${encodeURIComponent(normalized)}` : null;
}
