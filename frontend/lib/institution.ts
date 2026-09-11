const CIK_PATTERN = /^\d{1,10}$/;

const INSTITUTION_BRANDS: Record<string, string> = {
  blackrock: "BlackRock", jpmorgan: "JPMorgan", invesco: "Invesco",
  dimensional: "Dimensional", vanguard: "Vanguard", morgan: "Morgan",
};
const INSTITUTION_ACRONYMS = new Set("LLC LLP LP PLC AG SA UBS FMR BNY BNP HSBC RBC TD CIBC BMO TIAA US USA UK PNC KKR AQR DWS DNB SEI GMO CWA CWM LPL RIA ETF ETFS".split(" "));
const SMALL_WORDS = new Set(["of", "and", "the", "for", "de", "van", "von"]);

/** Presentation only: preserve identity, punctuation, acronyms and mixed-case brands. */
export function institutionDisplayName(value?: string | null): string | null {
  if (!value?.trim()) return null;
  const cleaned = value.trim().replace(/\s+/g, " ");
  return cleaned.replace(/[A-Za-z]+(?:['’][A-Za-z]+)?/g, (token, offset) => {
    const lower = token.toLowerCase();
    if (offset > 0 && cleaned[offset - 1] === "/" && cleaned[offset + token.length] === "/") return token.toUpperCase();
    if (INSTITUTION_BRANDS[lower]) return INSTITUTION_BRANDS[lower];
    if (INSTITUTION_ACRONYMS.has(token.toUpperCase())) return token.toUpperCase();
    if (token !== token.toUpperCase() && token !== lower) return token;
    if (SMALL_WORDS.has(lower) && offset > 0) return lower;
    return lower.replace(/(^|['’])([a-z])/g, (_match: string, prefix: string, letter: string) => prefix + letter.toUpperCase());
  });
}

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
