const CIK_PATTERN = /^\d{10}$/;

function cleanReportingCik(value?: string | null): string | null {
  if (!value) return null;
  const cleaned = value.trim();
  if (!cleaned || !CIK_PATTERN.test(cleaned)) return null;
  return cleaned;
}

function slugifyName(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-");
}

export function insiderSlug(name?: string | null, reportingCik?: string | null): string | null {
  const cik = cleanReportingCik(reportingCik);
  if (!cik) return null;
  const displayName = getInsiderDisplayName(name);
  const nameSlug = displayName ? slugifyName(displayName) : "";
  return nameSlug ? `${nameSlug}-${cik}` : cik;
}

export function insiderHref(name?: string | null, reportingCik?: string | null): string | null {
  const slug = insiderSlug(name, reportingCik);
  if (!slug) return null;
  return `/insider/${encodeURIComponent(slug)}`;
}

export function reportingCikFromInsiderSlug(slug?: string | null): string | null {
  if (!slug) return null;
  const cleaned = decodeURIComponent(slug).trim().toLowerCase().replace(/\/+$/, "");
  if (!cleaned) return null;
  if (CIK_PATTERN.test(cleaned)) return cleaned;
  const match = cleaned.match(/-(\d{10})$/);
  return match ? match[1] : null;
}

function cleanName(value?: string | null): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.replace(/\s+/g, " ").trim();
  return cleaned ? cleaned : null;
}

function isInitialToken(token: string): boolean {
  return /^[A-Za-z]\.?$/.test(token);
}

function isSuffixToken(token: string): boolean {
  return /^(JR|SR|II|III|IV|V)\.?$/i.test(token);
}

function isLikelyEntityName(name: string): boolean {
  if (/[,/&]/.test(name)) return true;
  return /\b(INC|LLC|LTD|LP|PLC|CORP|CORPORATION|HOLDINGS|PARTNERS|TRUST|CAPITAL|VENTURES|GROUP|CO|COMPANY)\b/i.test(name);
}

function isTitleCaseToken(token: string): boolean {
  return /^[A-Z][a-z'`.-]+$/.test(token);
}

const commonGivenNames = new Set([
  "James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles",
  "Christopher", "Daniel", "Matthew", "Anthony", "Mark", "Donald", "Steven", "Paul", "Andrew", "Joshua",
  "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen",
  "Nancy", "Lisa", "Margaret", "Betty", "Sandra", "Ashley", "Kimberly", "Emily", "Donna", "Michelle",
  "Meredith", "Peter", "Scott", "Kevin", "Brian", "Timothy", "Jason", "Jeffrey", "Ryan", "Jacob",
]);

function isLikelyGivenName(token: string): boolean {
  const normalized = token.replace(/\.$/, "");
  return commonGivenNames.has(normalized);
}

function toDisplayCase(name: string): string {
  if (name !== name.toUpperCase()) return name;
  return name
    .toLowerCase()
    .replace(/\b[\p{L}][\p{L}'`.-]*/gu, (word) => word.charAt(0).toUpperCase() + word.slice(1));
}

function reorderLikelyInvertedPersonName(name: string): string {
  if (isLikelyEntityName(name)) return name;
  const parts = name.split(" ");
  if (
    parts.length === 2 &&
    !isInitialToken(parts[0]) &&
    !isInitialToken(parts[1]) &&
    !isSuffixToken(parts[1]) &&
    isTitleCaseToken(parts[0]) &&
    isTitleCaseToken(parts[1]) &&
    !isLikelyGivenName(parts[0]) &&
    isLikelyGivenName(parts[1])
  ) {
    return `${parts[1]} ${parts[0]}`;
  }

  if (parts.length === 3) {
    if (!isInitialToken(parts[0]) && !isInitialToken(parts[1]) && isInitialToken(parts[2])) {
      return `${parts[1]} ${parts[2].replace(/\.$/, "")} ${parts[0]}`;
    }
    return name;
  }

  if (parts.length === 4 && isSuffixToken(parts[3]) && isInitialToken(parts[2])) {
    return `${parts[1]} ${parts[2].replace(/\.$/, "")} ${parts[0]} ${parts[3]}`;
  }

  return name;
}

export function normalizeInsiderName(name?: string | null): string | null {
  const cleaned = cleanName(name);
  if (!cleaned) return null;
  return reorderLikelyInvertedPersonName(toDisplayCase(cleaned));
}

export function getInsiderDisplayName(...candidates: Array<string | null | undefined>): string | null {
  for (const candidate of candidates) {
    const normalized = normalizeInsiderName(candidate);
    if (normalized) return normalized;
  }
  return null;
}
