import type { BadgeTone } from "@/components/Badge";

export type InsiderRoleCode =
  | "CEO"
  | "CFO"
  | "COO"
  | "CTO"
  | "CCO"
  | "CLO"
  | "CAO"
  | "EVP"
  | "SVP"
  | "PRES"
  | "VP"
  | "DIR"
  | "OFFICER"
  | "INSIDER";

function normalizeRoleText(raw?: string | null): string {
  return (raw ?? "")
    .toUpperCase()
    .replace(/[.,/()_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeNameLikeText(raw?: string | null): string {
  return (raw ?? "")
    .replace(/^[\s.,;:()\-_/]+|[\s.,;:()\-_/]+$/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

export function firstNonEmptyInsiderRole(...candidates: Array<string | null | undefined>): string | null {
  for (const candidate of candidates) {
    const normalized = normalizeNameLikeText(candidate);
    if (normalized) return normalized;
  }
  return null;
}

function canonicalizeComparisonText(raw?: string | null): string {
  return normalizeNameLikeText(raw)
    .toUpperCase()
    .replace(/[.,;:()\-_/]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isRoleLikeLabel(raw?: string | null): boolean {
  const canonical = canonicalizeComparisonText(raw);
  if (!canonical) return false;
  if (canonical === "INSIDER") return true;
  return normalizeInsiderRoleBadge(canonical) !== "INSIDER";
}

export function normalizeInsiderRoleBadge(raw?: string | null): InsiderRoleCode {
  const s = normalizeRoleText(raw || "INSIDER");
  if (/\bCHIEF EXECUTIVE OFFICER\b|\bCEO\b/.test(s)) return "CEO";
  if (/\bCHIEF FINANCIAL OFFICER\b|\bCFO\b/.test(s)) return "CFO";
  if (/\bCHIEF OPERATING OFFICER\b|\bCOO\b/.test(s)) return "COO";
  if (/\bCHIEF TECHNOLOGY OFFICER\b|\bCTO\b/.test(s)) return "CTO";
  if (/\bCHIEF COMPLIANCE OFFICER\b|\bCCO\b/.test(s)) return "CCO";
  if (/\bCHIEF LEGAL OFFICER\b|\bCLO\b/.test(s)) return "CLO";
  if (/\bCHIEF ACCOUNTING OFFICER\b|\bCAO\b/.test(s)) return "CAO";
  if (/\bEXECUTIVE VICE PRESIDENT\b|\bEXEC\s+VP\b|\bEVP\b/.test(s)) return "EVP";
  if (/\bSENIOR VICE PRESIDENT\b|\bSR\s+VP\b|\bSVP\b/.test(s)) return "SVP";
  if (/\bVICE PRESIDENT\b|\bVP\b/.test(s)) return "VP";
  if (/\bPRESIDENT\b/.test(s)) return "PRES";
  if (/\bDIRECTOR\b/.test(s)) return "DIR";
  if (/\bOFFICER\b/.test(s)) return "OFFICER";
  return "INSIDER";
}

export function resolveInsiderRoleBadge(...candidates: Array<string | null | undefined>): InsiderRoleCode {
  return normalizeInsiderRoleBadge(firstNonEmptyInsiderRole(...candidates));
}

export function insiderRoleBadgeTone(roleCode: InsiderRoleCode): BadgeTone {
  switch (roleCode) {
    case "CEO":
      return "insider_ceo";
    case "CFO":
      return "insider_cfo";
    case "COO":
      return "insider_coo";
    case "CTO":
      return "insider_cto";
    case "CCO":
      return "insider_cco";
    case "CLO":
      return "insider_clo";
    case "CAO":
      return "insider_cao";
    case "EVP":
      return "insider_evp";
    case "SVP":
      return "insider_svp";
    case "PRES":
      return "insider_pres";
    case "VP":
      return "insider_vp";
    case "DIR":
      return "insider_dir";
    case "OFFICER":
      return "insider_officer";
    default:
      return "insider_default";
  }
}

export function resolveInsiderDisplayName(name?: string | null, position?: string | null): string | null {
  const normalizedName = normalizeNameLikeText(name);
  if (!normalizedName) return null;
  const normalizedPosition = normalizeNameLikeText(position);

  if (isRoleLikeLabel(normalizedName)) return null;
  if (!normalizedPosition) return normalizedName;

  if (canonicalizeComparisonText(normalizedName) === canonicalizeComparisonText(normalizedPosition)) {
    return null;
  }

  return normalizedName;
}

if (process.env.NODE_ENV !== "production") {
  const roleCases: Array<[string, InsiderRoleCode]> = [
    ["Chief Executive Officer", "CEO"],
    ["CEO", "CEO"],
    ["Chief Financial Officer", "CFO"],
    ["CFO", "CFO"],
    ["Chief Operating Officer", "COO"],
    ["Chief Technology Officer", "CTO"],
    ["Chief Compliance Officer", "CCO"],
    ["Chief Legal Officer", "CLO"],
    ["Chief Accounting Officer", "CAO"],
    ["Executive Vice President", "EVP"],
    ["SVP, Operations", "SVP"],
    ["Senior Vice President", "SVP"],
    ["Vice President", "VP"],
    ["Vice-President", "VP"],
    ["Vice President, Finance", "VP"],
    ["Executive Vice President and CFO", "CFO"],
    ["President and CEO", "CEO"],
    ["President", "PRES"],
    ["Director", "DIR"],
    ["Officer", "OFFICER"],
    ["Random Role", "INSIDER"],
  ];

  for (const [title, expected] of roleCases) {
    console.assert(
      normalizeInsiderRoleBadge(title) === expected,
      `normalizeInsiderRoleBadge('${title}') expected '${expected}'`,
    );
  }

  console.assert(resolveInsiderDisplayName("vice president, finance", "Vice President, Finance") === null);
  console.assert(resolveInsiderDisplayName("  PRESIDENT & CEO ", "President and CEO") === null);
  console.assert(resolveInsiderDisplayName("Jane Doe", "SVP, Operations") === "Jane Doe");
}
