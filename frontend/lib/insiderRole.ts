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

export function normalizeInsiderRoleBadge(raw?: string | null): InsiderRoleCode {
  const s = (raw ?? "INSIDER").toUpperCase();
  if (/\bCHIEF EXECUTIVE OFFICER\b|\bCEO\b/.test(s)) return "CEO";
  if (/\bCHIEF FINANCIAL OFFICER\b|\bCFO\b/.test(s)) return "CFO";
  if (/\bCHIEF OPERATING OFFICER\b|\bCOO\b/.test(s)) return "COO";
  if (/\bCHIEF TECHNOLOGY OFFICER\b|\bCTO\b/.test(s)) return "CTO";
  if (/\bCHIEF COMPLIANCE OFFICER\b|\bCCO\b/.test(s)) return "CCO";
  if (/\bCHIEF LEGAL OFFICER\b|\bCLO\b/.test(s)) return "CLO";
  if (/\bCHIEF ACCOUNTING OFFICER\b|\bCAO\b/.test(s)) return "CAO";
  if (/\bEXECUTIVE VICE PRESIDENT\b|\bEXEC\s+VP\b|\bEVP\b/.test(s)) return "EVP";
  if (/\bSENIOR VICE PRESIDENT\b|\bSR\s+VP\b|\bSVP\b/.test(s)) return "SVP";
  if (/\bPRESIDENT\b/.test(s)) return "PRES";
  if (/\bVICE PRESIDENT\b|\bVP\b/.test(s)) return "VP";
  if (/\bDIRECTOR\b/.test(s)) return "DIR";
  if (/\bOFFICER\b/.test(s)) return "OFFICER";
  return "INSIDER";
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
  const normalizedName = name?.trim() ?? "";
  if (!normalizedName) return null;
  const normalizedPosition = position?.trim() ?? "";
  if (!normalizedPosition) return normalizedName;
  if (normalizedName.toUpperCase() === normalizedPosition.toUpperCase()) return null;
  return normalizedName;
}
