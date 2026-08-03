import type { SearchSuggestResult } from "@/lib/api";
import { memberHref } from "@/lib/memberSlug";

const COMPANY_SUFFIXES = new Set([
  "inc",
  "incorporated",
  "corp",
  "corporation",
  "co",
  "company",
  "ltd",
  "limited",
  "llc",
  "llp",
  "lp",
  "plc",
  "holdings",
  "holding",
  "class",
]);
const HIGH_CONFIDENCE_INSTITUTION_PREFIXES = new Set(["vanguard"]);

export function searchResultsHref(query: string) {
  return `/search?q=${encodeURIComponent(query.trim())}`;
}

export function routeForSearchResult(result: SearchSuggestResult) {
  if (result.kind === "member") return memberHref({ name: result.label, memberId: result.id });
  return result.href;
}

function words(value: string | null | undefined): string[] {
  return (value ?? "").toLowerCase().match(/[a-z0-9]+/g) ?? [];
}

function normalizedKey(value: string | null | undefined) {
  return words(value).join(" ");
}

function compactKey(value: string | null | undefined) {
  return words(value).join("");
}

function companyBaseKey(value: string | null | undefined) {
  const parts = words(value);
  while (parts.length > 1 && COMPANY_SUFFIXES.has(parts[parts.length - 1])) {
    parts.pop();
  }
  return parts.join(" ");
}

export function isHighConfidenceSearchResult(result: SearchSuggestResult | undefined, query: string) {
  if (!result) return false;
  const queryKey = normalizedKey(query);
  if (!queryKey) return false;

  if (result.kind === "ticker") {
    const symbol = (result.symbol || result.id || "").trim().toUpperCase();
    const rawQuery = query.trim().toUpperCase();
    if (symbol && symbol === rawQuery) return true;
    return companyBaseKey(result.label) === queryKey || normalizedKey(result.label) === queryKey;
  }

  if (result.kind === "institution") {
    const idKey = normalizedKey(result.id);
    if (idKey && idKey === queryKey) return true;
    const queryCompact = compactKey(query);
    const labelCompact = compactKey(result.label);
    const baseCompact = compactKey(companyBaseKey(result.label));
    return Boolean(
      queryCompact &&
      (
        labelCompact === queryCompact ||
        baseCompact === queryCompact ||
        (
          queryCompact.length >= 4 &&
          (words(query).length >= 2 || HIGH_CONFIDENCE_INSTITUTION_PREFIXES.has(queryCompact)) &&
          (labelCompact.startsWith(queryCompact) || baseCompact.startsWith(queryCompact))
        )
      )
    );
  }

  return normalizedKey(result.label) === queryKey;
}
