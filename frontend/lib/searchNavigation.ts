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
  "plc",
  "holdings",
  "holding",
  "class",
]);

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

  return normalizedKey(result.label) === queryKey;
}
