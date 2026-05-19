const aliasMap = new Map<string, string>([
  ["dod", "Department of Defense"],
  ["defense department", "Department of Defense"],
  ["department of defense", "Department of Defense"],
  ["hhs", "Department of Health and Human Services"],
  ["health and human services", "Department of Health and Human Services"],
  ["department of health and human services", "Department of Health and Human Services"],
  ["nasa", "NASA"],
  ["national aeronautics and space administration", "NASA"],
  ["energy department", "Department of Energy"],
  ["department of energy", "Department of Energy"],
]);

export function canonicalDepartmentName(value?: string | null): string | null {
  const cleaned = normalizeDepartmentKey(value);
  if (!cleaned) return null;
  return aliasMap.get(cleaned) ?? value?.trim() ?? null;
}

export function departmentSlug(value?: string | null): string | null {
  const canonical = canonicalDepartmentName(value);
  if (!canonical) return null;
  if (canonical === "NASA") return "nasa";
  const slug = canonical.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  return slug || null;
}

export function departmentHref(value?: string | null): string | null {
  const slug = departmentSlug(value);
  return slug ? `/departments/${encodeURIComponent(slug)}` : null;
}

function normalizeDepartmentKey(value?: string | null): string {
  return (value ?? "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/\bdept\.?\b/g, "department")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
