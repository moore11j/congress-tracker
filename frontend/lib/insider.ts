export function insiderHref(reportingCik?: string | null): string | null {
  if (!reportingCik) return null;
  const cleaned = reportingCik.trim();
  if (!cleaned) return null;
  return `/insider/${encodeURIComponent(cleaned)}`;
}
