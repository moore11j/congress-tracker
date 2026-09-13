// Confirmed public crawl findings. Re-audit a held URL before removing its
// exclusion; an unavailable/thin page must not be advertised as indexable.
export const sitemapCorrections: Readonly<Record<string, { canonical?: string; exclude?: boolean; reason: string }>> = {};
