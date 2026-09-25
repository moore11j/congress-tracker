# Ahrefs SEO repairs — September 25, 2026

## Baseline

Reviewed the authenticated September 15 crawl for Ahrefs project 10225330. Health Score: **56 (Fair)**. The crawl stopped after exhausting monthly crawl credits; its findings are not a complete current-site crawl.

The ten Error groups were: 404 (2), 4XX (2, overlapping the 404s), canonical points to redirect (2), indexable orphan pages (3,238), canonical URLs without internal links (16), pages linking to broken pages (15), broken redirects (2), duplicate pages without canonical (2), large images (2), and non-canonical sitemap URLs (15).

## Changes

- Added server-rendered, alphabetized public directories for stocks, Congress members, insiders, institutions, and agencies. Every page links directly to all pagination pages in its category. Shared footer links and the marketing footer make the directory reachable from existing pages. Profiles retain their original content and eligibility rules.
- Added a directory sitemap to the sitemap index. Directory data uses the same public sources and eligibility rules as entity sitemaps, with canonical corrections and deduplication. Upstream failures return errors rather than successful empty cached directories.
- Replaced 15 stale insider sitemap aliases with their verified canonical destinations, and used those same corrections in directory links.
- Corrected `/outcomes` and `/market-pressure` canonicals to their app-host URLs.
- Fixed the anonymous screener rewrite bypass that omitted `noindex`; both screener entry routes now expose consistent canonical metadata. Query-based screens remain intentionally excluded from indexing.
- Redirected retired ticker `/earnings` and `/financials` routes directly to the working ticker financials view, including marketing and legacy hosts.
- Stopped constructing Congress.gov links from provider-specific `FMP_` IDs. Valid Bioguide IDs retain official links.
- Corrected two obsolete Apple investor-relations URLs and the mistyped Motley Fool SpaceX article URL in rendered research links.
- Served local research hero images through responsive Next.js image optimization.
- Added the missing Insights heading and enabled initial HTML metadata delivery to Ahrefs audit bots.
- Corrected subscription-service structured data. No fabricated reviews or ratings were added to qualify for software rich results.
- Research sitemap modification dates now reflect actual publication or documented material changes rather than using an unrelated static date.

## Validation and limits

The scoped production build was assembled from committed HEAD plus only the files in the repair manifest, excluding unrelated work in progress. Focused regression suite: **61 passing tests**. Additional HTTP checks compare all entity sitemap URLs against rendered directory links, and verify flagged canonicals, screener directives, redirects, source links, and optimized image responses.

Artifacts and detailed verification output: `artifacts/ahrefs-repair-2026-09-25/` (local, intentionally not committed).

Intentional noindex pages, ordinary redirects, URL parameters, and change notices are not automatically defects. External publishers returning 403 to audit bots were not stripped of valid citations. Slow-page and title-length warnings require current measurements/editorial judgment rather than hiding pages to improve a score.

A new Ahrefs crawl is required to measure the resulting Health Score. These repairs do not establish that Google has indexed the pages or guarantee a particular score or traffic increase.
