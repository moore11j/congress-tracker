# Walnut sitemap and indexing audit — September 13, 2026

## Scope

Audit every unique page URL advertised by robots.txt and the marketing/app sitemap indexes. Check HTTP 200, no redirect, exactly one self-referencing canonical, robots meta directives, X-Robots-Tag, and robots.txt permission. The initial inventory contains 3,185 unique URLs across 10 XML files, including the app sitemap index. Shared research/comparison URLs are deduplicated when auditing.

Verification spans the session rather than one instantaneous snapshot. The final ledger retains 1,793 successful initial checks for pages whose successful response path was unchanged. Previously failing URLs, corrected canonical destinations, all ticker pages, and remaining unvisited pages are checked after the relevant deployments. Each retained row is labeled `verification_phase`; fresh checks have a UTC `checked_at` timestamp. No failed initial result is treated as a pass without a successful retry.

## Root causes and corrections

- **Department URLs use hyphens.** The National Science Foundation and National Transportation Safety Board hyphen URLs were valid; replacing sitemap paths with underscores would advertise aliases. Department aliases now receive an HTTP 308 before page rendering. The sitemap and internal links share the same department URL helper. Congress member slugs use a separate underscore convention; there is no global separator replacement.
- **Temporary failures became durable indexing instructions.** Department, member, institution, insider, ticker, and generated research metadata could convert API errors or short timeouts into `noindex`. Metadata now shares public page data where appropriate and propagates transient failures instead of declaring a valid entity unindexable. Genuine missing/thin profiles retain their existing restrictions.
- **A real agency lookup defect:** U.S. International Development Finance Corporation was in the department index but appeared empty because database punctuation and normalized aliases did not match. The lookup now accepts both forms; its live profile resolves two contracts and two linked tickers. Old empty profile fetches use a new cache version.
- **Insider canonical names differed from snapshot names.** Verified sitemap replacements are recorded in `frontend/lib/sitemapCorrections.ts`. Sitemap output uses those canonical destinations and deduplicates them. This is a versioned correction list, not a blanket rule to index empty profiles.
- **Cold ticker renders could overload the API.** Public page requests could trigger full context rebuilds involving hundreds of queries; observed builds exceeded 90 seconds. Anonymous server rendering now requests cached context only. A cache miss uses the existing dated public research snapshot. The interactive/authenticated build path remains available, and public cache keys are partitioned from live requests.
- **Sitemap failures could replace the inventory.** Entity sitemap routes now return 503 with Retry-After and no-store when their source fails, instead of a misleading empty or pilot sitemap.
- **Metadata delivery:** Search crawler user agents receive blocking metadata so canonical/robots tags are included in the initial HTML.

## Operational observations

The initial crawl was stopped after 2,986 pages when API readiness checks began timing out. It observed 496 noindex responses, 34 canonical findings (including incomplete responses), and one fetch timeout. These are observed responses, not 496 proven permanently invalid pages. The servers recovered after the crawl stopped. Database checks identified I/O pressure; no instance size or subscription was changed. The final audit uses bounded concurrency, delays, and a circuit breaker for consecutive incomplete/error responses.

At the closing health check, both API readiness checks passed. Fly's database VM check still displayed its I/O warning, while the database/role checks passed. A direct `/proc/pressure/io` read showed 0.00% full I/O stall over the latest 10 seconds and 1.20% over 60 seconds, versus 16.28% over 300 seconds. Intermittent database I/O pressure remains a performance concern even though the URL verification completed successfully; this audit does not claim that underlying performance issue is eliminated.

## Validation

- Backend department regression tests: 5 passed.
- Backend public-render/cache regression tests: 13 passed.
- Audit-tool regression tests: 3 passed, including duplicate canonical and X-Robots-Tag detection.
- Focused frontend regression coverage: 57 distinct tests passed (54 indexing/canonical/render tests and 3 daily-research SEO tests).
- TypeScript checks and Vercel production builds are verified during deployment.
- Live checks confirmed both reported departments, the repaired development-finance department, and the sampled previously failing tickers return HTTP 200 with index/follow and one matching canonical.
- Underscore department alias verified as HTTP 308 to the hyphenated path.
- Final inventory reconciliation: **3,185 / 3,185 URLs passed**, with no unresolved noindex, robots blocks, redirects, non-200 responses, or canonical mismatches. 1,392 URLs were verified after their relevant deployments; 1,793 unchanged successful checks were retained as described above.
- Latest check: 2026-09-13T20:56:43.123238+00:00

| Page family | Verified URLs |
|---|---:|
| Comparisons | 8 |
| Congress members | 237 |
| Departments | 38 |
| Insiders | 968 |
| Institutions | 868 |
| Other public pages | 18 |
| Research | 23 |
| Tickers | 1,025 |

Detailed results: `backend/artifacts/sitemap-audit-final/audit-results.csv`, `verified-pages.jsonl`, and `verified-summary.json`. The initial and resumed crawl logs remain in `backend/artifacts/sitemap-audit/`.

Canonical comparisons normalize the equivalent bare-origin and `/` homepage forms. Other path/query differences and duplicate canonical tags remain failures. All 3,185 URLs remain represented; 14 insider sitemap URLs were replaced with their verified canonical names.

Application deployments: frontend through `9c2851ae` verified successful on Vercel; backend through `f8c705d8` deployed to the existing Fly app. The remaining commit contains audit tooling, tests, and this report.

## Repeating the audit

From the repository root:

```powershell
python backend/scripts/audit_public_sitemaps.py --output backend/artifacts/sitemap-audit-new --workers 2 --delay 0.5
```

The command writes `inventory.json`, `pages.jsonl`, and `summary.json`. Exit 1 means findings; exit 2 means an interrupted audit or sitemap error. Use `--resume` to retain successful checks and retry unfinished/failed URLs. Audit output contains only public URLs and metadata.

## Search Console interpretation

The supplied screenshots show 95 indexed pages, about 2.48k not indexed, and 438 impressions/6 clicks in the selected three-month view. The largest excluded group is 2,433 “Discovered — currently not indexed,” which is different from an explicit noindex block. Correcting technical signals removes avoidable barriers; it does not guarantee indexing or rankings for every discovered URL. After production validation, retest the reported canonical pages, request indexing for those priority pages, and use Validate Fix for the relevant errors. Do not submit underscore department aliases as canonical sitemap URLs.

References: [Google URL structure](https://developers.google.com/search/docs/crawling-indexing/url-structure), [noindex](https://developers.google.com/search/docs/crawling-indexing/block-indexing), [canonical sitemaps](https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap), [Next.js crawler metadata](https://nextjs.org/docs/app/api-reference/config/next-config-js/htmlLimitedBots).
