# Public insider profile SEO audit

Audit date: September 10, 2026. Scope: public insider profiles only. Changes are local and have not been deployed.

## 1. Cause of the Search Console report

The exact historical blocking rule was `Disallow: /insider/`. It appeared both in `frontend/public/robots.txt` and in `robotsDisallowPaths` in `frontend/middleware.ts`. Commit `6de2e616` (July 3, 2026, “Add inactive-request bypasses for API endpoints”) introduced the middleware restriction; commit `f0fc65c8` (July 30, 2026, “Improve SEO metadata and loading states”) removed the profile restrictions. The historical middleware also assigned profile routes a noindex header, a separate mechanism from robots blocking.

Current source and a direct HTTP fetch of https://app.walnutmarkets.com/robots.txt both allow insider crawling, including query variants. The live response was HTTP 200 with a five-minute cache lifetime. There is no current insider disallow or query wildcard block. All six supplied live URLs returned HTTP 200; clean URLs had `index, follow`, and the query URL had `noindex, follow` in both HTML and the response header.

This proves the historical rule could produce the reported exclusion; it does **not** prove when Google fetched it. Search Console's last robots fetch, last page crawl and exact inspected property are unavailable in this workspace. A stale/historical report is consistent with the evidence, but cannot be confirmed without those records. Preview or nonproduction hosts deliberately remain blocked.

The additional current defect was metadata based on a separate SEO snapshot or a 365-day summary, while the page rendered a different, normally 90-day, response. A stale snapshot could declare a failed or placeholder page indexable. Snapshot names also disagreed with visible normalized identities: live Fields and Bolduc pages canonicalized to `fields-curtland-e-0001310881` and `bolduc-john-0001050047` respectively.

## 2. Files changed

- `frontend/app/insider/[slug]/page.tsx`: shared public data for anonymous rendering and metadata; honest unavailable state; missing handling; resolved canonical redirects preserving queries.
- `frontend/lib/insiderSeo.ts`: public loader, metadata/canonical policy and existing sitemap candidate filtering.
- `frontend/lib/seoQuality.ts`: substantive insider data gate.
- `frontend/lib/insider.ts`: malformed URI decoding safely returns an invalid identifier.
- `frontend/app/sitemap-insiders.xml/route.ts`: apply insider candidate filtering.
- `frontend/tests/insider-seo.test.mjs`: behavioral metadata, rendering, privacy, missing, canonical, robots and sitemap checks.
- `frontend/tests/insider-chart-toggle.test.mjs` and `frontend/tests/seo-quality-gates.test.mjs`: update existing assertions for the shared loader and preserved query handling.
- This audit report.

## 3. Final indexability policy

A clean profile is indexable only when the public request resolves the matching nonzero CIK, a non-placeholder identity and substantive dated disclosure information: a dated aggregate with trades and associated tickers, a dated filing-backed company/role relationship, or a dated transaction with a valid ticker and transaction type. Identity alone or an arbitrary database row is insufficient.

Thin profiles, loading/error/locked responses, mismatched identities and unavailable public requests are `noindex, follow`. Unavailable profiles render explicit unavailable copy without inventing a name from the URL or substituting a fabricated zero-valued summary. Malformed routes and explicit API 404s invoke Next.js `notFound()`; an API outage is not reclassified as nonexistent. The backend currently answers an unknown CIK with HTTP 200 and empty identity, so that case remains an honest noindex unavailable page.

Metadata no longer trusts the independently materialized SEO snapshot. Anonymous metadata and rendering share a React-cached loader and identical public request parameters. Existing authenticated cache bypass behavior is preserved, but API calls carry no session token, cookies or entitlement context. Locked summary payloads are discarded before rendering. No gated endpoint was added.

## 4. Exact robots.txt changes

**None.** The erroneous historical rules were already removed before this task. Current production output is preserved:

```text
User-agent: *
Allow: /
Disallow: /api/
Disallow: /account
Disallow: /billing
Disallow: /settings
Disallow: /admin

Sitemap: https://app.walnutmarkets.com/sitemap-index.xml
```

Private-route restrictions and existing noindex headers are unchanged. Preview/dev robots remain `User-agent: *` and `Disallow: /`; nonproduction page responses retain `noindex, nofollow`. Behavioral tests guard this host/environment distinction.

## 5. Canonicals

Metadata uses one query-free canonical on `https://app.walnutmarkets.com`, built from the same resolved display name used by the profile and the original matching CIK. Indexable clean destinations are self-canonical. Resolved aliases redirect to that destination while preserving all query values. No extra trailing slash or identifier substitution is introduced. An unresolved page keeps its own clean incoming path and is noindex.

## 6. Query-state handling

Supported URL state remains `lookback` (30/90/180/365/1095, default 90), `issuer`, `symbol` and `recent_trades_page`. All nonempty UI state, including legacy `chart=performance`, repeated values and unknown filter parameters, receives `noindex, follow` and the clean canonical. The shared middleware also sends the existing query noindex header; robots allows the crawl.

Existing analytics-only parameter exceptions (`utm_*`, `_ga*`, `_gl`, `gclid`, `fbclid`, `msclkid`) remain unchanged; they do not select profile content and canonicalize cleanly. Empty parameters likewise do not select a separate content state.

The current application already uses a company stock chart and client-local performance horizon controls. Legacy `chart` was not an active chart switch before this change. It is preserved in redirects and remains noindex; no working feature was removed. Redirects no longer inject `chart=stock` into otherwise clean URLs.

## 7. Metadata

Resolved names generate `{Name} Insider Trades & SEC Form 4 Activity | Walnut`. Substantive descriptions refer to disclosed transactions, Form 4 activity and reported company relationships. They make no performance, holdings, wealth or returns claims. Thin and unresolved responses receive appropriately limited copy. Metadata no longer invents identity from the slug or uses a snapshot's conflicting name/canonical.

## 8. Sitemap

The existing `sitemap-insiders.xml` and materialized SEO snapshot index are retained. Candidates must already be marked indexable and additionally have a matching public CIK, resolved identity, a valid dated disclosure with ticker/type, and a valid modification date. Locked, unavailable, malformed, thin and duplicate candidates are removed. Paths are reconstructed from identity and CIK, stripping any untrusted query or conflicting snapshot canonical. No per-profile request fan-out or new large sitemap system was added.

The local HTTP check returned a valid sitemap with 968 unique clean URLs and zero query variants. Eligibility is based on the existing materialized public disclosure data; it is not a promise that every profile request will succeed during a later API outage. Existing refresh and cache lifetimes remain in place.

## 9. Six requested URLs: raw server HTML

Checked using a local production build, `Host: app.walnutmarkets.com` and a Googlebot user agent, with the live public API. No browser hydration was used. Every row below returned HTTP 200 without a redirect, exactly one canonical, and a resolved profile heading. Canonicals use `https://app.walnutmarkets.com` followed by the path shown.

| Requested path | Robots | Canonical path | Resolved identity and substantive raw HTML | Index under policy? |
| --- | --- | --- | --- | --- |
| `/insider/curtland-e-fields-0001310881` | index, follow | `/insider/curtland-e-fields-0001310881` | Curtland E Fields; Kearny Financial Corp., KRNY, director; 8 filings and latest filing date | Yes |
| `/insider/bodenstedt-matthias-0001920545` | index, follow | `/insider/bodenstedt-matthias-0001920545` | Bodenstedt Matthias; MoonLake Immunotherapeutics, MLTX, CFO; 2 filings and latest filing date | Yes |
| `/insider/king-mark-james-0001783820` | index, follow | `/insider/king-mark-james-0001783820` | King Mark James; Jack in the Box, JACK, director/executive chairman/interim CEO; 3 filings and latest filing date | Yes |
| `/insider/john-bolduc-0001050047` | index, follow | `/insider/john-bolduc-0001050047` | John Bolduc; WhiteHorse Finance, WHF, director; 1 filing and latest filing date | Yes |
| `/insider/whalen-amanda-0001991131` | index, follow | `/insider/whalen-amanda-0001991131` | Whalen Amanda; Klaviyo, KVYO, CFO; 5 filings and latest filing date | Yes |
| `/insider/john-bolduc-0001050047?lookback=180&chart=performance` | noindex, follow (HTML and header) | `/insider/john-bolduc-0001050047` | John Bolduc and WHF context; 180-day window with 11 filings and latest filing date | No: query variant |

Meaningful content in these responses consists of identity, CIK, filing-backed company/role context, dated activity and aggregate filing counts. Their recent transaction tables were empty in the raw response; neither a populated transaction table nor a performance result is claimed as evidence. Existing empty-table behavior and calculations were not changed.

## 10. Related profile families

The same historical middleware and static robots rules also blocked `/member/` and `/institution/`. Those restrictions were already removed in the July 30 change. `/department/` was not in that historical block list. Current shared production robots allows all three families. Their existing metadata quality gates were inspected; no shared current robots defect required modifying those routes. A broader review of their independently materialized metadata would be separate work.

## 11. Validation

- Focused tests: 41 passed, zero failed.
- Full frontend suite: 558 tests, 512 passed, 46 failed. Baseline before this task: 550 tests, 504 passed, the same 46 failed. Failure names match exactly; no new failures.
- TypeScript (`npx tsc --noEmit`): passed. Frontend production build (`NEXT_DIST_DIR=.next-insider-seo npm run build`): passed, including Next's configured lint/type checks. There is no separate lint script. The build's existing Browserslist freshness warning was not addressed by changing dependencies.
- Tests exercise actual metadata/route functions with public API fixtures, server-render both anonymous and authenticated states, and check that private/session sentinel data is absent from HTML and loader options. They also verify locked-payload rejection, API 404 propagation, unavailable rendering, canonical aliases and query preservation, sitemap filtering and production/preview middleware behavior.
- Live production and local raw HTML were both inspected. Live production was not changed.
- All six raw HTML results were reconfirmed against the final build. The unknown CIK `0000000000` returned HTTP 200 with `noindex, follow` and explicit unavailable copy.
- Missing-route limitation: `/insider/not-a-cik` invokes Next.js `notFound()` and renders the not-found response with noindex, but the project's streaming shell commits HTTP 200. Both unchanged live production and the final local build behave this way for Googlebot and Google-InspectionTool. Metadata now also propagates missing status, but this does not change the already-streamed HTTP status. Tests prove explicit API 404s reach the framework's missing handler; they do not claim a hard HTTP 404 for streamed responses. Changing the application-wide streaming/error response architecture was deferred.

## 12. Intentionally deferred

Deployment and Search Console revalidation were not performed. Confirm Google's last robots fetch/crawl and the property host in Search Console before attributing a currently displayed block to today's production rules. After deployment, the owner can run live URL inspection and request revalidation.

No homepage/ticker changes, profile redesign, insider calculation or performance-methodology changes, entitlement changes, new sitemap backend, or additional profile-family SEO phase were undertaken. Existing empty transaction-table display, date formatting and the streaming missing-page HTTP-status limitation described above are outside this targeted change.
