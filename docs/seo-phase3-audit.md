# Phase 3 SEO Foundation Audit

Date checked: 2026-08-01

## Page-Type Audit

| Page type | Current route | Indexation stance | Main risk | Phase 3 control |
| --- | --- | --- | --- | --- |
| Ticker pages | `/ticker/[symbol]` | App-domain public pages, pilot sitemap only | Empty or unresolved ticker shells | `tickerHasIndexableContent` requires identity and multiple research modules |
| Congress member pages | `/member/[slug]` | App-domain public pages, pilot sitemap only | Duplicate member ids or low-activity profiles | Missing profile metadata is `noindex, follow`; sitemap stays approved-pilot only |
| Insider pages | `/insider/[slug]` | App-domain public pages, pilot sitemap only | Ambiguous identity or no issuer/activity context | `insiderHasIndexableContent` requires CIK plus identity and activity/issuer context |
| Institution pages | `/institution/[cik]` | App-domain public pages, pilot sitemap only | Locked or delayed filings mistaken for live data | `institutionHasIndexableContent` excludes locked/unavailable shells |
| Department contract pages | `/departments/[slug]` | App-domain public pages, pilot sitemap only | Empty department shells | `departmentHasIndexableContent` requires contract or linked ticker context |
| Research briefs | `/research/[slug]` | Marketing-domain indexable only when published | Draft or missing generated briefs | Existing unavailable metadata remains `noindex, follow` |
| Stock comparisons | `/compare/[left]/[right]` | App-domain approved pilot only | Bulk indexing arbitrary ticker pairs | Non-pilot pairs are `noindex, follow` |
| Marketing comparison pages | `/compare/walnut-markets-vs-*` | Marketing-domain static pages | Competitor claim drift | Phase 1 centralizes source URLs and checked dates |
| Commercial feature pages | Phase 2 static routes | Marketing-domain static pages | Cannibalization against older tools pages | Distinct intent and internal-link map from Phase 2 |

## Indexation Quality Gates

The implementation lives in `frontend/lib/seoQuality.ts`.

Ticker pages need a valid symbol, non-loading identity, and at least two useful research modules such as price history, technical indicators, confirmation context, source activity, trades, or research freshness.

Member pages need a valid canonical identity. Empty or unresolved profile metadata should not be indexed.

Insider pages need a reporting CIK, insider identity, and either activity, role context, or issuer context.

Institution pages need a normalized CIK, holder name, and reported holdings, top holdings, or filing date context. Locked Pro shells are not indexable public research pages.

Department pages need a valid department identity plus contract count or linked public-company context.

Research briefs must be published and visible. Draft or missing generated briefs stay `noindex, follow`.

Ticker comparison pages must be in the approved pilot registry before indexing.

## Sitemap Architecture

The app-domain sitemap is already segmented:

- `/sitemap-tickers.xml`
- `/sitemap-members.xml`
- `/sitemap-insiders.xml`
- `/sitemap-institutions.xml`
- `/sitemap-departments.xml`
- `/sitemap-research.xml`
- `/sitemap-comparisons.xml`

Phase 3 keeps these segmented files and moves approved pilot URLs into a centralized registry. Each URL now carries `lastmod`. The system intentionally avoids database-wide sitemap expansion.

## Programmatic Evergreen Foundation

The implementation lives in `frontend/lib/evergreenSeo.ts`.

Supported future page types are:

- Ticker vs ticker research
- Stock-analysis comparisons
- Sector peer comparisons
- Congress/member activity summaries
- Insider activity summaries

Indexing requires published editorial status, evidence, quality requirements, and an approved pilot canonical path. Drafts and unapproved pages should remain `noindex, follow`.

## Structured Data

Marketing pages use Organization, WebPage, BreadcrumbList, SoftwareApplication, and FAQ where valid. Research briefs use article Open Graph metadata. Phase 3 does not add ratings, reviews, prices, authors, or dates that are not visible or supported.

## Remaining SEO Risks

- Full entity metadata quality still depends on backend availability during metadata generation.
- The app-domain terminal chrome can affect local mobile screenshots even when marketing-host HTML checks pass.
- Only a small pilot set is included in app-domain sitemaps. Scaling requires a backend-driven eligibility job, not direct database-wide generation at request time.
- Some older app-contract tests remain red outside this SEO work and should be fixed separately before treating the whole frontend suite as green.
