# FAQ canonical and redirect audit

Checked September 11, 2026. Preferred URL: https://walnutmarkets.com/faq. Changes are local; production has not been deployed from this task.

## Live production findings

| Check | Observed result |
| --- | --- |
| `https://www.walnutmarkets.com/faq` | HTTP 308 to `https://app.walnutmarkets.com/faq` |
| Redirect hops | One hop, but to the wrong preferred host |
| `https://walnutmarkets.com/faq` | Also HTTP 308 to the app host; does not currently return 200 directly |
| Final app-host response | HTTP 200 |
| Raw HTML canonical | Exactly one: `https://app.walnutmarkets.com/faq` |
| Open Graph URL | `https://app.walnutmarkets.com/faq` |
| FAQ sitemap entry | `https://app.walnutmarkets.com/faq` |
| Robots | Final page `index, follow`; production robots.txt does not block `/faq` |
| Internal www links | None found in inspected live pages or frontend link source |
| Other FAQ links | Several pointed to the app host; marketing research links used relative `/faq` |

The current implementation does not match the requested preferred URL, so this cannot be dismissed as requiring only Search Console “Validate Fix.” The exact reason for Google's historical classification still requires its crawl records.

The responsible routing rule was `/faq` in middleware's `appHostedPaths`, which ran before general www-to-non-www normalization. FAQ metadata used `appPageMetadata`, and the sitemap and internal links consistently selected the app host.

## Targeted fix

FAQ now has an explicit host rule: www, HTTP, app and legacy FAQ aliases redirect directly with HTTP 308 to `https://walnutmarkets.com/faq`, preserving query values. The preferred HTTPS host serves the existing FAQ route and page shell directly. FAQ canonical and `og:url` use the existing marketing metadata helper. The sitemap contains only the preferred FAQ URL, and app-host FAQ links now go directly there. The relative `/faq` link on marketing research pages already resolves to the preferred host.

FAQ questions, answers, structured data, title, description and layout are unchanged. A source comparison against HEAD confirmed the only FAQ page edits are the metadata helper import and call. The navigation's active-link check accepts the absolute FAQ URL to preserve existing menu highlighting.

## Marketing hostname consistency

Live `/` and `/stock-research-app` requests on www each returned one permanent 301 to non-www, followed by 200. Tests also cover `/stock-analysis-tools` and `/compare`. The same shared normalization rule applies to marketing-owned routes.

About, Pricing, Terms, Privacy and Contact intentionally remain app-owned routes and retain their direct permanent redirects to `app.walnutmarkets.com`. Live About and Pricing checks confirmed this behavior. They were not migrated as part of this FAQ-only request. No marketing canonical or internal link introduces a www destination; remaining www literals are redirect aliases, an account-origin allowlist and test fixtures.

## Files changed for this task

- `frontend/middleware.ts`
- `frontend/app/faq/page.tsx`
- `frontend/public/sitemap.xml`
- `frontend/public/llms.txt`
- `frontend/components/AppTopNav.tsx`
- `frontend/components/auth/AccountNav.tsx`
- `frontend/components/insider/InsiderAnalyticsClient.tsx`
- `frontend/components/landing/MarketingHeader.tsx`
- `frontend/components/landing/ComparisonPages.tsx`
- `frontend/app/landing/page.tsx`
- `frontend/app/reddit/stock-research/page.tsx`
- `frontend/tests/seo-canonicalization.test.mjs`
- `frontend/tests/landing-polish.test.mjs`
- `frontend/tests/profiles-navigation.test.mjs`
- `frontend/tests/premium-ux-polish.test.mjs`
- This report.

Link-bearing components have only FAQ destination edits, plus the navigation active-link adjustment described above. Existing assertions were updated only where they encoded the old FAQ host. Prior insider SEO changes already present in the workspace were preserved.

## Validation

SEO/robots tests: 16 passed, zero failed. The broader focused group had 68 passes and 11 known baseline failures. Full suite: 561 tests, 515 passed, 46 failed; baseline: 558 tests, 512 passed, the identical 46 failures. Three behavioral regression tests were added for redirects, host consistency and metadata/link agreement.

Production build passed, including Next's configured lint and TypeScript validation. The existing Browserslist freshness warning was left unchanged.

Raw local production-build checks used the requested production host headers and inspected complete server HTML:

| Check | Verified local result |
| --- | --- |
| www FAQ | HTTP 308, Location `https://walnutmarkets.com/faq` |
| Preferred FAQ | HTTP 200, no Location header; therefore one redirect hop total |
| App FAQ alias | HTTP 308 directly to the preferred URL |
| Canonical | Exactly one `https://walnutmarkets.com/faq` |
| Open Graph URL | Exactly one `https://walnutmarkets.com/faq` |
| Robots | `index, follow`; no final X-Robots-Tag; robots.txt allows FAQ |
| Sitemap | Exactly one FAQ entry: `https://walnutmarkets.com/faq` |
| Content/layout | All 47 question entries and existing page header present |
| www marketing research URL | One HTTP 301 to the non-www marketing destination |

Deploy this change before using Search Console live inspection and Validate Fix; production still serves the pre-change behavior described above.
