# Walnut Markets homepage positioning and SEO

Scope: local implementation and verification, September 10, 2026. No deployment or new content phase.

## 1. Changes

Strengthened the existing homepage around historical participants and strategies, current stock rankings, the underlying datasets, and ongoing monitoring. Preserved the layout, responsive classes, dataset strip, product navigation, pricing, entitlements, calculations, and CTA structure. Research Memory remains labeled Coming Soon; the Outcomes recalculation notice remains intact.

The marketing homepage is `app/landing/page.tsx`, rewritten from `/` on walnutmarkets.com. The app-host root is a separate noindex feed. Important homepage copy already renders on the server; no client-only copy workaround was needed.

## 2. Files changed

- `frontend/lib/homepageContent.ts`: shared hero, search/social descriptions and score explanation.
- `frontend/app/landing/page.tsx`: narrative, contextual links, JSON-LD identities, image alt text and lazy loading.
- `frontend/lib/tickerSeo.ts`: public snapshot validation and ticker-specific metadata helpers.
- `frontend/app/ticker/[symbol]/page.tsx`: shared anonymous server loader, rendered-content indexability, dated public fallback and honest unavailability.
- `frontend/middleware.ts`: preview/local robots protection and marketing `/landing` redirect.
- `frontend/public/sitemap.xml`: remove a noindex leaderboard URL and update the homepage modification date.
- `frontend/tests/homepage-seo.test.mjs`: behavioral tests for metadata, snapshots, robots and canonical redirects.
- `frontend/tests/landing-polish.test.mjs`: update expected copy while retaining CTA/layout checks.
- `frontend/tests/ticker-hydration-contract.test.mjs`: check the revised fallback and constrain the existing source check to its intended function.
- This report.

## 3. Exact homepage metadata

Title (63 characters):

> Stock Analysis, Congress Trades & Insider Data | Walnut Markets

Description (163 characters):

> Research ranked stocks with fundamentals, technicals, Congress trades, insider data, institutional holdings, contracts and analysts. Compare backtested strategies.

Search snippets can truncate by rendered width, so character counts are guidance rather than a display guarantee. Existing Next.js server metadata utilities remain the source of canonical, Open Graph and Twitter/X tags. The existing branded 1200 × 630 `/og/walnut-og-v1.png` is reused.

## 4. Hero before and after

Before headline:

> Find top-ranked stocks. See what performed.

After headline:

> Find Top-Ranked Stocks. See Who Actually Outperformed.

Before supporting copy:

> Find stocks that rank highly now, see which market participants have performed, and explore strategies with historical results. Understand the rankings, then track what happened next.

After supporting copy:

> See which stocks rank highest now, how Congress members, insiders and institutions performed historically, and which backtested strategies beat their benchmarks. Then see the data behind every result.

“What's Working on Walnut.” becomes “Start With What Has Worked”. “See Who's Beating the Market.” remains, with historical/data-availability qualifications. The closing heading uses “Don't follow a signal. Follow the evidence.” Existing performance disclaimers remain.

## 5. Structured data

Retained Organization, WebSite and SoftwareApplication. Added stable `@id` values and linked publisher references; corrected WebSite from the app URL to the canonical marketing homepage. Reused the real organization logo, social profiles, application category and existing visible plan configuration. No ratings, testimonials, user counts, awards or performance values were added. Existing pricing offers and pricing logic were preserved.

## 6. Technical SEO

- Preserved the non-www HTTPS marketing canonical, app-host canonicals, legacy/www redirects and Next.js trailing-slash normalization.
- Added a permanent `/landing` → `/` redirect on the marketing domain, preserving query parameters.
- Added `noindex, nofollow` response headers for non-production hosts and Vercel preview/development deployments; their robots response disallows crawling and omits production sitemaps.
- Production marketing robots remain indexable. Existing private route restrictions remain intact.
- Removed `/leaderboards` from the static marketing sitemap because middleware marks it noindex, despite its route metadata declaring indexable. The leaderboard and all its navigation links remain unchanged.
- Preserved one homepage H1 and the existing H2/H3 section/card hierarchy.
- Shortened the product screenshot alt text to describe the image, retained dimensions and horizontal image scrolling, and added lazy loading/asynchronous decoding. The decorative brand SVG remains aria-hidden beside visible brand text.
- Added no fonts, animation libraries, tracking scripts, dependencies or client components. Core Web Vitals field measurements were not performed.

## 7. Internal links and topical pages

Added four inline links within the existing data explanation: Congress stock trades, insider buying/SEC Form 4, institutional holdings/13F, and government contracts. Existing score-methodology, screener, strategy, leaderboard and footer navigation remains available. Source comparison confirmed all eight existing HomepageCtaLink components, every existing native anchor, and the LandingSearch component are unchanged.

Existing topical landing pages use page-specific server metadata, canonical helpers, visible explanatory sections and contextual navigation. They already cover Congress, insiders, institutions, contracts and Confirmation Score. No additional landing pages or keyword expansion system was necessary.

## 8. Ticker findings and fixes

The old unconditional anonymous crawler-shell behavior is no longer the normal code path: anonymous ticker requests already use the real context bundle and a delayed public cache. However, a 2.5-second context timeout followed by a failed 1-second profile fallback could still render loading identity, unavailable modules and empty arrays. Metadata independently trusted an SEO snapshot, so an indexable declaration could accompany that shell.

The anonymous renderer and metadata now share their request-scoped context result through React cache. Existing API cache duration, entitlement handling and calculations are unchanged. Normal indexability requires the existing public snapshot approval plus meaningful resolved profile content. Query-state variants remain noindex.

If the live/cached context and profile cannot resolve, the page can render the existing public SEO snapshot, with its data date and historical-snapshot label. The validator rejects missing, undated, mismatched and non-indexable snapshots. Only public snapshot sections are shown; no authenticated bundle is serialized and no missing values become synthetic research data. If no usable snapshot exists, the page clearly says research is unavailable and metadata is noindex. A genuine 404 continues through the existing missing-ticker view.

Normal title example:

> AAPL Stock Analysis, Insider Trades & Congress Data | Walnut

Descriptions include company and ticker identity where available, with a ticker-only fallback for unusually long company names. Snapshot-only and unavailable pages use narrower, truthful titles and descriptions. All branches now reuse the shared app social metadata utility.

## 9. Validation

- TypeScript: passed.
- Focused homepage/SEO/canonical/robots/context tests: 47 passed.
- Full frontend suite: 550 tests; 504 passed and 46 failed. All 46 failing test names also fail on an isolated unchanged HEAD checkout (546 tests; 500 passed, 46 failed). No new failing tests.
- Production build: passed twice, including the final source changes; 62 static pages generated.
- Lint: no standalone lint script or ESLint configuration is installed. Next build runs its configured lint/type validation stage. Existing Browserslist data-age notice was not resolved through unrelated dependency updates.
- Browser: checked the homepage at 1440 × 1000 and 390 × 844. No horizontal overflow; one H1; existing hero CTA order/labels intact. Product screenshot loaded. No console or hydration errors were observed. Followed the Congress contextual link successfully and verified the hydrated AAPL identity and metadata.
- Raw HTML: confirmed the exact title/description, one canonical (Next renders the root as `https://walnutmarkets.com`), one H1, nonduplicated Open Graph/Twitter metadata, and valid Organization/WebSite/SoftwareApplication JSON-LD with linked IDs. The sitemap parses as XML with 27 unique URLs. Production robots advertise the correct sitemap; preview/local robots disallow crawling.
- Route smoke checks: Congress, insiders, institutional filings, contracts, score methodology, screener, leaderboards, strategies, pricing and login all returned 200. Local `.env.local` points app navigation at port 3000, so app destination paths were checked on the isolated preview at port 3107; the environment configuration was not changed. Production-host rewrite/redirect behavior was additionally exercised through middleware tests, since an HTTPS forwarded host cannot be proxied back to a plain-HTTP local preview.
- Tickers: AAPL and NVDA returned real server-rendered company content, ticker-specific metadata, a single canonical and `index, follow` on warm requests, without the loading-shell message. An unknown ticker returned honest unavailable content and `noindex, follow`. Cold requests conservatively remained noindex when the available response did not satisfy the quality gate. Snapshot validation has behavioral tests; a forced backend outage was not induced against production.

## 10. Intentionally deferred

- No deployment, Search Console submission, indexing expansion, new keyword pages or further SEO/content phase.
- No changes to leaderboard indexation policy, screener behavior, strategy calculations, scoring, Outcomes, plan entitlements or pricing. The contradictory leaderboard sitemap entry was removed without changing the product route.
- No broad rewrite of otherwise specific topical-page metadata.
- No fabricated data, new performance claims, reviews or SEO-only assets.
- No bulk ticker sitemap expansion or backend snapshot-generation changes. Snapshot freshness remains governed by the existing jobs; the fallback displays the supplied date.
- No dependency updates or fixes to the 46 unrelated baseline test failures.
- Broader app-route findings were left outside this scope: strategies lacks a self-referencing canonical and the pricing response includes repeated H1 content. These do not affect the homepage's verified single H1/canonical. No pricing or app navigation redesign was undertaken.
