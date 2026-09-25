# Keyword-led indexing priorities — September 21, 2026

The owner requested Google Ads Keyword Planner research followed by Search Console indexing requests for suitable existing Walnut pages. No ad campaign was launched or website code changed.

## Demand evidence

Read directly from Google Ads Keyword Planner in Chrome. Geography: United States; network: Google; period: Last 12 months. Discovery batch used English. The historical-volume batch used English query strings with the language selector locked to All languages. The account reports ranges, not exact search volumes. These are market search estimates, not projected Walnut traffic. Ads competition is not organic ranking difficulty.

| Query | Average monthly searches | Batch |
|---|---:|---|
| congress stock trades | 1,000–10,000 | English discovery |
| insider trading tracker | 1,000–10,000 | English discovery |
| stock screener | 10,000–100,000 | English discovery |
| stock research tools | 100–1,000 | English discovery |
| institutional ownership | 100–1,000 | English discovery |
| nvidia institutional ownership | 100–1,000 | Both |
| government contract stocks | 10–100 | English discovery |
| stock analysis software | 100–1,000 | English discovery |
| stock comparison | 1,000–10,000 | English discovery |
| investment thesis | 100–1,000 | English discovery |
| stock analysis tools | 1,000–10,000 | Historical volume |
| stock research software | 10–100 | Historical volume |
| nbis vs crwv | 100–1,000 | Historical volume |
| who is buying nvidia stock | 10–100 | Historical volume |
| boeing government contracts | 10–100 | Historical volume |
| defense stocks | 10,000–100,000 | Historical volume |
| insider buying stocks | 100–1,000 | Historical volume |
| finviz alternative | 10–100 | Historical volume |
| quiver quantitative alternative | 10–100 | Historical volume |
| unusual whales alternative | 10–100 | Historical volume |
| apple institutional ownership | 10–100 | Historical volume |
| broadcom institutional ownership | 10–100 | Historical volume |
| nebius stock analysis | 10–100 | Historical volume |
| nvidia vs micron | 10–100 | Historical volume |
| nancy pelosi stock tracker | No volume reported (not necessarily zero) | Historical volume |

Saved research plan: https://ads.google.com/aw/keywordplanner/plan/keywords/workspace?ocid=8410154099&planId=1438646259&authuser=0

## Search Console actions

Completed: 12 priority URLs inspected, 9 already indexed, 3 new requests accepted. All three acceptance dialogs were verified. No duplicate requests or sitemap resubmissions were made in this pass.

| URL | Inspection result before action | Action/result |
|---|---|---|
| https://walnutmarkets.com/congress-trades | On Google; indexed | No repeat request |
| https://walnutmarkets.com/insider-trading-tracker | On Google; indexed | No repeat request |
| https://walnutmarkets.com/research/nbis-vs-crwv-ai-neoclouds | On Google; indexed | No repeat request |
| https://walnutmarkets.com/research/who-is-buying-nvidia-stock-in-the-latest-13f-filings | URL unknown to Google | Accepted: Indexing requested; priority crawl queue |
| https://walnutmarkets.com/stock-analysis-tools | Discovered, currently not indexed | Accepted: Indexing requested; priority crawl queue |
| https://walnutmarkets.com/institutional-activity-tracker | Discovered, currently not indexed | Accepted: Indexing requested; priority crawl queue |
| https://walnutmarkets.com/compare/walnut-markets-vs-finviz | On Google; indexed | No repeat request |
| https://walnutmarkets.com/stock-research-software | On Google; indexed | No repeat request |
| https://walnutmarkets.com/research/boeing-government-contract-backlog-ba-stock | On Google; indexed | No repeat request |
| https://walnutmarkets.com/compare/walnut-markets-vs-quiver-quant | On Google; indexed | No repeat request |
| https://walnutmarkets.com/compare/walnut-markets-vs-unusual-whales | On Google; indexed | No repeat request |
| https://walnutmarkets.com/research/public-companies-winning-department-of-defense-contracts | On Google; indexed | No repeat request |

Each candidate is matched by topic and intent, not volume alone. Broad defense-stock demand is not the search volume of Walnut's specific contracts article. Existing indexed pages need better relevance, internal distribution and conversion measurement rather than repeated indexing requests.

## Public technical checks

Evidence files: `artifacts/growth-diagnosis-2026-09-21/keyword-priority-check.json` and `keyword-priority-check-additional.json` in the same directory. The NVIDIA ownership article was also checked independently: direct HTTP 200, one self-canonical, index/follow, no X-Robots-Tag block and allowed by robots.txt.

The checked marketing landing pages, research articles and competitor comparisons returned direct HTTP 200, one matching canonical, no noindex directive and no robots.txt block.

Excluded candidates:

- `https://app.walnutmarkets.com/compare` redirects to `/compare/_/_`, an empty comparison route with noindex. Do not submit this as a generic stock-comparison landing page.
- `https://app.walnutmarkets.com/screener` returned HTTP 200 but no canonical in the server HTML inspected. Its rendered metadata needs separate verification before prioritizing it; this check does not establish that it is noindex.
- The NVIDIA strategic-investments article needs a source-period freshness review, recorded in `docs/distribution-actions-2026-09-21.md`, before further promotion.

Earlier today, requests were already accepted for the main-domain AI-memory-shortage article and AMD ticker page. They were not repeated. NVDA, ANET, ALV, leaderboards and the NASA-contract article were already indexed in the earlier inspection pass.

An accepted indexing request is not confirmation of indexing. Search Console explicitly states that repeat submissions do not improve queue position or priority. No ranking or indexing deadline is promised.

## Recommended focus after these submissions

1. Track the three requested URLs in Search Console; avoid repeated submissions while awaiting crawling.
2. Evaluate impressions, query match and clicks for the already-indexed Congress, insider and research-software pages. Their indexing status means resubmission alone cannot resolve low traffic.
3. Connect relevant research and Reddit discussions to the exact ticker/research workflow and measure signup outcomes. Prioritize NBIS versus CRWV and NVIDIA ownership because the observed demand closely matches existing content.
4. Use competitor comparison pages for commercial-intent traffic. Their 10–100 monthly query ranges are smaller, but visitors explicitly evaluating alternatives may be closer to a purchase; that is a hypothesis to test with conversion data, not a proven conversion rate.
5. Review the screener's rendered canonical separately before making it the target for broad stock-screener searches. The current generic app comparison URL is not an indexable landing page.

The keyword evidence does show searches for “investment thesis” (100–1,000 monthly), but it does not establish that this is the best customer-facing phrase or that searchers want software. Specific tasks such as stock analysis, insider tracking, ownership and stock comparisons provide clearer connections to Walnut's workflows.
