# Walnut Markets: search growth audit

Audit date: September 18, 2026. Read-only inspection of the Chrome Search Console domain property, live public pages, and relevant local source. No website, Search Console configuration, or indexing submissions changed during this audit.

## Assessment

Search traffic is very small, but it is not flat. Recent impressions more than doubled. The evidence supports a combination of low discovery activity, a recently enlarged URL inventory, limited reported linking-domain diversity, and content/discovery improvements that remain necessary. It does not establish a sitewide technical block or a penalty. Google does not disclose the precise reason for every crawl or ranking decision.

The aim should be qualified search traffic to useful research and product pages, not indexing every generated profile. Indexing makes a page eligible to appear; it does not ensure competitive rankings or clicks.

## Search Console evidence

### Performance

Property: walnutmarkets.com domain property. Search type: Web. Three-month report: June 17–September 16, 2026. Recent comparison uses the UI's last-28-days versus previous-28-days preset; exact comparison date endpoints were not separately transcribed.

| Metric | Three months | Last 28 days | Previous 28 days |
|---|---:|---:|---:|
| Clicks | 7 | 3 | 3 |
| Impressions | 489 | 319 | 145 |
| CTR | 1.4% | 0.9% | 2.1% |
| Average position | 43.6 | 49.9 | 34.8 |

Recent impressions increased 120%. Clicks did not increase. These volumes are too small to draw strong conclusions from CTR fluctuations.

| Page | Recent impressions | Previous impressions | Recent position | Previous position |
|---|---:|---:|---:|---:|
| Marketing homepage | 39 | 56 | 19.4 | 33.8 |
| App homepage | 9 | 15 | 2.0 | 5.9 |
| ALV ticker | 64 | 0 | 71.0 | — |
| ANET ticker | 37 | 0 | 53.0 | — |
| NASA department | 37 | 0 | 53.2 | — |
| Insights | 34 | 37 | 31.9 | 24.1 |
| Department of Defense | 32 | 1 | 66.4 | 79.0 |
| ACI ticker | 19 | 0 | 70.1 | — |
| Department of Energy | 18 | 0 | 12.2 | — |
| Marketing compare hub | 15 | 8 | 4.9 | 3.5 |

The blended position worsened partly as previously unseen pages gained low-position impressions. This is not uniform deterioration: the homepage improved while Insights declined. Department of Energy is an early opportunity to inspect at the query level, but 18 impressions are not enough to establish durable demand.

Across three months, six clicks went to the marketing homepage and one to the app homepage. Insights had 79 impressions and no clicks. Queries included both relevant searches such as “dod contracts awarded today” and irrelevant or ambiguous searches such as “walnuts price trend.” Raw impressions therefore overstate qualified investor exposure.

### Indexing and sitemap status

The overview showed 101 indexed pages and about 3.18k not indexed. Its principal category was 3,138 “Discovered—currently not indexed.” This means most of that backlog has not yet been crawled, rather than being rejected after a substantive content assessment.

Other categories: 19 noindex, four redirect pages, three crawled/not indexed, 12 redirect errors, six robots exclusions, one duplicate without selected canonical. Report dates and processing lag differ from live inspection.

All nine submitted sitemap entries showed Success. The marketing sitemap and app sitemap index were both submitted and read September 18. Marketing now reports 27 discovered pages instead of the previously stale 12. The index reports 3,907; its child reports update on different dates. These figures need not match the current live inventory immediately.

The earlier same-day verification documented 4,392 unique sitemap URLs, up from 3,185 on September 13. The site's age is not the age of every URL. That audit checked 207 distinct URLs, including 141 sitemap page samples, not all 4,392 HTML pages. See `seo-verification-2026-09-18.md` for scope and exceptions. Most small exclusion categories were intentional filtered URLs or historical aliases; the three crawled/not-indexed examples were two XML files and a filtered feed, not three valuable research articles.

No further sitemap resubmission is needed now. Google's latest reads confirm receipt of the earlier updates.

### Individual research pages

| Brief | Inspection result | Last crawl displayed |
|---|---|---|
| NVIDIA strategic investments versus core chip sales | Indexed; crawl/fetch/indexing allowed; self-canonical accepted | September 18, 18:10:46 |
| Public companies winning NASA contracts | Indexed; crawl/fetch/indexing allowed; self-canonical accepted | September 18, 18:10:46 |
| NBIS versus CRWV | Indexed; crawl/fetch/indexing allowed; self-canonical accepted | September 10, 03:21:50 |

The NVIDIA and NASA latest crawls occurred after the September 16 performance cutoff. The daily publishing effort that began in September is too recent to evaluate properly. Three successful inspections do not prove every brief is indexed.

NASA's individual inspection showed a sitemap “Temporary processing error,” despite an indexed page and a successful research sitemap report. Recheck if it persists; this is not evidence that all research is blocked.

### Crawl health and links

- 2,667 crawl requests over the report's 90 days; 227 ms average response time.
- 97% HTTP 200. Apex, app and www hosts all showed no host problems.
- 99% refresh; less than 1% discovery.
- 73% of requests were JavaScript resources and 12% HTML. Requests are not unique pages. This does not establish broken JavaScript, and blocking required scripts would be inappropriate.
- No manual actions or security issues detected.
- External Links report: 108 links, concentrated in three listed domains: reddit.com (62), walnut-intel.com (42), t.co (4). These are not 108 independent referring sites. This report is a sample, not a complete backlink inventory; ownership of walnut-intel.com was not established here.

Healthy responses alongside very little discovery are consistent with limited crawl demand rather than an overloaded host. Site age alone cannot explain or resolve that. Google describes relevance, uniqueness, quality and popularity among crawl-demand considerations: [crawl budget guidance](https://developers.google.com/crawling/docs/crawl-budget).

## Confirmed live-site improvement opportunities

1. **Broken research navigation.** The NVIDIA strategic-investments article's “latest earnings view” and “balance sheet snapshot” links lead to `/ticker/NVDA/earnings` and `/ticker/NVDA/financials` on the app host. Both displayed “Page not found.” Use actual supported ticker/tab destinations and validate generated internal links before publishing. The observed result is a visible not-found page; HTTP statuses were not separately captured in this audit.
2. **Research archive discovery.** The live archive shows six of 27 briefs and a Show more button. Local `ResearchBriefsSection.tsx` confirms generated briefs start empty, load in a client-side effect, and paginate through state-changing buttons without distinct page links. Insights uses the same component. Initial observed content contained older static briefs before newer generated cards loaded. Render current links on the server and provide crawlable archive page URLs with normal links. Existing sitemap and other links can still lead Google to articles, as the successful inspections demonstrate. Google explains that its crawler generally does not click load-more buttons: [pagination guidance](https://developers.google.com/search/docs/specialty/ecommerce/pagination-and-incremental-page-loading).
3. **Research sourcing and accountability.** The NVIDIA article's specifically named earnings-release source points to the generic NVIDIA investor-relations homepage. Link the exact release or filing instead. The article lacked an obvious top-level visible byline and publication/update date in the inspected content, although its body contained an as-of date and archive cards have dates. Add truthful authorship/review information, methodology and source timestamps. This audit did not conclude that structured metadata was missing.
4. **Evidence matching the question.** The strategic-investments article has a clear thesis and substantive sections, but its question would be better answered by a sourced comparison table distinguishing investment exposure from operating revenue/cash flow. Do not imply these are directly interchangeable measures. Financial figures were not independently validated in this SEO audit.
5. **Overlapping topics.** Two adjacent NVIDIA briefs ask who is buying and whether institutions are still buying after the same filing period. Check query overlap before creating more variants. Update one strong article when there is no materially new question or evidence. Keyword cannibalization is a risk to investigate, not a measured finding here.

## Recommended order of work

### First week

Fix the two broken destinations and the generator's route validation. Make research archive links available in initial HTML and add linked pagination. Select 10–20 priority landing pages and record individual indexing status, target intent, internal links and current query performance. Retain intentional noindex controls for private/filtered/duplicate pages.

### Following weeks

Build a small set of connected research topics around Walnut's own useful evidence: institutional position changes, named insider activity, government-contract recipients and comparative company research. Each article should answer a distinct question, give an answer near the top, identify the reporting period, show original evidence and limitations, and link to the relevant product workflow. Do not describe delayed holdings disclosures as real-time purchases.

Prefer a strong existing article updated with new evidence over a fresh near-duplicate URL. Daily publication is reasonable when quality and novelty justify it; frequency itself is not the objective. Google's content guidance emphasizes original value, clear sourcing and accurate authorship: [helpful-content guidance](https://developers.google.com/search/docs/fundamentals/creating-helpful-content).

Distribute useful charts and findings to relevant investor communities, newsletter writers and journalists, with an accessible source page. Aim to earn independent relevant references. Do not purchase bulk backlinks or expect social posting alone to produce rankings.

Improve titles and descriptions against actual page/query intent, beginning with pages already receiving relevant impressions. A proposed shorter NVIDIA title is “NVIDIA Strategic Investments vs. AI Chip Sales: What Drives the Business?” Treat this as an editorial proposal, not a proven CTR improvement or measured search-volume target.

### Measurement and timing

Review the priority pages after 30 days for discovery, indexing, new relevant queries and impressions. Use 60–90 days to assess sustained changes in non-brand clicks and rankings for those same pages and queries. These are management checkpoints, not promised Google timelines. Compare like-for-like pages as well as the whole property, because new low-ranked URLs can distort the blended average.

Track: indexed priority pages; relevant non-brand impressions; page/query pairs reaching top 20 and top 10; organic clicks; and verified organic signups/activation. With seven Google clicks in three months, there is not enough organic traffic to diagnose the absence of paid conversions from SEO alone.

Google says recrawling can take days to weeks, but requests do not guarantee inclusion and repeated submissions do not accelerate it: [recrawl guidance](https://developers.google.com/search/docs/crawling-indexing/ask-google-to-recrawl). There is no defensible date when every Walnut page will be indexed; some low-value or duplicative pages may never be selected. Sitemap submission is a discovery aid, not an indexing guarantee: [sitemap overview](https://developers.google.com/search/docs/crawling-indexing/sitemaps/overview).

## Scope limitations

This is a sampled audit, not a complete crawl of every URL or a financial fact-check. GSC reports have different update dates and omit some low-volume query data. The inspected Chrome app session was logged in; successful public Google inspections independently demonstrate access to the three named research pages. Local source findings agree with observed archive behavior but were not a deployment commit comparison. Recommendations do not claim a proven single cause for slow search growth.
