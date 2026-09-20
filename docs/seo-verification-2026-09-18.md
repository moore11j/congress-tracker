# Walnut sitemap and Search Console verification — September 18, 2026

Read-only production verification, performed September 18 Pacific / September 19 UTC. No website configuration, indexing requests, validation requests, or sitemap submissions were changed.

## Scope and result

All 10 XML files reached through the two public robots files and submitted sitemap roots returned HTTP 200 and parsed successfully. They advertise 4,392 unique page URLs. Every inventory URL uses HTTPS, excludes www and query strings, and is permitted by its host's current robots.txt. No within-file duplicate entries were found. Shared research and comparison entries across sitemap files were deduplicated for counting; this overlap is not itself an error.

Performed 215 page/file fetch checks covering 207 distinct URLs: 141 sitemap page samples, all 45 examples in the six small Search Console exclusion categories, 10 examples from the discovery backlog, and 19 additional canonical destinations. All final HTTP responses were 200. All 141 sitemap samples and all 19 additional canonical destinations passed robots, noindex, redirect, and single self-referencing canonical checks. Sampling included every department, research, comparison, and marketing sitemap page, plus 12 randomly selected entries per large entity sitemap and targeted ticker checks. **This is not a fresh HTML crawl of all 4,392 pages.**

## Sitemap inventory

| Sitemap | Live entries | Search Console last read | GSC discovered entries |
|---|---:|---|---:|
| app.walnutmarkets.com/sitemap-index.xml | 7 child files / 4,373 unique pages | September 13 | 3,907 |
| app.walnutmarkets.com/sitemap-tickers.xml | 1,040 | September 17 | 1,037 |
| app.walnutmarkets.com/sitemap-members.xml | 237 | September 13 | 237 |
| app.walnutmarkets.com/sitemap-insiders.xml | 2,156 | September 16 | 1,694 |
| app.walnutmarkets.com/sitemap-institutions.xml | 868 | September 12 | 868 |
| app.walnutmarkets.com/sitemap-departments.xml | 38 | September 17 | 38 |
| app.walnutmarkets.com/sitemap-research.xml | 27 | September 18 | 26 |
| app.walnutmarkets.com/sitemap-comparisons.xml | 7 | September 8 | 7 |
| walnutmarkets.com/sitemap.xml | 27 | July 21 | 12 |
| walnutmarkets.com/sitemap-research.xml | 27 | Not separately submitted in visible list | — |

Every submitted sitemap shows Success in Search Console. Both research files contain the same canonical marketing-host research URLs. A separate submission of the marketing research file is optional because it is declared in robots.txt and its pages are already in the app research sitemap.

The dynamic research sitemap includes a September 18 lastmod; ticker and insider inventories have grown since Google's last reads. Older activity dates in otherwise valid entity sitemaps do not, by themselves, mean the files are stale.

One confirmed freshness cleanup remains: the static marketing sitemap gives the homepage a September 10 lastmod, but commit 55d722f2 changed the homepage on September 13. Update that date to reflect the actual significant content change, and maintain accurate modification dates going forward. Do not replace all dates with today's date. This stale date is not an indexing block.

## Exclusion findings

The Page indexing report is last updated September 13: 101 indexed, approximately 3.18k not indexed.

| Report category | Live result / interpretation |
|---|---|
| Excluded by noindex: 19 | 16 filtered ticker URLs, the app feed root, and /signals remain intentionally noindexed and absent from sitemaps. The one canonical profile, /member/BYRON_DONALDS, is now 200, index/follow, self-canonical, and included. All six unfiltered ticker canonical destinations also passed. |
| Redirect error: 12 | All 12 old hyphenated member URLs now return 200 with canonical tags pointing to the uppercase underscore member URLs. All 12 canonical destinations return 200, index/follow, and self-canonicals and occur in the sitemap. No redirect loop reproduced. These aliases currently canonicalize through HTML rather than HTTP redirects. |
| Blocked by robots.txt: 6 | All six are now permitted. The five clean insider profiles are indexable and in the sitemap. The sixth is a filtered variant with intentional noindex and a canonical to the clean John Bolduc profile. |
| Duplicate without user-selected canonical: 1 | https://www.walnutmarkets.com/faq returns one HTTP 308 directly to https://walnutmarkets.com/faq; final 200, index/follow, exactly one matching canonical. Only non-www FAQ appears in the sitemap. GSC's displayed last crawl is June 15. |
| Page with redirect: 4 | Expected normalization: app-host research redirects 301 to its marketing canonical; HTTP apex redirects 308 to HTTPS; HTTPS www root redirects 301 to apex. HTTP www root uses two hops (HTTPS www, then apex), a minor optimization opportunity rather than a loop. All destinations are valid canonical pages. |
| Crawled, currently not indexed: 3 | Two XML sitemap files and the filtered app feed /?mode=all. These are not three missing public content pages. The feed is intentionally noindexed and excluded from sitemaps. |
| Discovered, currently not indexed: 3,138 | Ten displayed examples checked. All return 200. Eight department examples are indexable and self-canonical. One old insider name canonicalizes to its current sitemap name, whose destination passed; the other insider example is already self-canonical and indexable. This sample does not establish Google's reasons for delaying every URL. |

The National Transportation Safety Board canonical URL also passed **Google's own live smartphone inspection** at September 18, 18:23:52 Pacific: URL available to Google, page can be indexed, crawl allowed Yes, fetch Successful, indexing allowed Yes, correct user-declared canonical. Its normal indexed-data view still says Discovered—currently not indexed, no last crawl, and no detected referring page. The live test demonstrates current accessibility; it does not add the page to Google's index.

## Crawl activity and next steps

Crawl Stats, updated September 16, shows 2,667 total requests across hosts, 227 ms average response time, 97% HTTP 200, and No problems for app, apex, and www. These requests include resources, not just pages. The report classifies 99% as refresh and under 1% as discovery; 73% are JavaScript resources and 12% HTML. The resource percentages alone do not prove a JavaScript rendering defect or justify blocking scripts.

The evidence is consistent with slow discovery/crawl prioritization rather than an ongoing blanket robots or noindex block. The inventory has grown from the September 13 audit's 3,185 URLs to 4,392. Passing technical checks cannot compel Google to crawl or index all of them.

Recommended actions:

1. Correct the homepage's stale lastmod, then resubmit https://walnutmarkets.com/sitemap.xml once. Google's visible copy still shows July 21 and only 12 URLs, while the live file has 27.
2. Optionally resubmit https://app.walnutmarkets.com/sitemap-index.xml once to refresh the child inventory. Do not delete and re-add every child or repeatedly submit unchanged files. Existing child submissions can stay.
3. Start Validate Fix for the 12 redirect errors. Robots and FAQ validation already show Started; leave those running. Do not attempt to eliminate legitimate noindex or ordinary redirect exclusions by making variants indexable. Check the repaired Byron Donalds profile individually, or filter validation to its submitted sitemap.
4. Request indexing for a small set of priority canonical pages, including the repaired department and member pages, rather than thousands of variants. Strengthen crawlable internal links from relevant public category and research pages to priority profiles. The inspected department has no detected referring page, although GSC's referring-page data is not exhaustive.
5. Monitor discovery and priority-page indexing over subsequent report updates. Improve distinctive, useful page content and relevant external visibility; resubmission alone does not guarantee indexing.

Google references: [Sitemap guidance](https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap), [Page indexing and validation](https://support.google.com/webmasters/answer/7440203), [Recrawl requests](https://developers.google.com/search/docs/crawling-indexing/ask-google-to-recrawl).

## Evidence files

Public audit data and scripts are under `backend/artifacts/sitemap-verification-2026-09-18/`: inventory.json, sample-pages.jsonl, sample-summary.json, gsc-examples.json, gsc-live.jsonl, and canonical-destinations.json. Earlier September 13 results are historical context only, not counted as current live checks.

## Authorized follow-up completed September 18

After the user requested implementation of the recommendations:

- Corrected only the homepage lastmod in `frontend/public/sitemap.xml` from September 10 to September 13, matching the significant homepage update. Commit `ab1ccdf5` was pushed to main; Vercel reported successful deployment. Production `https://walnutmarkets.com/sitemap.xml` returned HTTP 200 with the corrected date. XML parsing confirmed 27 unique URLs.
- Resubmitted `https://walnutmarkets.com/sitemap.xml` once. Search Console displayed **Sitemap submitted successfully**.
- Resubmitted `https://app.walnutmarkets.com/sitemap-index.xml` once. Search Console displayed **Sitemap submitted successfully**. Existing child submissions were retained.
- Started validation for the 12 redirect-error examples. Search Console displayed **Validation Started — Started: 18/09/2026**.
- Requested indexing for the canonical National Transportation Safety Board, National Science Foundation, and Byron Donalds pages. Each request received **Indexing requested**, confirming admission to Google's priority crawl queue. This confirms request acceptance, not completed indexing.
- Existing robots/FAQ validation and intentional noindex rules were left intact. No unrelated working-tree changes were committed.
