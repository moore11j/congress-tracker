# Search and acquisition audit — October 5, 2026

Read-only review through the owner's Chrome extension: Search Console performance, indexing, sitemaps, crawl statistics, manual actions, security, links and three URL inspections; GA4 acquisition, landing pages, events, and pages/screens. Anonymous HTTP checks covered NVIDIA's ticker, its buyers article and the research sitemap. No site, account configuration, indexing submission, advertising, email or deployment changes were made.

## Assessment

Organic acquisition remains very weak. There are modest positive signals, but no demonstrated scalable acquisition or paid-conversion engine. Evidence supports slow discovery/recrawling of important content, weak visibility for indexed pages, a rapidly growing URL inventory, and very few visitors taking meaningful product actions. It does not establish an AI penalty, a financial-site impression cap, a manual penalty, or a guaranteed future date when Google will index everything.

## Search Console

The comparison is **September 6–October 3 versus August 9–September 5**, Web (text), domain property, no query/page filter. Dates verified in the comparison dialog.

| Metric | Latest 28 days | Previous 28 days |
|---|---:|---:|
| Clicks | 7 | 2 |
| Impressions | 182 | 285 |
| CTR | 3.8% | 0.7% |
| Average position | 36.1 | 49.7 |

Impressions fell 36.1%; seven clicks versus two is too small to declare a sustained win. Position averages reflect a changing query/page mix, not a controlled ranking comparison. September 27–October 3 alone recorded **3 clicks, 47 impressions, 6.4% CTR and position 28.3**. The originally open three-month report ended October 2; it showed 11 clicks/555 impressions/41.6 position and was superseded for analysis by the explicit newer windows above.

Six of the seven recent clicks went to the marketing homepage; one went to MSFT. Research pages in the expanded page table had no clicks. The visible query table accounts for only a subset of clicks; do not infer that homepage clicks were all branded. Several visible queries are poorly matched or ambiguous (including agricultural walnut terms); search-query counts are sparse.

| Selected page | Recent impressions | Previous impressions |
|---|---:|---:|
| ALV ticker | 35 | 33 |
| Department of Energy | 24 | 0 |
| Marketing homepage | 24 | 41 |
| ARQT ticker | 11 | 0 |
| ANET ticker | 10 | 29 |
| ACI ticker | 10 | 9 |
| Department of the Interior | 9 | 0 |
| NBIS ticker | 4 | 0 |
| Insights | 3 | 46 |
| NASA department | 0 | 37 |
| Defense department | 0 | 33 |
| NVDA ticker | 0 | 4 |

Page-level impressions must not be summed against property totals. New department/smaller-ticker visibility is a modest lead to investigate, not proof those markets will convert.

### Indexing and crawling

The Page Indexing dashboard is **last updated September 20**, despite this October 5 inspection: 113 indexed, 3,901 not indexed, including 3,854 discovered/not indexed, 20 noindex, 4 redirects, 4 crawled/not indexed, 12 redirect errors, 6 robots blocks, 1 duplicate. These are dated categories, not verified current defects. The September 21 audit recorded 105 indexed on a September 17 report. Do not compare the stale indexed count to today's sitemap inventory as an exact indexation percentage.

All nine submitted sitemaps show Success. Marketing, ticker and research maps were last read October 3; app index September 28. GSC now reports 7,402 discovered pages for that index, 2,565 tickers, 3,559 insiders, 868 institutions, 237 members, 38 departments, 38 research URLs, 7 comparisons and 32 marketing URLs. The index overlaps its children; do not add these counts. The index's recorded inventory grew from 3,907 in the user's September screenshot. Successful sitemap parsing is not proof of crawling its URLs.

Crawl Stats, through **October 3**, reports 3,407 requests across 90 days: app 2,018, marketing 1,370, www 19. All hosts show No problems; 98% of responses are 200; average response is 176 ms. Purpose rounds to 100% refresh and less than 1% discovery. Resources comprise 76% JavaScript and 12% HTML; 84% of requests are page-resource loads. These percentages alone do not prove a rendering fault and do not justify blocking JavaScript. Both Manual Actions and Security Issues show No issues detected.

URL Inspection sample:

- **NVDA ticker:** indexed; last recorded crawl August 6, successful/allowed, Google canonical is inspected URL, historical user canonical None. Today's anonymous page returns 200 directly, one self-canonical and index/follow, no X-Robots-Tag. Google has not recorded a recent crawl of the repaired page.
- **NASA department:** indexed; last recorded crawl August 19, successful/allowed; Google canonical inspected URL, historical user canonical None. Its loss of impressions is not explained by a current unindexed status in this inspection.
- **NVIDIA 13F buyers article:** URL unknown to Google, no recorded crawl. Today's URL returns 200 directly with one self-canonical and index/follow, no X-Robots-Tag; it is present in the live research sitemap. The earlier September 27 report called it discovered/not indexed. This report records the current tool response without interpreting the changed label as a known cause or lost indexation (neither status is indexed).

No fresh rendered live test or complete site crawl was performed. Two anonymous page checks do not certify all public templates.

### External discovery

Google's Links sample now contains **253 external links**, up from the September 21 recorded 108. Of these, 215 point to the homepage, 20 to Insights, 15 to app pricing, 2 to NBIS and 1 to TSM. Linking domains shown: Reddit 170, walnut-intel.com 76, t.co 5, freshbuilds.io 1, rebellionresearch.com 1. This is a limited Google sample, not a full link inventory or quality score. About 85% of sampled external links target the homepage; individual research pages have little visible support in this sample.

## Google Analytics 4

Dates: **September 7–October 4 versus August 10–September 6**. These are one day later than GSC's finalized comparison. All Users, without a newly applied internal-user exclusion. Reports mostly state 100% of available data; the source/medium report states mostly complete data. GA4 organic sessions are not the same unit as GSC clicks.

| Metric | Latest 28 days | Previous 28 days |
|---|---:|---:|
| Active users | 240 | 466 |
| New users (analytics identities, not registered accounts) | 233 | 464 |
| Sessions | 418 | 688 |
| Engaged sessions | 206 | 319 |
| Engagement rate | 49.28% | 46.37% |
| Page views | 1,867 | 4,809 |
| Organic Search sessions, all engines | 22 | 22 |
| google / organic sessions | 21 | 5 |
| Recorded key events | 1 | 0 |
| Recorded revenue | $0 | $0 |

Historical instrumentation changed during these windows, including the September 11 duplicate page-view correction and new product events. Raw page-view declines are not a clean estimate of lost customer traffic. No Stripe/account reconciliation or current paid-forwarding configuration inspection was done, so GA4 revenue and signup events are not authoritative business totals.

Recent channel behavior:

- Direct: 224 sessions, 27.23% engaged, seven seconds average engagement/session; strongest volume but weak measured attention. Direct also includes unattributed activity; not synonymous with intentional brand visits.
- Organic Social: 93 sessions, 84.95% engaged, 14m38s/session. The unusual duration needs internal-user separation before calling it a customer success.
- Organic Search: 22 sessions, 77.27% engaged, 1m20s/session, no key events.
- Paid Search: six sessions, 33.33% engaged, 29 seconds/session, no key events. This is not an Ads billing/delivery audit.
- accounts.google.com/referral still receives 59 sessions. The September record says it was already excluded from unwanted referrals; investigate current stream/tag/acquisition behavior and attribution persistence, rather than blindly re-adding the rule.
- Two Reddit labels account for 68 sessions (42 reddit/(not set), 26 reddit.com/referral), versus 79 previously. Missing medium and internal use complicate attribution. No recorded key events from these rows.

Page attention is concentrated among few identities: homepage path 390 views/132 active users; leaderboards 85/13 (previously 62/11); Strategies 109/9; Insights 104/9; NVDA 58/8. Page paths can combine matching paths on both hosts. These are candidates for investigation, not clean external-user cohorts. Admin Settings alone has 94 views from three users and 64 minutes average engagement/user; Research Admin has 60 views from two users and 82 minutes average. Admin landing pages account for at least 34 of 418 sessions. Removing only admin paths would still leave those users' product browsing mixed in.

Event inventory: ticker_viewed 235 events/19 users; confirmation_score_viewed 175/3; pricing_viewed 22/9; upgrade_prompt_clicked 4/2; signup_started 6/2; signup_completed 1/1; signin_started 14/9; signin_completed 7/5; strategy_followed 1/1; no recent ticker_follow_complete. No checkout or subscription-completion row appeared in the 55-event comparison inventory. These are independent counts, not a validated sequential funnel or proof of a login failure. The one key event is attributed to Direct and a homepage-entry session. It is not proven to be a new external customer without reconciliation.

## Recommended next work, in order

1. **Finish measurement verification, without undoing existing repairs.** Reconcile non-admin/non-test accounts and positive live payments with eligible GA4 events; inspect persistent Google-login referral attribution and missing UTM medium. Produce an external-user stock-view → signup → save/follow → seven-day return → paid-start cohort. Existing event definitions are not the missing deliverable; verified receipt and exclusions are.
2. **Track a fixed ten-page search cohort instead of chasing total sitemap size.** Include insider tracker, stock-analysis platform, NVDA, ANET, NBIS, ALV, NASA, Energy, the NVIDIA buyers brief and a reviewed NBIS comparison. Verify current public evidence, links from homepage/hubs, sitemap lastmod truthfulness and Google-rendered content. Preserve existing routes and entitlements. After a material fix, request recrawl once and record date/result; don't repeatedly submit successful unchanged sitemaps. No submissions in this audit.
3. **Improve and distribute a few distinctive existing resources.** Prioritize a named-buyer/share-change study, a current NBIS comparison and a public-company contract-winner table. Audit overlap with existing briefs before creating new URLs. Lead with the actual finding, comparison period, sources and uncertainty; show the matching Walnut workflow. The October 4 writing upgrade is too new for an outcome assessment and is not a substitute for editorial review.
4. **Send relevant readers directly to the resource.** Useful, short Reddit links and original charts that others can cite should point to the matching brief/ticker, with complete campaign tags. Pursue relevant editorial coverage for an original finding; no link buying, generic link spam, or new messaging without authorization. Broad homepage links currently dominate Google's sample.
5. **Run a four-week learning cycle with explicit evidence.** Weekly record the ten URLs' last-crawl dates/index status, relevant non-brand impressions/clicks, external qualified landings, first stock actions, verified signups and return visits. Keep the hero stable during this cycle. If recrawling improves but traffic does not, work on query fit/content competitiveness. If qualified landings grow but actions do not, investigate the product journey. If crawling remains stale, use verified server logs and rendered-page evidence to locate the next technical bottleneck.

Four weeks is a diagnostic review interval, not a ranking promise. More age alone is not a strategy. Google does not promise to crawl/index all pages; even indexed pages may attract almost no traffic. The current sample cannot prove willingness to pay or its absence.

Official references: [Google crawl capacity and demand](https://developers.google.com/crawling/docs/crawl-budget); [using GSC and Analytics together](https://developers.google.com/search/docs/monitor-debug/google-analytics-search-console). Google distinguishes site capacity from crawl demand and considers page value/popularity; observed healthy hosts plus slow recrawling support a demand/discovery hypothesis, not a proven secret throttle.

## Checks and boundaries

Browser reports and anonymous responses reviewed; documentation diff/link checks only, no application regression tests. Report and repository memory updated locally; no commit/deploy or production changes. Current provider credentials, true paid-customer totals, complete rendering coverage and clean external cohorts remain unverified.
