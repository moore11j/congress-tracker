# Final scheduled four-week growth review: November 2, 2026

Read-only review under [the approved October plan](growth-execution-2026-10-05.md). Search Console and GA4 were read through the owner's Chrome session. The first Chrome connector timed out, but the browser extension fallback worked. All ten public HTTP checks completed successfully. No application code, homepage hero, entitlements, posts or articles were changed; no indexing requests, email, spending or deployments occurred.

This is the final scheduled assessment, with an explicit limitation: Walnut redirected the admin page to login. Automatic approval review rejected initiating Continue with Google because the monitoring authorization covered an existing admin session. A sign-in approval question is pending. No alternative private-data access was attempted. Current authoritative account creation, completed activation, returning accounts and live payments remain unverified in this review. GA events are not substituted for those records.

## Assessment

The four weeks did not produce an organic growth breakthrough. Search impressions declined, and the fixed cohort contributed no reported clicks. Nine of ten priority pages remain indexed, so repeatedly submitting this cohort is not the main remedy. Current public delivery passes, but several stored Google crawl dates are still in August or September. This review does not establish a penalty, an impression ceiling, or the cause of Google's crawl scheduling.

The conversion evidence remains weak and partly contaminated: known internal traffic is present, the older Reddit baseline includes explicitly named test campaigns, and the actual account/payment reconciliation is blocked. There is evidence that people use stock research pages, but not enough verified external activation or payment evidence to claim improved commercial traction.

The owner separately approved and deployed the new homepage wording on October 31 (PR745, dbbe9cdd). That breaks the original unchanged-hero experiment assumption. Most of this comparison predates the new hero; no conversion effect is attributed to it. Preserve the current hero.

## Search: equal completed windows

Google Search Console Web search. Date-picker boundaries verified: October 4 to October 31 versus September 6 to October 3, 2026. The latter matches the saved October 5 baseline exactly.

| Metric | October 4 to 31 | September 6 to October 3 |
| --- | ---: | ---: |
| Impressions | 111 | 182 |
| Clicks | 5 | 7 |
| CTR | 4.5% | 3.8% |
| Average position | 26.1 | 36.1 |

Impressions declined 39.0%; clicks declined by two. Position improved within the mix of impressions recorded, not necessarily for the same queries. This is not proof of broad ranking improvement.

Completed week October 25 to 31 versus October 18 to 24: 16 versus 14 impressions, 2 versus 1 clicks, CTR 12.5% versus 7.1%, average position 4.8 versus 43.6. The very small sample makes the apparent position jump unreliable as a general trend. It is also far below the saved baseline week of 47 impressions and 3 clicks.

All 54 disclosed query rows were read. They account for 42 current versus 83 prior impressions and zero versus one disclosed clicks. One clearly branded query, "evaluate walnut as an ai investing app", has one current impression and zero clicks. "walnut market" and "walnut market report" contribute four ambiguous impressions. The other 37 current disclosed impressions are nonbranded, including three commodity-related impressions. These disclosed rows do not reveal the complete brand split of the property totals. The largest disclosed query, "masaru corporation bullish and bearish analyst opinions", contributes 15 impressions; its intent/entity fit needs investigation before selecting it as a content target.

The homepage generated four of the five current clicks (31 impressions). The Department of Defense page generated the other click (3 impressions). Other visible opportunities include ARQT's ticker (20 impressions), Insights (15), and the existing NVIDIA institutional-activity research article (14). These are small observed opportunities, not verified high-volume, low-competition keywords.

## Fixed ten-page cohort

All ten fresh public checks pass: direct HTTP 200, exactly one matching canonical, no blocking robots meta or HTTP header, robots.txt allowed, and canonical sitemap membership. The NBIS/CRWV brief omits a robots meta tag, which permits indexing by default. Public audit receipt is saved alongside this report. Eligibility is not indexing.

All ten stored URL inspections were read. Nine are indexed, as at baseline; the NVIDIA buyers brief is not indexed. Its current stored reason is "URL is unknown to Google", with no recorded crawl or referring sitemap. October 26 showed discovered but not indexed. This is a change in reported discovery detail, not a newly lost indexed page. The public audit still finds its sitemap membership. No live-test or indexing request was made.

| Page | Current / baseline impressions | Stored index state | Last crawl displayed (2026) |
| --- | ---: | --- | --- |
| /insider-trading-tracker | 2 / 1 | Indexed | September 19, 19:17:07 |
| /stock-analysis-platform | 3 / 5 | Indexed | October 23, 23:06:12 |
| app /ticker/NVDA | No row in either window | Indexed | August 6, 19:36:21 |
| app /ticker/ANET | 0 / 10 | Indexed | August 28, 11:06:40 |
| app /ticker/NBIS | 1 / 4 | Indexed | September 26, 19:03:32 |
| app /ticker/ALV | 2 / 35 | Indexed | August 28, 11:04:57 |
| app /departments/nasa | No row in either window | Indexed | August 19, 13:12:51 |
| app /departments/department-of-energy | 0 / 24 | Indexed | September 9, 22:59:53 |
| /research/who-is-buying-nvidia-stock-in-the-latest-13f-filings | No row in either window | Unknown, not indexed | N/A |
| /research/nbis-vs-crwv-ai-neoclouds | 3 / 1 | Indexed | October 5, 19:54:36 |

All 110 reported page rows were read. Listed cohort impressions sum to 11 versus 80, zero clicks in both windows. These page-level totals are not additive shares of property-level impressions. Missing rows do not prove a URL is unindexed. Inspection display timezone was not independently verified. The stored crawl dates are unchanged from October 26. Several stored summaries say declared canonical None, while today's HTTP markup has a valid canonical. NASA's inspection reports a temporary sitemap processing error while still indexed.

The lagged indexing inventory is updated October 22: 145 indexed, 12,213 discovered but not indexed, 10 crawled but not indexed, 26 noindex, 4 redirects, 12 redirect errors, 6 robots-blocked, 1 duplicate and 0 alternative canonical pages. The duplicate validation says Passed despite the displayed historical count. Compared with October 18's 146 indexed and 11,200 discovered, this is a growing discovery backlog, not evidence of mass deindexing. These exclusion groups were not individually re-audited.

## Analytics: completed 28-day comparison

GA4 date picker: October 5 to November 1 versus September 7 to October 4, exactly matching the saved GA baseline. These are equal completed 28-day windows; November 1 can still receive processing updates. Property timezone was not independently rechecked. These are consented analytics totals including internal and unidentified activity, not verified external accounts.

| Metric | October 5 to November 1 | September 7 to October 4 |
| --- | ---: | ---: |
| Sessions | 256 | 418 |
| Active users | 179 | 240 |
| Engaged sessions | 115 | 206 |
| Engagement rate | 44.92% | 49.28% |
| Average engagement per session | 51 seconds | 3m 49s |
| Google organic sessions | 19 | 21 |
| Direct sessions | 150 | 224 |
| Reddit.com referral sessions | 58 | 26 |
| Google account referral sessions | 15 | 59 |
| signup_completed events | 1 | 1 |
| GA revenue | $0 | $0 |

The current signup event is attributed to Google organic. Google organic has 17 engaged sessions, Reddit referral 50. Prior Reddit sources also include 42 reddit/(not set) sessions and one reddit/(none) session explicitly tagged campaign "test". It would be misleading to combine those with organic customer traffic or interpret the referral-row increase alone as acquisition growth.

Event-scoped Internal activity shows current Google organic sessions associated with true=9, false=19 and unknown=8; Reddit referral true=18, false=52 and unknown=32. Unset events are also present. These subsets overlap: a session can contain multiple classifications. Do not subtract or sum them to infer verified external visitors. False can include guests and is not authenticated external identity. The dimension was created at the beginning of the current window, so prior missing values are not a comparable clean external baseline.

All 19 source/campaign rows were read. No october_research campaign row appears. This means no reported tagged GA acquisition, not proof that nobody clicked those links. Consent, tag retention and attribution need reconciliation with first-party data.

## Product use, activation and payments

All 50 GA event rows were read. Counts below are telemetry events, including internal/unknown activity; they are not a same-person funnel.

| Event | Current / previous counts | Current / previous GA users |
| --- | ---: | ---: |
| ticker_viewed | 64 / 235 | 41 / 19 |
| research_entry_clicked | 28 / 14 | 21 / 5 |
| analyze_stock_click | 12 / 20 | 11 / 10 |
| save_stock_click | 4 / 0 | 4 / 0 |
| signin_started | 12 / 14 | 12 / 9 |
| signin_completed | 3 / 7 | 3 / 5 |
| signup_started | 3 / 6 | 3 / 2 |
| signup_completed | 1 / 1 | 1 / 1 |
| pricing_viewed | 17 / 22 | 10 / 9 |
| upgrade_prompt_viewed | 209 / 66 | 42 / 12 |
| upgrade_prompt_clicked | 0 / 4 | 0 / 2 |
| strategy_followed | 0 / 1 | 0 / 1 |

No checkout-start, subscription-completed or purchase event row appears in this comparison. Four save-button clicks do not establish four completed saves. A sign-in start is not a new registration, and the start/completion count gap alone does not prove an OAuth defect. More upgrade prompt views without clicks warrants a review of where value is demonstrated, but does not authorize changing entitlements.

The admin aggregate report is unavailable because sign-in is blocked pending approval. Current verified account creation, completed saves/follows, returning accounts, positive live payments and the late-October signup's subsequent retention are therefore unknown. Do not report them as zero. The last authoritative read, documented [October 26](growth-review-2026-10-26.md), found one verified new account and no positive live paid invoices for October 18 to 24, three active accounts including two with earlier activity, and no save/follow or checkout activity in that weekly window. Those historical weekly values cannot be substituted for the current full four-week window.

## Priorities after monitoring

1. Complete first-party reconciliation after approved admin sign-in: equal completed dates, known admin/test exclusion, unidentified bucket, verified accounts, completed saves/follows, returning accounts and positive live invoices. Reconcile the one signup event and four save clicks without manufacturing transactions. Keep paid-event forwarding unverified until a real positive live payment can be matched.
2. Investigate discovery of the existing NVIDIA buyers brief and current guest render/internal links for the older-crawled cohort. Confirm links from Insights, ticker Research and the research directory; distinguish fresh render quality from a stored old crawl. Do not keep resubmitting indexed URLs or create more near-duplicate briefs to fill the backlog.
3. Review actual external stock-research-to-save/sign-in journeys and whether the paid prompt arrives after a useful result. The current aggregate event totals justify investigation, not a diagnosis of a broken button or permission to change prices/paywalls.
4. Concentrate the next authorized content/distribution test on a small set of existing pages with relevant observed interest (ARQT, sourced NVIDIA institutional research, government contracts) and one concrete research-to-save action. Treat keyword demand and competitive difficulty as separate evidence still to verify. Continue measuring tagged Reddit links rather than claiming referral traffic is already converting.
5. Keep the October 31 hero stable. Its two-day exposure cannot explain the preceding month's results. Set the next decision around verified external activation and relevant nonbrand clicks, not a promised indexing date or a daily impression target.

The finite October 12/19/26 and November 2 automation was deleted after this final scheduled check. The pending admin reconciliation is an explicit manual follow-up, not a claim that all measurement work is complete. Only aggregate findings and project context were saved. No application regression tests were run because no application files changed.
