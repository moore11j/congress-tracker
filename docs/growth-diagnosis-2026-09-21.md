# Walnut growth diagnosis — September 21, 2026

Investigation: live Search Console, Walnut admin reports, a separate guest browser journey, current source code, the GA4 reports read during this investigation, and prior deployment/verification records. No production settings, entitlements, pages, pricing, or accounts were changed. No messages or posts were sent. No signup or purchase was completed.

## Decision

The clearest immediate problem is unnecessary friction in free email signup. The wider business problem is a small flow of visitors reaching useful product actions, with measurement that exaggerates or obscures that activity. Search discovery is weak, but the evidence does not show a general current crawl-blocking outage. A different hero alone cannot resolve these problems.

The hypothesis worth testing is: people considering a stock want to see where the data agrees, where it conflicts, and what changes afterward. Payment demand for that workflow remains unproven. Market commentary engagement is evidence of interest in the commentary, not evidence of willingness to subscribe to research software.

## Evidence and limits

| Evidence | Observation | What it supports |
|---|---|---|
| Guest NVDA → Follow → Create free account | Registration requires first name, last name, email, password, country, postal code, city, street address; region is additionally required for some countries | Confirmed signup friction; its conversion impact has not yet been measured |
| Backend registration schema and validation | Address requirements are enforced by the API, not just cosmetic form labels | Fix must cover both frontend and backend while retaining billing requirements at paid checkout |
| Google signup code | Callback preserves the requested destination without a billing-profile gate in the inspected path | Do not claim the email address hurdle affects every signup method; new Google account creation was not executed |
| Follow navigation | Registration URL retains `/ticker/NVDA?follow=1` | Return-to-stock handling is already present; do not rebuild it blindly |
| Admin business summary | 18 account records total, 4 created in last 30 days, 4 active Free, 2 Premium, 2 Pro; MRR $0 | Real account scale is small. Total/new counts do not exclude admin/test/deleted records; plan access is not proof of payment |
| Sales ledger | September has one $0 Premium transaction; historical ledger includes positive invoices and a negative adjustment; YTD report is $41.14 | Current recurring revenue is zero. It is inaccurate to say the database contains no historical payment activity. Test/customer status of old invoices is not verified |
| GA4, Aug 24–Sep 20 | 300 active users, 508 sessions, 2,738 page views, zero key events | Visitors are not registered accounts; zero tracked conversions cannot establish zero signups |
| GA4 same window | 8 users with `pricing_viewed`, 2 with `upgrade_prompt_clicked`, 1 with `signup_started`; no listed checkout-start/completion event | Very little measured purchase intent; counts span instrumentation changes and are not a validated sequential conversion funnel |
| GA4 usage concentration | Admin Settings: 133 views from 2 users; Outcomes: 111 views from 4 users; Strategies: 121 views from 12 users | Aggregate engagement is influenced by a small group; internal/test use must be separated before drawing customer conclusions |
| First-party recent 7 days | 50 Confirmation Score events from 1 signed-in identifier; 61 ticker-viewed events from 5 identifiers | Broad independent use of the central paid feature has not been demonstrated |
| Live guest ticker / pricing | Ticker says options flow unlocks with Pro; pricing calls Options Flow Feed and Filters “Coming soon” | Confirmed inconsistency in the paid-feature promise |

### Reporting defects confirmed in code

`backend/app/routers/accounts.py:6428` and following: page analytics groups both ordinary page records and `/events/...` records into one report. Visitor identity is the first available account ID, session hash, or individual event row ID. Anonymous sessions are not distinct people; missing identity can count individual records as distinct visitors. No admin-user exclusion appears in this aggregation.

`frontend/components/admin/PageAnalyticsReport.tsx`: fetches only the top 30 rows, totals those rows under “Views,” and labels the returned row count “Tracked pages.” This is neither a complete site page-view total nor a count of all tracked pages, and it includes product-event rows.

The much larger ticker page visitor count (369) than ticker-viewed-event visitor count (5) needs reconciliation. It is not sufficient evidence to call the difference bots; identity, coverage, deployment history, and event delivery must be checked.

Signup and subscription key events were already configured in GA4 on September 11 according to `docs/analytics-conversion-followup.md`. Do not propose simply creating the same events again. That record also documents missing `GA4_API_SECRET` for paid-event forwarding. Current Fly secret presence could not be verified because this CLI has no authenticated token; the old absence is a follow-up item, not a freshly confirmed configuration defect. A new Measurement Protocol secret previously required an owner privacy acknowledgement. No acknowledgement was accepted during this investigation.

## Search diagnosis

Live Search Console indexing report, updated September 17: 105 indexed; 3,843 discovered but not indexed; 18 noindex, 4 redirected, 4 crawled but not indexed, 12 redirect errors, 6 robots-blocked, 1 duplicate. These categories are not interchangeable. The report predates the September 20 leaderboard fix, and historical URL exclusions must be checked individually rather than removed indiscriminately.

App-host Crawl Stats, updated September 19: about 1,563 requests, 247 ms average response, 99% HTTP 200, no host problems in the last 90 days, rounded 100% refresh / less than 1% discovery. About 76% of requests are JavaScript resources. That resource mix alone does not prove a rendering problem and is not a reason to block JavaScript.

Fresh bounded public check at `artifacts/growth-diagnosis-2026-09-21/public-check.json`:

- All 10 sitemap XML documents returned HTTP 200.
- 4,638 unique page URLs across those documents, up from 4,392 in the September 18 audit.
- All 13 sampled pages returned 200, one self-canonical, no noindex header/meta, and no robots block. Samples include homepage, FAQ, leaderboard, NVDA, Insights, two departments, two members, two research pages and two comparisons. This is a sample, not certification of all 4,638 pages.
- Inventory includes 2,384 insiders, 1,052 tickers, 868 institutions, 237 members, 38 departments, 30 research pages, 7 comparisons, and other public pages. Duplicate inclusion across sitemap files is deduplicated in the total.

Google's Links report currently recognizes 108 external links: 96 to the homepage, 10 to one research article, 2 to Insights. Linking domains shown are Reddit (62), walnut-intel.com (42), and t.co (4). This report is a limited Google sample, not a complete backlink inventory. It supports a narrow observed distribution footprint, not a claim that Walnut has no links. 96/108 links point at the homepage, while the large entity inventory receives little visible external support in this report.

The strongest interpretation is weak discovery/crawl demand relative to the growing inventory. It is not proof of a penalty, a hostname branding problem, or page-quality rejection of URLs Google has not fetched. Google explicitly distinguishes crawl capacity from demand and does not guarantee indexing after crawling: https://developers.google.com/crawling/docs/crawl-budget . Definitions: https://support.google.com/webmasters/answer/7440203?hl=en .

There is no defensible deadline for indexing every page. Resubmitting unchanged successful sitemaps repeatedly does not solve this. Prior records show the marketing sitemap was resubmitted September 18 and specific indexing/validation requests were already made.

## Prioritized repair plan

1. **Make free email signup short.** Ask for email/password, optionally a display name; keep Google sign-in. Defer billing address collection to the paid checkout flow, retaining applicable payment/tax checks there. Preserve email verification, rate limits, authentication security, plan entitlements, and the requested stock/follow action. Validate both email and Google journeys in a controlled test environment and reconcile completion with account creation records. Do not hide fields only in the frontend while leaving the API mandatory.
2. **Make the conversion report trustworthy.** Separate page views from product events; report sessions, signed-in accounts and actual new accounts distinctly; label limited/top-N reports honestly. Separate owner/admin/test activity in analysis without collecting unnecessary personal information. Reconcile signup completions against non-test account creations, then checkout starts and positive paid subscription invoices. Treat $0 grants, test payments, refunds and cancellations separately. Check the paid-forwarding secret with an authenticated operator; do not assume the prior setup gap is still present.
3. **Repair the first product experience and paid promise.** Keep the existing Follow → signup → return path. Make the question, useful permitted evidence, and one next action clear. Resolve “unlocks with Pro” versus “Coming soon” options-flow language. Do not change data entitlements as a shortcut. A reviewed illustrative demo can explain gated value without pretending an unavailable feature exists.
4. **Run one measurable Reddit-to-product workflow.** Use one stock-specific post that gives a useful finding and links to the matching ticker/research page at the point readers would want the details. Invite readers to follow that stock for changes. Use campaign and per-post UTM tags, consistent with subreddit rules; do not paste the same promotion everywhere. Count attributed human visits, stock actions, verified signups, seven-day returns and paid starts. Do not compare Reddit feed views to website account counts as if they share a denominator.
5. **Concentrate SEO effort on 10–20 commercially useful pages.** Start with tickers/topics already attracting interest and the linked AI-memory article; retain accurate filing periods and current dated summaries. Ensure ordinary crawlable links connect research, relevant tickers, and useful category pages. Audit existing pagination/hubs before duplicating work already deployed September 18. Improve distinct evidence and usefulness on priority pages before accelerating entity-page production. Remove empty/duplicate URLs from sitemaps only after checking their actual content; avoid blanket deletion/noindex or blocking required assets. Track crawl/index status for this cohort and relevant non-brand impressions weekly.
6. **Validate willingness to pay with actual users.** After removing signup friction, observe a small set of non-admin users doing their own stock check. Identify which additional information or ongoing monitoring they would pay to retain. Ask why they stopped, rather than assume either price or headline is the answer. No outreach was sent in this audit.

The order matters: improve the path people already use, make its results measurable, then send focused traffic through it. A site-wide redesign or another broad feature expansion would make it harder to know what changed the outcome.

## Proposed positioning to test after the repairs

**Before you buy a stock, see where the data agrees—and where it doesn't.**

Supporting copy: “Bring company financials, price trends, insider trades and investor activity into one stock view. Compare the signals, then follow what changes.”

Primary action: **Check a stock** with a ticker search. Secondary: a real worked example. Scope paid feature claims to their actual plans; do not expose the numeric Confirmation Score contrary to entitlements.

This expresses a concrete decision problem without the jargon “investment thesis,” a promised winning trade, or manufactured urgency. It is a proposed message, not a proven keyword or conversion winner. No reliable absolute keyword-volume comparison for “investment thesis” versus these phrases was established in this audit; Search Console only measures queries where Walnut appeared.

The goal for the first measurement cycle is a trustworthy, attributable progression from interested visitor → useful stock action → free account → return visit → paid decision. Review cohort counts and reasons for drop-off after 2–4 weeks; that is a learning window, not a promised SEO or revenue turnaround.
