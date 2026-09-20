# Walnut Markets acquisition and conversion audit

Inspected September 18, 2026 Pacific (September 19 UTC), using the signed-in Chrome Google Analytics property **Walnut Markets**, property 545031828, stream G-QQTFFK7FBH. This is a read-only audit and recommendation document. No analytics settings, website code, pricing, entitlements, advertisements, or messages were changed.

## Diagnosis

Walnut has insufficient demonstrated external acquisition and activation to diagnose price as the primary cause of zero paying customers. The recorded engagement is heavily concentrated in a few browser identities and includes substantial administrator use. GA4 cannot currently identify a converting page or channel: it received no completed-signup or completed-subscription events in the examined 28-day window.

The strongest concrete product-friction finding is that the current email-registration implementation demands a full address before granting a free account. The strongest measurement finding is that one returning browser identity generated 54.38% of the recent week's events. Neither issue is solved by producing more videos or repeatedly rewriting the homepage.

## Windows and interpretation

- Broad window: **August 21–September 17, 2026**, as displayed in GA4.
- Recent window: **September 11–17, 2026**, after the September 11 measurement corrections documented in `analytics-conversion-followup.md`. This includes the correction day, so it is a recent baseline, not a perfectly clean experiment.
- Today's incomplete data was not used. Google may still revise recently processed data. Most reports indicated 100% of available data; the source/medium report later indicated mostly complete data.
- GA users are browser/analytics identities, not verified Walnut accounts. **66 new users does not mean 66 registrations.** Users across rows can overlap. Counts of different events are not an ordered funnel.
- Earlier pageviews may contain automatic-history duplication. There are both legacy and canonical custom events, such as `homepage_view` and `homepage_viewed`; do not add them as distinct user actions.
- Page reports initially combine both hosts. A hostname breakdown confirmed 85 of the recent 93 root-path views belong to the marketing homepage and eight to the app root.

## Overall evidence

| Metric | Aug 21–Sep 17 | Sep 11–17 |
|---|---:|---:|
| Sessions | 527 | 130 |
| Active users | 318 | 70 |
| Total users (Events/User acquisition) | 319 | 71 |
| Pageviews | 3,149 | 525 |
| Engaged sessions | 283 | 68 |
| Engagement rate | 53.70% | 52.31% |
| Derived bounce rate | 46.30% | 47.69% |
| Average engagement per session | 3m 07s | 6m 05s |
| Key events | 0 | 0 |

Recent User acquisition shows **66 new users and five returning users**. This is not a seven-day retention rate: that requires a cohort of actual new accounts followed through a defined return window.

### Internal traffic and concentration

Recent administrator pageviews: `/admin/settings` 41, `/admin/research-briefs` 40, `/admin/ai-marketing` 28. Total **109/525 = 20.8%**. These pages were visited by one or two users each; users overlap. Their engagement times are measured in tens of minutes to hours.

First-user source `accounts.google.com / referral` contains **one returning user, 1,036 events (54.38% of all events), and 12h 44m average engagement** during the recent week. This does not establish the identity of that user, but it establishes that aggregate usage is dominated by a single browser identity. Owner/test use is a strong hypothesis given the administrator reports, not a proven user-level join.

The GA4 `My IP` internal-traffic exclusion is Active. The broader `Internal Traffic` exclusion is Testing. Testing does not remove that data from ordinary reports. Current frontend tracking checks production host and consent but does not suppress admin routes or explicitly exclude admin users. Merely excluding `/admin/` pages would still retain an owner's visits to ticker, pricing, and research pages.

Current automated navigation/product/research capture code sets analytics consent off and restricts external requests. Do not assume all video worker recordings cause these GA events; manual production QA and owner browsing are separate concerns.

## Acquisition: which sources deserve attention?

Recent Traffic acquisition:

| Channel | Sessions | Engagement | Average engagement/session | Interpretation |
|---|---:|---:|---:|---|
| Direct | 69 | 36.23% | 8s | Largest group; 63.77% derived bounce. Source unknown, not necessarily deliberate brand visits. |
| Organic Social | 35 | 82.86% | 21m 50s | Apparent winner is heavily suspect because of concentration and inconsistent attribution. |
| Referral | 18 | 72.22% | 52s | 17 sessions attributed to Google Accounts; not a true acquisition partner. |
| Paid Search | 4 | 0% | 14s | Too small to judge an advertising strategy; no observed downstream success. |
| Organic Search | 2 | 50% | 9s | Far too little volume to supply customers consistently. |
| Organic Shopping | 1 | 0% | 4s | Too small to interpret. |
| Unassigned | 1 | 0% | 0s | Too small to interpret. |

Recent session-source details distinguish misleading aggregates:

- `reddit / (not set)`: 24 sessions, 30m 15s average engagement, 1,008 events.
- `reddit.com / referral`: three sessions, **zero engaged**, 2s average engagement.
- Facebook desktop/mobile referral sources: four sessions each, **0s average measured engagement** despite high engagement rates. These counts do not establish human attention; inspect pageview/session measurement and referral quality before spending.
- `google / organic`: one new user in User acquisition; Bing organic: one.
- `billing.stripe.com / referral`: one session; returning from Stripe does not prove a purchase.

The 28-day source report had 33 `reddit.com / referral` sessions with 69.7% engagement but only 19s average engagement. Reddit is a reasonable *test channel*, not an established signup source.

The September 11 audit already confirmed Google Accounts was in unwanted referrals. Historical attribution can persist after an exclusion, as Google documents. Do not re-add it blindly or count OAuth as organic acquisition. Verify fresh acquisition through the marketing-to-app-to-OAuth journey with consistently tagged links. Use `utm_source`, `utm_medium`, `utm_campaign`, and a unique `utm_content` per post; reserve paid mediums for actual paid campaigns.

## Pages capturing attention versus pages producing customers

Recent Pages and screens, with hostname inspected:

| Page | Views | Active users | Average engagement/user | Assessment |
|---|---:|---:|---:|---|
| Marketing homepage | 85 | 43 | 1m 34s | Broadest measurable public reach. Best place to clarify the first action. |
| NVDA ticker | 38 | 5 | 25m 25s | Detailed interest, but too concentrated to distinguish demand from owner/QA usage. |
| Insights | 32 | 4 | 1m 22s | Repeat usage from a very small audience; not broad discovery. |
| Strategies | 64 | 4 | 14s | Many views, little time per user. Do not label it the most popular product solely on views. |
| Leaderboards | 18 | 3 | 6m 51s | Small audience; no proven upgrade conversion. |
| Screener | 8 | 4 | 43s | Small but relevant discovery entry point. |
| Pricing, app host | 7 | 10 | 4s | Weak observed attention. Page-scoped users can be counted on other events; seven canonical pricing events reached seven users. Investigate collection consistency. |
| NVIDIA strategic-investments brief | 7 | 2 | 1m 17s | Very small distribution. |
| NVIDIA Q2 institutional brief | 6 | 3 | 10m 59s | Some depth, no established acquisition or signup success. |
| Stock analysis tools | 5 | 5 | 3s | Short visits and insufficient sample. |
| Stock research software | 5 | 5 | 3s | Short visits and insufficient sample. |

The 28-day **landing-session** report provides broader context: NVDA had 15 entrance sessions / 11 active users / 2m 40s engagement per session; AAPL 12 / 12 / 1m 11s; pricing 13 / 13 / 13s; NBIS-vs-CRWV research seven / seven / 3s. These are flags for reviewing message-to-page fit, not proof of a rendering defect or precise page bounce rates. Do not compare landing-session engagement with page engagement per user as though they were the same metric.

**No page can currently be called a signup or paid-conversion winner.** No completed conversion events were recorded in this period.

## Funnel and measurement status

Recent canonical event users (not an ordered, mutually exclusive funnel):

| Event | Events | Total users |
|---|---:|---:|
| `homepage_viewed` | 78 | 36 |
| `ticker_viewed` | 60 | 7 |
| `screener_opened` | 8 | 4 |
| `leaderboard_viewed` | 17 | 3 |
| `pricing_viewed` | 7 | 7 |
| `upgrade_prompt_viewed` | 7 | 3 |
| `signup_started` | Not present | Not present |
| `signup_completed` | Not present | Not present |
| `checkout_started` | Not present | Not present |
| `subscription_completed` | Not present | Not present |

The 28-day inventory contains only one `signup_started`, one `ticker_follow_complete`, and two upgrade-prompt clicks from one user; no recorded completed signup or checkout start. Generic `form_start` and `form_submit` are not verified account creations.

Live GA Admin confirms `signup_completed` and `subscription_completed` are already key events, both **No stream data detected** in the last 28 days. `purchase` is also present without data. Do not mark generic pageviews as conversions to make the numbers look healthier.

**Important correction to older documentation:** a Measurement Protocol credential named “Walnut backend paid conversions” now exists, created September 11. The prior report saying no GA4 credential existed is stale. This audit did not verify the current Fly secret installation or a real Stripe-to-GA delivery. A credential existing does not prove successful forwarding. No credential values are included here.

Recommended ordered journeys:

1. Research/stock landing → useful product action → signup completion → first followed ticker/watchlist.
2. Activated free account → contextual paid-feature preview → pricing → checkout start → authoritative subscription completion.
3. New account cohort → meaningful return within seven days → paid upgrade within a defined later window.

Reconcile account creation and verified Stripe subscriptions with consent-eligible analytics events. Blockers, consent, and timing can explain missing GA data; GA absence alone does not establish no actual accounts. Aggregate Walnut admin reconciliation was requested separately after automatic approval review blocked that read as outside the specifically authorized Google Analytics scope; it remains pending.

## Mobile and retention

Recent desktop: 51 active users, 50.53% engagement, 15m 10s average engagement/user. Mobile: 19 active users, 57.14%, 56s. Mobile is 27% of active users and does not show a clear overall engagement-rate deficit. Desktop averages contain substantial internal activity. Two Safari-in-app users had zero engagement; too few to diagnose an in-app-browser defect.

Five returning analytics users and no recent follow/watchlist creation events do not demonstrate a recurring public-user habit. Build retention around a saved stock and an actual change worth returning to; count activation and returns rather than email sends or video views. Do not infer exact cohort retention from the aggregate returning-user count.

## SEO evidence

Linked Google organic landing-page report for Aug 21–Sep 17 shows **708 impressions, three clicks, 0.42% CTR, average position 36.06**. All three recorded clicks were to `/`. The named-query report showed 125 impressions and zero attributable query clicks; this smaller query table must not replace the landing-page totals because query privacy/aggregation differ.

Visible terms include `alv` (46 impressions, position 77.96), `walnuts price trend` (six, 92.17), `dod contracts awarded today` (three, 39.33), `nasa contracts` (three, 79), and `neocloud stock` (one, 24). Most terms have one or two impressions. These do not support confident keyword-volume estimates.

Recommendation: build a focused research cluster around questions Walnut can answer distinctively with sourced data: who increased/reduced a named stock position, which listed companies won a department's contracts, and specific company comparisons. Link the brief, ticker evidence, ownership/research tab, and monitoring action together. Avoid scaling generic stock-price pages as the sole SEO strategy. A sitemap makes pages discoverable; it does not create demand or guarantee ranking.

The live homepage has already changed from the pasted HeyCatch audit: its H1 is “Build Your Next Winning Portfolio,” with a Top 3 preview and a worked GOOG research example. Avoid acting on the old audit as though it describes today's page.

## Highest-priority recommendations

### 1. Establish a reliable customer funnel before increasing spend

Separate admin/owner/QA use across the whole production site; test exclusions before activation. Validate one consented email signup and one Google signup in a controlled flow, preserving original campaign and intended destination. Validate checkout and server-confirmed paid receipt safely; do not manufacture production conversions. Keep canonical event names consistent, retain error-stage telemetry without personal form values, and register only useful reporting dimensions.

Acceptance: verified first-party outcomes can be reconciled to eligible GA events; admin sessions do not influence customer KPIs; every campaign has a distinguishable source and creative label. Historical data is not retroactively repaired by new filters.

### 2. Remove unnecessary free-signup friction and return users to their task

Current `frontend/components/auth/LoginRegisterPanel.tsx` requires first name, last name, email, password, country, postal code, city, street address, and region when applicable. Unless a documented requirement demands these before a free account, defer billing-address collection until paid checkout. Keep Google sign-in. Preserve existing return-to/follow intent. When no return path exists, the current email flow goes to account settings; a stock-selection/first-watchlist step is a stronger activation hypothesis.

This is a code-based friction finding, not proof that those fields caused a measured abandonment rate. No current entitlement, billing, or address requirements were changed.

### 3. Give each campaign one answer and one useful next step

For an NVIDIA video, answer the question with dated evidence and take viewers directly to the corresponding brief or ticker research section. Then offer a specific action such as saving NVIDIA to a free watchlist, within current plan limits. Show the outcome before requesting registration. Keep Premium/Pro data boundaries intact and label paid previews clearly.

Test two or three topic-led creatives serially with consistent attribution. Example editorial angles: “Who added NVIDIA—and who cut their position?”, “Which public companies won NASA contracts?”, and an evidence-led NBIS/CRWV comparison. These are research proposals, not claims that today's filings support a particular bullish answer. Judge campaigns by activated free users and later paying users, not views alone.

### 4. Explain the paid benefit and correct offer inconsistencies

The live pricing page leads with capacity limits and a long matrix. Add a short, concrete explanation of the workflow each tier unlocks, with an authentic product example. Keep the detailed matrix available. Avoid changing price based on seven recent pricing-event users.

Correct the homepage's Pro copy mentioning options flow as included while the pricing matrix labels Options Flow Feed/Filters **Coming soon**. Also make any alert/monitoring promise explicit about Free versus paid access. No exaggerated results, fabricated testimonials, or unsupported performance claims.

### 5. Learn directly from the first real users

Invite five actual free users to a short research walkthrough and ask what task they wanted to complete, whether they reached an answer, what remains missing, and what would justify paying. Obtain permission before contacting anyone; this audit sent no messages. A small, observed activation study is more informative now than a multi-variant homepage experiment with insufficient traffic.

## Proposed 30-day sequence

| Timing | Work | Decision metric |
|---|---|---|
| Days 1–3 | Verify funnel, segment internal activity, reconcile real signup/subscription totals; fix the options-flow promise | Trustworthy external-user baseline and complete event receipt |
| Days 4–7 | Simplify free signup where permissible; preserve destination; guide first stock save; conduct five walkthroughs | Completed account → first saved/followed ticker, error/abandonment reasons |
| Weeks 2–3 | Distribute a small set of specific research answers through consistently tagged videos/posts and matching pages | Activated accounts per eligible landing visitor; source and creative attribution |
| Week 4 | Review seven-day returns, paid-feature interest, checkout starts, and actual subscriptions | Decide whether the next constraint is acquisition, activation, value, or checkout |

Use a working target of **100 qualified external landing visitors** to get initial directional feedback on a single journey, not as a guarantee of statistical significance or a customer forecast. Keep prices stable until real activated users encounter and evaluate the paid offer. No large paid acquisition increase is justified by the present conversion evidence.

## Sources and boundaries

- Signed-in GA4 Chrome reports: Traffic acquisition; User acquisition; Events; Landing page; Pages and screens with Hostname; Tech details by browser/device; engagement/retention overview; linked Search Console Queries and Google organic landing pages; Admin Events, Data filters, and stream configuration.
- Live Chrome marketing homepage and authenticated pricing page. Authenticated pricing buttons are not evidence of what a guest sees. No account or checkout was submitted.
- Current tracking/auth/capture source; previous analytics audit documents treated as historical and checked against current UI where possible.
- Google: [engagement/bounce definitions](https://support.google.com/analytics/answer/12195621?hl=en), [unwanted referrals and persistent historical attribution](https://support.google.com/analytics/answer/10327750?hl=en), [filters do not change historical data](https://support.google.com/analytics/answer/13296761?hl=en), [Search Console data discrepancies](https://support.google.com/webmasters/answer/17010575?hl=en).

GA4 bounce is the inverse of engagement rate, not simply any exit. A useful short research visit can still lead to an exit. Internal contamination, small samples, consent, shifting instrumentation, and missing verified conversion receipt limit causal conclusions. Recommendations above are prioritized hypotheses with explicit verification steps, not promised conversion lifts.
