# Analytics conversion follow-up — September 11, 2026

Implements the first fixes from the GA4 audit of August 14–September 10. The user authorized proceeding after the audit. No FAQ content, pricing, billing requirements, or entitlement rules changed.

## GA4 changes saved and verified

- Added `signup_completed` and `subscription_completed` as key events, using **Create with code**, once per event, and **no default monetary value**. These names already exist in the application. No pageview or generic form submission was converted into a successful signup. Existing key events remain.
- Disabled Enhanced measurement → Page views → **Page changes based on browser history events**, then reopened settings to verify it is off. Walnut's existing PageAnalyticsTracker sends manual pageviews and initializes gtag with `send_page_view: false`. Google documents that automatic history events operate independently of that flag and must also be disabled to avoid duplicates: https://developers.google.com/analytics/devguides/collection/ga4/views
- Verified `accounts.google.com` is already in the unwanted-referrals list. No referral-list change was necessary. Historical sign-in referrals are not evidence that the current rule is absent.
- Existing internal filters remain unchanged: `My IP` is Active and `Internal Traffic` is Testing. Current source already limits production analytics to the HTTPS marketing/app hosts and rejects localhost/preview. No speculative IP exclusions were added.

The two new key-event names currently show no received stream data. Configuration does not prove a real signup or payment was received. Start a new measurement baseline after these corrections; historical pageviews and engagement may include duplicate history events and internal/testing traffic.

## Application corrections

1. The guest ticker Follow dialog linked to signup/login without `follow=1`, even though the ticker component already supports resuming that intent after authentication. Both links now preserve the requested follow action and existing route/campaign parameters. Successful continuation uses the existing follow API and removes the flag. Entitlements and limits still apply.
2. First-party acquisition previously classified all Google subdomains as organic, including accounts.google.com, Search Console, and Tag Assistant. Only Google search hostnames are now classified as organic. Google sign-in returns preserve the shared acquisition cookie when present and do not become new organic acquisition when absent.
3. `utm_content` now survives the shared acquisition cookie, product event sanitization, backend storage, and verified paid-event forwarding. Follow events use the shared acquisition context instead of relying on query parameters still being present on the ticker page.

## Validation

- 24 focused frontend tests passed, including real return-path construction, authentication round trips, post-label preservation, Google referrer classification, consent/environment gating, and existing funnel/provider contracts.
- 14 backend analytics tests passed before the added post-label assertion; the final paid-claim test also verifies `utm_content` survives into the GA4 payload.
- TypeScript passed. Production build and deployment results are recorded in the task's final response.
- Live AAPL and NBIS/CRWV article pages were inspected in Chrome. AAPL rendered its heading and research sections; the article rendered its summary, comparison, ticker links, and contextual signup links. Chrome was authenticated as the owner, so this is not a guest end-to-end signup verification. No account or payment was created.
- The three proposed SEO targets already contain differentiated copy, product examples, and related internal links in current source. Preserve those recent improvements and gather clean post-release data before another title/content test. The historical bounce report does not establish a specific rendering defect or justify a broad rewrite.

## Paid measurement prerequisite still blocked

The production Fly secret-name listing confirms `ANALYTICS_FORWARDING_SECRET` exists and `GA4_API_SECRET` is absent. The GA4 stream has no Measurement Protocol API secrets. Creating one is gated by **User Data Collection Acknowledgement**:

> I acknowledge that I have the necessary privacy disclosures and rights from my end users for the collection and processing of their data

The acknowledgement also covers association with Google Analytics visitation information. It has not been accepted on the user's behalf. After explicit confirmation, create a stream-scoped credential and install it privately as `GA4_API_SECRET` on the existing backend. Do not put it in repository files, public environment variables, URLs shown to the user, or logs. Verify the next legitimate consented paid completion against its authoritative first-party event and provider receipt. Do not create a real payment or replay historical events to manufacture a conversion.

## Next measurement and acquisition cycle

Use confirmed signup → first saved/followed ticker as the activation journey, and checkout → server-confirmed subscription for paid conversion. Analyze eligible external visitors separately from owner/test traffic, preserve original campaign through OAuth, and distinguish Google/email methods. A missing GA4 event cannot establish zero actual accounts; reconcile consented events with backend outcomes.

Use consistent Reddit campaign tags before comparing posts:

`https://walnutmarkets.com/research/nbis-vs-crwv-ai-neoclouds?utm_source=reddit&utm_medium=organic_social&utm_campaign=neocloud_research&utm_content=nbis_crwv_post_1`

Use a new `utm_content` for each distinct post and `paid_social` only for paid campaigns. No Reddit posts were published and no advertising spend was changed. Evaluate completed signups and subsequent activation; do not claim a conversion uplift from the old, small sample.
