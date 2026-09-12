# Daily SEO research

Daily SEO turns audience interest into one reviewable research article per day.
It is opt-in under Research Briefs → Daily SEO. Existing campaigns are unchanged.

## Editorial workflow

1. Save topics, optional focus tickers, a generation time, and an IANA timezone.
2. Enable daily drafts, or request today's draft once. The five-minute cron worker
   starts generation after the configured local time, including daylight saving.
3. Aggregate the last 30 days of exact known-ticker searches with at least three
   search events. No raw queries, personal searches, or user identifiers leave the DB.
4. Discover at most five web-grounded keyword opportunities using the configured
   research discovery model. No model upgrades or image-generation calls are added.
5. Rank by editorial opportunity score plus a capped on-site interest bonus.
   Exclude exact/near-identical queries from recent campaign items (90 days), recent
   drafts, and all published database drafts. This is heuristic deduplication, not
   semantic clustering. Static articles are not yet included in the exclusion set.
6. Generate one ticker research article using the existing Walnut data/research
   pipeline. Keep it in scheduled review. Propose publication 24 hours after planning;
   an editor must approve it and can change the time. Generation is not publication.
7. Use the existing email notification to open the saved preview, edit, request AI
   changes, approve, or discard. Run history links to the draft and shows the email
   provider's status. "Sent" does not prove inbox delivery.

## Guardrails and scope

- One durable discovery attempt per local date, shared by manual and automatic runs.
  Failed or interrupted attempts never silently restart paid discovery. Review the
  existing campaign/draft before manually retrying its generation. There is no
  reset-daily-budget button.
- Existing generation correction limits and OpenAI audit logging remain in force.
  A daily attempt limit is NOT a dollar-spend guarantee; provider billing still applies.
- Seven outstanding Daily SEO drafts/pending articles pause new discovery.
- No qualifying, sourced candidate means no article that day.
- Non-ticker candidates are excluded because the old non-ticker campaign path still
  creates an editorial placeholder. Broad thematic automation needs a real research
  generator before enabling it here.
- Search events are not unique users or Google demand. Scores are editorial estimates,
  not keyword volume, keyword difficulty, ranking promises, or predicted revenue.
- Google Search Console provides measured query demand and review suggestions as
  described below. Keyword volumes, conversion attribution, backlink acquisition,
  and automatic rewrites remain outside this version.

## Deployment and operation

Deploy backend and frontend from the same revision. Backend crontab invokes
`python -m app.jobs.run_research_seo` every five minutes; this worker has its own
database opt-in setting and does not alter the old campaign scheduler environment gate.
The two small `research_seo_*` tables initialize idempotently. Enable in the UI only
after confirming the cron process is deployed and email delivery is configured.
Defaults: disabled, 07:00 America/Los_Angeles, one draft, existing review recipient
(`RESEARCH_BRIEF_REVIEW_EMAIL`, default jarod@walnutmarkets.com).

The worker records queued → planning → generating → draft_ready (or skipped/failed/
needs_attention). A crash can leave a claim in progress; inspect its linked campaign
and OpenAI request audit before any recovery. Do not delete a daily claim to retry
blindly. Settings disable future automatic runs; explicitly queued manual runs still run.

Run backend tests with `python -m pytest tests/test_research_seo.py` and frontend
tests with `node --test tests/daily-research-seo.test.mjs tests/admin-research-briefs.test.mjs`.
All automated tests mock paid discovery and generation. Production activation and
end-to-end email delivery require a separate controlled live check.

## Verification (September 11, 2026)

- New scheduler plus targeted existing campaign/approval tests: 19 passed.
- Daily SEO and existing admin UI tests: 6 passed.
- Frontend TypeScript `--noEmit --incremental false`: passed.
- Existing backend research suite: 107 passed, 8 failed. A clean checkout of base
  `0eae1692` reproduces the same eight failures (schema-test mock, stale correction
  prompt assertions, campaign ordering, paywall schema, external research fixture,
  and financial-value expectations).
- Existing public research frontend tests have two failures, also reproduced on
  the clean base (archive metadata assertion and old ticker lookup assertion).
- No production setting changed, no paid generation run, no live email sent.
- Local tests used an isolated Python 3.14 environment and mocked provider calls;
  production runtime/email smoke verification remains outstanding.
# Search Console performance connection

Daily SEO now has an admin-only Google Search Console connection. It reuses the
existing Google web client and registered `/auth/google/callback` URI; ordinary
Google login still requests only its existing identity scopes. Search Console
asks separately for `openid email` and `webmasters.readonly`, with offline access,
PKCE, and a ten-minute single-use server nonce bound to the current admin.

Enable `searchconsole.googleapis.com` on the existing OAuth project. The connector
requires `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` and a strong `APP_SESSION_SECRET`.
Refresh tokens and temporary PKCE verifiers are Fernet encrypted using a
domain-separated key derived from the server session secret. Rotating that secret
requires reconnecting Google. Tokens and provider error bodies are never returned
to the browser or written to application logs. No new OAuth client is required.

Connect from Daily SEO while logged into Walnut as an administrator, then select
the Google account owning `sc-domain:walnutmarkets.com`. Google grants this scope
across accessible properties, but the backend uses a fixed walnutmarkets.com
property. No Gmail, Drive, Ads, or Search Console write scope is requested.
Apps in Google's Testing state may receive expiring refresh grants; reconnect or
complete Google's production/verification requirements if Google requests it.

The existing five-minute research SEO worker imports once per Pacific calendar
day, even if draft generation is disabled. Manual sync is limited to once per five
minutes. It fetches finalized web query and page rows over a 28-day window ending
three days ago, and page rows for the prior 28 days. Pagination is bounded at
50,000 rows per report. Google returns top rows, not an exhaustive query census.
The last good snapshot survives failures, but failed or >3-day-old snapshots are
excluded from automated topic selection. Disconnected or revoked-admin connections
cannot sync. Disconnect removes the local credential and snapshot; it does not
revoke the shared Google login grant. Users can revoke it in Google account access.

Fresh aggregate queries with at least ten impressions can guide discovery. Exact
query matches receive a bounded ranking bonus after clearing the editorial
threshold. Low CTR, declining clicks and positions 5–20 produce explicit review
suggestions for existing research pages, never automatic rewrites. Daily draft
and approval limits remain unchanged. These are measured property performance
metrics, not keyword search volumes. Keyword Planner has its own connector below.

## Google Keyword Planner

Daily SEO now includes a separate Connect Google Keyword Planner action. It uses
the existing registered Google callback with an admin-bound, single-use `gads_`
state and S256 PKCE. Tokens use a different encryption domain and separate tables
from Search Console. Google requires the broad `adwords` OAuth scope; the UI must
disclose that this includes management permission even though our transport only
allows account identity queries and historical keyword metrics. No ad, budget or
campaign mutation endpoint is implemented. Disconnect removes the local grant and
cache without revoking the shared Google sign-in client.

Cloud project `walnut-intel` must have Google Ads API Basic access and published,
verified branding. No developer token is sent (Google sunset tokens September 9,
2026). The connection validates account 453-375-9595 and actually probes keyword
service access before marking itself connected. OAuth credentials remain in the
existing server environment; never commit or log tokens. No new credentials needed.

Targeting is explicitly US / English / Google Search. `v24` historical metrics are
cached 30 days per normalized phrase; at most 50 phrases per batch, 80 characters
and ten words each. No visitor histories, emails or URLs are submitted. One atomic
connection-level five-minute claim bounds parallel lookups and failed retries.
Disconnection/reconnection prevents stale in-flight requests repopulating the cache.
Provider errors keep the last good cache; only fresh metrics can affect ranking.

After daily discovery and before selection, primary and up to three secondary
keywords per candidate are checked. Primary-keyword demand adds at most ten points
only after the editorial threshold is met. Secondary volumes are not summed into
ranking because close variants can overlap. The run history retains provenance,
Google grouping, targeting and monthly history. Missing metrics are null, not zero.
These are Google estimates, not exact counts, organic difficulty or guaranteed
traffic. Existing one-draft-per-day and explicit review/approval rules are unchanged.

References: https://developers.google.com/google-ads/api/docs/keyword-planning/generate-historical-metrics
and https://developers.google.com/google-ads/api/docs/api-policy/developer-token
