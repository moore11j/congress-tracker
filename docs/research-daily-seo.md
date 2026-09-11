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
- Google Search Console, a licensed keyword volume provider, conversion attribution,
  backlink acquisition, and automatic refresh of underperforming articles are not
  part of this first version. Those would close the measured SEO feedback loop.

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
