# Daily research videos and Buffer publishing

The existing two-minute video worker now runs daily discovery, the native video
pipeline, review notifications and a publishing outbox. Enable daily creation
under **AI Growth → Video settings**. The enabling administrator owns captures.

## What happens automatically

- At most one newly published eligible brief per Pacific day, with a unique
  source-brief claim. Enabling does not backfill old research. Briefs must have a
  primary ticker and a concise takeaway that is actually rendered on the site.
- Actual admin navigation: search the ticker, open Insights, scroll to Research
  Briefs, open the article and reveal the quoted passage. The short-lived capture
  session is checked against the administrator before recording.
- One continuous ElevenLabs founder voice, word-aligned captions, Walnut fonts,
  logo and colors, and 1080×1920 H.264/AAC at 24 fps. NVIDIA uses the dim server
  room; other tickers use a restrained abstract Walnut backdrop.
- A review email links to the finished draft. Rendering failures also notify the
  configured AI Growth recipient. Disabled/missing email configuration is not
  reported as successful delivery. Existing render/creative budgets apply.

Daily scripts quote approved published research; they do not invent new financial
claims or independently verify the article's conclusions. Editorial content and
the original research snapshot are fingerprinted and checked again before render
and publishing. The previously identified NVIDIA brief
`rb_1789151207553_89bc04` is excluded from factual automation until its conflicting
institutional aggregates are reconciled and that exclusion is deliberately
reviewed. The older navigation-only campaign is preserved.

## What approval does

The administrator loads and watches the preview, reviews/edits the caption,
selects Instagram/TikTok, confirms Buffer channel settings, then chooses
**Approve and publish now**. Approval records the asset hash, caption hash, actor
and destinations. It creates one durable outbox row per job/destination. The old
**Approve for download only** action does not publish.

The worker submits to Buffer with `mode: shareNow`, automatic publishing and AI
disclosure. Instagram is a Reel shared to the feed. Final status and public post
links appear in Walnut. Buffer's TikTok API currently exposes AI disclosure but
does not expose custom privacy, interaction or commercial-content fields; review
those settings in Buffer before approval. Do not claim that Walnut overrides them.

A timeout or interrupted submit becomes **UNCERTAIN** and is never replayed
automatically. An admin can link an existing Buffer post (channel and caption
must match), or explicitly confirm the queue/sent lists contain no post before
retrying. Existing confirmed Buffer posts are managed in Buffer. Publishing
freezes the approved asset so a later draft edit cannot change the delivered file.

Only approved final MP4s are exposed through unguessable delivery URLs. The
private Tigris bucket, captures, voice files and thumbnails stay private. Delivery
supports HEAD and byte ranges, and completed delivery URLs expire after seven
days. A stable URL avoids an expired signed S3 URL at publish time.

## Setup and limits

`BUFFER_API_KEY` is a Fly secret. The owner approved a key expiring September 12,
2027, with only `account:read`, `posts:read`, and `posts:write`. Rotate the key in
Buffer and replace the Fly secret before expiration; never commit it to the repo.

Configured Walnut channel IDs:

- Instagram: `6aa61e05ea19ca0bde31a165`
- TikTok: `6aa61e4cea19ca0bde31a20f`

Buffer Free allows three channels and ten queued posts per channel. The worker
uses immediate publication after approval. Walnut caps API requests below the
free quotas: 2,800/month, 200/day and 80/15 minutes. Polling is no more frequent
than every 30 minutes per submitted post. Provider usage outside Walnut still
counts against Buffer's account limits.

The optional first-comment draft is manual on Buffer Free. Daily captions contain
the research URL and the video does not promise an automatically posted comment.
No paid Buffer features, licensed music or paid Creatomate rendering are required.
Existing ElevenLabs, storage and Fly usage still apply.

The owner configured five renders per UTC day. A draft that reaches the cap waits
until the next UTC day before capture or narration starts. Increasing this cap
does not increase the daily discovery cadence of one published brief. Local
Buffer request-budget exhaustion also leaves unsent publications queued.

Authenticated capture preflight and approved media delivery use the existing
production backend, `https://congress-tracker-api.fly.dev`. The capture preflight
does not follow redirects. The `api.walnutmarkets.com` hostname is not configured
as this backend and must not be used for either purpose.

## Validation

Focused tests cover day/source deduplication, source changes, unsafe/unrendered
excerpts, review email state, HD requirements, authenticated approval, media
ranges/revocation, duplicate approvals, uncertain submissions, API budgets and
existing native video templates. Publishing tests use a fake provider; a real
social post must only be sent after the owner approves that finished video.

API contracts checked against Buffer's official documentation:

- https://developers.buffer.com/reference.html
- https://developers.buffer.com/examples/create-video-post.html
- https://developers.buffer.com/guides/hosting-media.html
