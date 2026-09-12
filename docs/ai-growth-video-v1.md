# Walnut AI Growth video V1

## Audit and implementation plan

The existing AI Growth application has `AiMarketingOpportunity` draft records,
`AiMarketingSuggestion`, campaigns/runs, a React admin queue, admin authentication,
mutation rate limiting, an OpenAI credential resolver and request audit wrapper.
Research briefs retain their original structured `research_context` in PostgreSQL
JSON payloads, with published/approved-scheduled states. The separate Research
Evidence engine already normalizes market records; it must not be duplicated or
treated as independently verified merely because an LLM extracted a sentence.
Daily SEO uses Search Console snapshots, Keyword Planner and aggregated ticker
interest. Existing cron jobs run bounded modules with database claims.

Existing social-card assets are inline image data URLs. No shared object-storage,
Creatomate or ElevenLabs adapter was found. Fly volumes are machine-local, so they
cannot safely serve assets produced on the cron machine to every API replica.

### Additions

* Versioned Growth Brief using existing settings for the current pointer/config.
* Format-independent content opportunities and immutable evidence snapshots.
* Video jobs linked to existing AI Growth draft records; durable stages, quotas,
  leases, revisions and provider render IDs.
* Growth decision memory and private object-store asset metadata.
* Separate capture, narration, renderer and orchestration modules.
* Opportunities, video review, Growth Brief, Memory and video settings within the
  existing AI Growth view. Existing X/Reddit workflows remain intact.

### Exact implementation phases

1. Persist approved-research opportunities, transparent scores, source snapshots,
   brief versions and relevant review feedback.
2. Ask Astra for a strict storyboard. Validate routes, statement provenance and
   timing. V1 uses verified statement IDs plus a curated educational copy library;
   arbitrary financial prose is rejected, including unsupported qualitative claims.
3. Capture actual public Walnut research UI with allowlisted Playwright operations,
   synthesize per-scene ElevenLabs audio, and render branded vertical video through
   Creatomate. Store binary assets in private S3-compatible storage.
4. Integrate review, edits, regeneration and approved downloads. Exercise failure,
   authorization, concurrency and external-provider contracts with meaningful tests.

### Required server configuration

Existing `OPENAI_API_KEY` is reused. Additional keys are server environment only:
`CREATOMATE_API_KEY`, `ELEVENLABS_API_KEY`, `GROWTH_ASSET_BUCKET`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `GROWTH_S3_ENDPOINT_URL`,
and `AWS_DEFAULT_REGION` (defaults to us-east-1). IAM roles may replace AWS keys.
Configure an ElevenLabs voice in Video Settings. Creatomate supports built-in
RenderScript templates without remote template IDs; optional remote templates
must be 1080×1920 MP4 compositions with an automatic root duration and a full-frame
composition named `Content` at time zero for deterministic `Content.elements`
scene injection. Keep the background dark and remove other text/data layers.

The browser worker needs the separate video-worker requirements and Chromium.
Only a dedicated non-admin capture account may be used for future private routes;
this slice uses published, public research pages and requires no login credentials.
No provider credentials are sent to the frontend or captured in video metadata.

### Risks / scope

The first source adapter deliberately supports dated quote metrics retained in
approved research snapshots, with measured search signals used for ranking.
Additional anomaly adapters and verified financial claim templates can be added
without changing the domain model. Existing research prose is not automatically
promoted into a verified financial claim. Capture fails on missing/unpublished UI.
Growth settings and feedback influence selection; they cannot override factual
validation. This release does not implement social publishing or performance APIs.

Search scoring is transparent: `min(100, 15*ln(1+GSC impressions) + keyword bonus)`.
The Keyword Planner bonus is `min(20, 5*log10(1+monthly volume))` and applies only
to an exact normalized keyword with a cached metric younger than 30 days. Missing
metrics receive no bonus and remain unknown. GSC property impressions and Keyword
Planner search estimates remain separate, dated signals. Evidence scores are 40
points per supported retained fact (capped at 100); freshness loses four points per
day; novelty loses 35 per previous same-ticker opportunity. Audience/visual/CTA fit
are initial editorial constants (80/90/75), explicitly not measured conversion data.

Provider calls cannot be made exactly-once across a network failure. A render ID
is saved before polling; uncertain paid submissions fail visibly and are never
automatically resubmitted. Manual retries require an explicit new action. Assets
use expiring signed URLs; approved files remain in Walnut's private bucket rather
than relying on provider retention. Configure a bucket lifecycle for abandoned
multipart uploads, but do not expire approved content automatically.

## Deployment and verification

Build the backend with `INSTALL_GROWTH_VIDEO_WORKER=true` (Docker build argument)
to install the video requirements and Chromium. Set `GROWTH_VIDEO_WORKER_ENABLED=true`
only on a deployment that has that runtime and the provider/storage configuration.
The existing cron process invokes the bounded worker every two minutes. One pass
advances at most two jobs by one stage. The database lease prevents overlap across
cron instances; interrupted leases surface for manual reconciliation after 20 minutes.
The API needs boto3 to issue short-lived private asset URLs. Existing admin auth,
same-origin frontend API proxying and mutation rate limiting are retained.

Configure a private S3-compatible bucket accessible to all API/cron replicas. Permit
the service identity to PutObject/GetObject only under `ai-growth/`. Do not enable
public bucket access. Creatomate receives signed asset URLs valid for two hours;
admin previews and downloads receive 15-minute URLs. Use HTTPS on any custom S3 endpoint.

Open AI Growth → Content Opportunities → Discover → Generate storyboard. In Draft
Queue inspect the storyboard and evidence, then choose Capture and render. Review
the finished video before approving. Download is gated on approval and rechecks
source provenance. Rejection and editing do not overwrite existing MP4 revisions.

Existing generated research pages gain non-visual capture attributes. During a
rolling deployment the worker can use the existing `main h1` and `main h1 + p`
semantic targets. The approved research title must match exactly; ambiguous or
missing targets fail instead of substituting unrelated footage.

## Validation record — September 12, 2026

* 151 affected backend tests passed: video pipeline, existing AI Growth,
  scheduler, settings and daily research SEO. This includes the 35 new focused
  video tests. The local test runtime is Python 3.14; production remains Python
  3.12. A locally missing Pillow dependency was installed for the existing image
  and email tests; production's pinned image dependency was not changed.
* Opt-in Chromium browser test passed against the real React admin application
  and authenticated test API: opportunity inspection, evidence, Growth Brief save,
  approve, reject and Growth Memory. Paid provider responses and test market values
  are fixtures, not a live rendered-video demonstration.
* The actual public NBIS/CRWV research page returned 200 and was captured as PNG
  and a five-second WebM recording on the first attempt. Matching title and
  allowlisted semantic selector were verified. The optional-cookie banner is
  rejected in the worker's fresh, unauthenticated browser context.
* Frontend production build and TypeScript check passed.
* Full frontend suite: 527 passed, 46 failed. All 46 failure names also reproduce
  against the unchanged HEAD snapshot. Two additional tests in that isolated
  snapshot could not load their outside-frontend fixtures.
* Full backend run: 2,011 passed, 112 failed, one collection error. The unchanged
  `tests/test_institutional_ingest_job.py:149` contains an indentation/syntax error.
  The two AI Growth failures reproduced on baseline and were resolved locally by
  installing missing Pillow; the 151-test affected regression run is clean. Other
  full-suite failures were not changed or all individually classified by this task.

### Provider setup follow-up — September 12, 2026

With explicit user approval, created the `Walnut AI Growth` ElevenLabs key with
Text to Speech access only, a 10,000-credit limit per refresh period, and automatic
disable-if-leaked enabled. Saved that key and the existing Creatomate project key
as `ELEVENLABS_API_KEY` and `CREATOMATE_API_KEY` in `congress-tracker-api` Fly secrets.
Both were verified as staged, then applied by the approved storage provisioning
rollout. Secret values were not printed or written to repository files.

The existing private Tigris bucket is attached to `congress-tracker-pg-production`;
Separate private video storage `walnut-ai-growth-assets` was provisioned with explicit
user approval, and Fly installed its server credentials. The original bucket is unchanged.
ElevenLabs now shows the Starter plan following the user's upgrade. Fly configuration
retains the Chromium build argument and video-worker/bucket settings for future releases.
The backend and frontend were deployed. The production cron machine successfully
launched Chromium 140 and accessed the private bucket. ElevenLabs voice
`JBFqnCBsd6RMkjVDRZzb` is configured in Video Settings.

The first live job `gv_fd6a1a70a11e4f00a3f343babe988260` uses the published NVIDIA
Q2-filings brief. Astra generated a validated five-scene, 35-second storyboard,
followed by three real page captures and five ElevenLabs narration clips. The
source adapter currently supports the dated quote fact ($219.05 on September 11),
not a verified claim about institutional buying. Search signals were absent for
this source; its score must not be presented as measured search momentum.

An initial Astra response exhausted its output budget. Prompt version
`walnut-video-v2` requests compact JSON with low reasoning effort and saves response
status, incomplete details and usage before parsing. A controlled retry succeeded
with 289 output tokens. The focused suite passed after this fix.

Creatomate render `ba2d7c4e-599d-4e99-8223-2d204b47bcd5` succeeded in 4.68 seconds
and used 1.36 trial credits. Its free trial limits output to 270×480 despite the
requested 1080×1920. The user chose to keep the trial preview and declined a paid
Creatomate upgrade. No second render was submitted. The adapter now accepts the
v2 render-object response as well as a legacy singleton array; ambiguous responses
still stop for reconciliation. Lower-resolution results are retained as private
previews while the final-video approval and download gates remain closed.

Full-resolution acceptance, detailed caption/audio synchronization review and
approved download remain pending. The trial preview is a pipeline demonstration,
not a publishing-ready or conversion-validated creative. No social content was
published. Future work should improve source-backed storytelling and mobile
legibility before scaling video production.

Provider contracts checked against:

* https://developers.openai.com/api/docs/models/gpt-6-astra
* https://creatomate.com/docs/api/quick-start/create-a-video-by-render-script
* https://creatomate.com/docs/api/quick-start/inject-render-script-into-a-template
* https://creatomate.com/docs/api/render-script/video-element
* https://elevenlabs.io/docs/api-reference/text-to-speech/convert-with-timestamps
