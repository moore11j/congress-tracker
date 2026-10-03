# Social video layout and content mix

Owner feedback, September 26, 2026: native TikTok/Instagram controls obscure
the masthead and lower captions; repeated account branding is distracting.
Keep the content and voice, recompose the scene instead of scaling the full
video, and interleave useful app tutorials with search-led research videos.

## Composition

- `social_v1` was the September 26 layout (template version 5). The current
  default is `motion_v1` (template version 6); see the September 30 update.
- Full-bleed atmosphere remains 1080×1920. Essential content occupies
  x48–912, y270–1390. These conservative bounds derive from the supplied mobile
  screenshots; native app chrome can vary by device and viewing mode.
- Real app footage occupies a focused panel above a caption band at y1060.
  Preserve table columns; follow the recorded vertical action or evidence crop.
- Omit the repeated top masthead. Use the original Walnut logo on the closing
  card. Center all closing-card text beneath the logo on the shared safe-area
  axis (x480), including the title, tagline, URL and small print. Research and
  tutorial scene headings remain left aligned. Keep the existing system
  typography, mint accent and narration voice.
- Retain research/paid-plan context inside the safe area. Keep the native
  account header, right action rail and bottom description area clear.
- Never alter financial values or redraw product screens to fit the layout.

## Content rotation

Production `GROWTH_VIDEO_AUTOMATION` has `tutorial_every: 4`, effective
`2026-09-26T17:04:00.273097+00:00`. Three new research-brief videos are followed
by a feature tutorial. Tutorials alternate between:

1. Search a company → Research → read its dated brief and risks.
2. Search a company → Ownership → check reported holders and reporting period.

Each tutorial teaches one task and why it helps. Historical holdings must not
be presented as live purchases or investment recommendations. Each remains
bound to published research, uses actual app navigation and enters the usual
review queue. Enabling this mix does not approve or publish any new video.
Successful event acknowledgements determine the cadence; failed/retried events
do not advance it. Existing jobs and approved publishing records are immutable.

## October 2 feature tutorial expansion

The 3 research / 1 tutorial cadence now rotates through six lessons, in order:
research, ownership, financials, Congress activity, insider activity, and analyst
expectations. The latter four teach a different product task rather than opening
another brief. Each uses a problem-led hook, actual clicks in Walnut, what to
check on the screen, and a short research-only CTA. They use the same motion
layout, safe areas, logo, and configured narration as the existing videos.

- Financials: compare Revenue Trend and Earnings Trend with reporting periods.
- Congress: select the Activity View filter, inspect the displayed date and
  reported value range, then click Buys and Sells to separate direction.
- Insiders: select the Activity View filter and inspect the person, role, and
  filed date, then click Buys and Sells. Filing dates can differ from trade dates.
- Analysts: compare rating distribution and the target range, with coverage and
  freshness context. Targets are expectations, not promised returns.

No new financial claims are generated. Captures preserve actual values and wait
for lazy-loaded activity records (including a valid zero-event state); unavailable
data cannot pass as a loaded demo. Existing tutorial scripts, approved media,
scheduled posts, publishing permissions, and generation budgets are unchanged.
New videos still enter the review queue before social scheduling.

Deployed as `registry.fly.io/congress-tracker-api:feature-tutorials-20261002-v3`,
an overlay of production `d05dc6d3f58af745baf3b18391ea201382fe6542` containing
only the tutorial catalog, version-aware validation, and navigation capture
changes. All four Fly machines passed rollout checks. The 58 focused workflow
tests passed, including duplicate directory/filter link labels and preservation
of existing script versions. Activity lesson version 2 matches the logged-in
tables' actual date columns and demonstrates the direction filters.

Initial MSFT product demonstration jobs (new drafts, not publication approvals):

- Financials: `gv_9879cf0a363e43d18e61ba0cd5bc9e8a`.
- Congress: `gv_76af723adffc400b8b1879a4a8d0cc86`.
- Insiders: `gv_676c7f9f546a4e6889d84c0c4ee42db9`.
- Analysts: `gv_84db3b9c60734d96b8224cc23841e638`.

The first Congress draft and failed insider capture are retained as rejected
versions. Corrected review revisions are:

- Congress version 2: `gv_b03bd692265f4e5d80f6d3d3b0981ee1`.
- Insiders version 2: `gv_0f61514698374cba930c6e3aaf07b333`.

The five-render daily cap is unchanged; excess work waits for the next UTC day.

## September 26 delivery

Deployed image: `registry.fly.io/congress-tracker-api:social-layout-20260926-v2`.
The video worker subsequently received `social-layout-centered-20260926` for
the owner's closing-card alignment correction. The same three unapproved review
jobs were recomposed using their retained narration and captures; their earlier
render assets remain recorded in `previous_renders`. Seven layout/workflow
checks passed for this correction.
Built from production `dc9c8f74f0883051af5fd79fa534125ec46a6f9f` plus only the
six video service files changed for this task; healthy app/cron/video machines.
Validation: 133 relevant tests plus an additional publish-event integration
test; complete Microsoft preview audio/video decode passed. Tutorial revisions
retain their selected tutorial format (34 affected tests passed after that safeguard).

All three production renders completed successfully and are ready for review:

- ASML layout revision: `gv_7b8519a6de504f4b8c170ed07a1b2111`.
- Research tutorial: `gv_04d74788d74d4101892b710c04db4bd7`.
- Ownership tutorial: `gv_ddd86a262a7e4e5884ab3477562fd8a5`.

The previous ASML version is still scheduled for September 27 at 10:02 AM
Pacific. The revised version requires review before replacing that queued post;
never overwrite its approved media asset in place or publish both versions.
Already published Microsoft media remains unchanged. The local comparison
uses its original footage and narration in `artifacts/social-video-refresh/`.

## September 30 motion design

The approved motion design is now `motion_v1`, navigation template version 6,
used by newly generated schema 4/5 navigation videos. Previous presentation
names remain available for reproducible older renders.

- Dark graphite background with a subtle grid and green chapter accents.
- NVDA keeps the related server-room image at 12% opacity; other tickers use
  the neutral dark plate rather than unrelated stock imagery.
- Short scene headings wrap without dropping words. Chrome fades in over
  220 ms, with a restrained chapter-line reveal and timed progress segments.
- Actual recorded product pixels, navigation actions, narration timestamps,
  and word-aligned captions remain intact. Captions do not bounce or fade.
- Original logo, a concise closing card, tight captions, and fine-print
  research/paid-plan notices stay within the existing social safe area.
- Decorative motion does not imply price movement or invent financial data.

This deploy changes presentation only. It does not change voice providers,
worker capacity, tutorial cadence, approval gates, publishing permissions,
or existing approved/scheduled media. The earlier local concept preview used
retained Grace audio; production continues using its configured narration.

Validation includes safe-area/source-pixel tests and a full native render
using the retained Microsoft footage and audio. Timing, action alignment,
caption count and research provenance are compared with the previous render.
The local comparison is in `artifacts/walnut-motion-production-2026-09-30/`.
