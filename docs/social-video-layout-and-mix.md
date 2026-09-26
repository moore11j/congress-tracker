# Social video layout and content mix

Owner feedback, September 26, 2026: native TikTok/Instagram controls obscure
the masthead and lower captions; repeated account branding is distracting.
Keep the content and voice, recompose the scene instead of scaling the full
video, and interleave useful app tutorials with search-led research videos.

## Composition

- `social_v1` is the default navigation-video layout (template version 5).
- Full-bleed atmosphere remains 1080×1920. Essential content occupies
  x48–912, y270–1390. These conservative bounds derive from the supplied mobile
  screenshots; native app chrome can vary by device and viewing mode.
- Real app footage occupies a focused panel above a caption band at y1060.
  Preserve table columns; follow the recorded vertical action or evidence crop.
- Omit the repeated top masthead. Use the original Walnut logo on the closing
  card. Keep the existing system typography, mint accent and narration voice.
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

## September 26 delivery

Deployed image: `registry.fly.io/congress-tracker-api:social-layout-20260926-v2`.
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
