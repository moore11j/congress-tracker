# Social video layout and content mix

## October 4 approved implementation follow-up

The subsequent [editorial and video implementation report](research-editorial-quality-2026-10-04.md)
supersedes the proposal status below: a bounded Sol selector and version 3 daily
research flow were deployed October 4 as `fdafb86f`, with the finding shown before navigation.
Old videos/tutorials remain reproducible. Two voice-only auditions completed;
neither is selected for production. API/cron/video model settings were verified; the retained Zeely
presenter crop still needs repair before that footage is republished.

## October 4 creative-quality audit and local comparison

The owner requested viral-video research, more natural narration, stronger
graphics/editing and a better-value writing model than 5.4 mini. This audit
examined current source and retained Zeely media, not the live provider settings
or current channel analytics. No subscription, production setting or publishing
permission changed. Implementation is an **offline comparison renderer**, not
an activated replacement for the daily pipeline.

### Reference sites and evidence

- [TikTok Creative Center Top Ads](https://ads.tiktok.com/business/creativecenter/inspiration/topads/pc/en)
  provides advertiser-authorized examples with performance filters and
  second-by-second analysis. These are paid-ad references, not proof of organic
  virality. [TikTok's explanation](https://ads.tiktok.com/resources/help/article/top-ads?lang=en&redirected=2)
  describes what the dashboard measures.
- [Motion's eToro library](https://motionapp.com/library/etoro) provides embedded
  Meta videos and timestamped creative breakdowns. Two videos were downloaded
  through the visible player and inspected at six timestamps each: Tori's
  spreadsheet/tab-overload skit and the West Ham "ick" interview. The page does
  not establish their views, conversions, Instagram-only placement or organic
  virality. Its automated annotations are hypotheses, not verified outcomes.
- [Shortimize](https://www.shortimize.com/) offers cross-platform account tracking
  and outlier detection for TikTok and Instagram. Useful for finding videos
  outperforming an account's own baseline; no account/trial was created.
- For a documented organic example, [Money's interview with Humphrey Yang](https://money.com/tiktok-financial-advice-humphey-yang/?amp=true)
  reports that his Hydro Flask cost breakdown took him from roughly 10,000 to
  over 100,000 followers. A familiar product, a surprising price/cost comparison
  and plain explanation are plausible creative mechanisms, not proven causes.
- Yang's rice/wealth visualization is another documented historical example:
  [contemporary coverage](https://www.businesstoday.in/amp/latest/trends/story/tiktok-user-uses-rice-to-show-jeff-bezos-enormous-wealth-251151-2020-03-02).
  The transferable idea is a visible comparison with a clear unit, not the
  celebrity subject or dated wealth figures. This example was reviewed through
  coverage, not a complete audiovisual playback.

### What to borrow, and what the samples actually show

| Example | Observed or documented device | Walnut adaptation |
|---|---|---|
| Tori skit | Large presenter, sunglasses as recurring role-change cue, short captions, familiar manual-work frustrations | State one investor question, reveal the relevant product evidence, then explain the answer. Keep one recognizable visual cue; avoid importing Tori's automation promises. |
| West Ham interview | Question/reaction loop, large paddle reveal, conversational framing | A short "holding shares or buying more?" reveal followed by actual quarter/date labels. Do not fabricate interviews or testimonials. |
| Hydro Flask breakdown | Familiar object and surprising price/cost gap, with reported audience growth | Compare a headline with the financial evidence behind it. Show the difference rather than narrating a full article. |
| Rice visualization | Concrete visual scale makes a large number comprehensible | Use accurately scaled, labeled comparisons from verified data; never decorative fake price charts. |

These are testable creative hypotheses. Public examples do not isolate editing
from distribution, existing audience, paid spend, topic demand or luck.

### Specific Walnut findings

The retained September Google/Grace export keeps essentially the same dense
4:5 panel throughout. The changing lower callouts are much smaller than the
main composition, and most data is difficult to read at phone size. At the
16-second sample, the presenter circle contains artwork instead of a face:
source tracking is a separate QA failure and must not be carried into a new
publishable render. This does not establish that every current export has it.

Current source confirms three distinct systems:

1. `growth_daily_video.py` builds an extractive title/navigation/takeaway script
   with a fixed sequence; it does not invoke the creative-writing model. It
   hardcodes an ElevenLabs voice ID and `eleven_v3` in the board.
2. `growth_video_pipeline.py` uses the configured model to select existing
   statement IDs. `growth_video_domain.py` restricts hooks to generic approved
   copy; a better model cannot write a new hook under that contract.
3. Zeely's Grace workflow is separate. Its proprietary writing/voice settings
   were not inspected live. Changing Walnut's OpenAI model does not replace
   Zeely's voice, avatar or lip-sync.

Local defaults are `gpt-5.6-sol` for general AI Growth and `gpt-6-astra` for the
generic video configuration. Stored production overrides may differ; there is
no basis in this audit to assert that all current videos use 5.4 mini.

### Proposed production direction

- Open with a specific question or useful distinction in the first 1–2 seconds.
  Deliver an initial answer before a long navigation sequence. For tutorials,
  show the real clicks; for research stories, lead with the actual finding.
- Use one claim per scene, a meaningful visual change around every 2–4 seconds,
  and longer holds when reading numbers requires them. Test 20–35 seconds first;
  these are design hypotheses rather than algorithmic thresholds.
- Use hard cuts on thought changes and restrained 120–200 ms transitions only
  for context changes. No bouncing text, random zooms or constant cursor motion.
  Keep one continuous voice take across edits; align captions and highlights
  to actual timestamps, not estimated reading speed.
- Make the selected row/figure large. Preserve source, period and metric label.
  Use true-to-data comparison graphics and actual Walnut screens. Dim related
  background footage, such as server racks, so it never competes with evidence.
- Keep the approved logo, system font and mint/dark palette. A closing card
  should take roughly 1–2 seconds and ask for one action tied to the story.
  Preserve platform disclosure and owner review. Do not impersonate a human
  analyst to make generated media appear authentic.

**Voice:** audition the same 12–15-second script in the current voice and two
licensed alternatives. Aim for a calm, conversational researcher with varied
sentence rhythm, clear company names and short pauses, not an announcer or
manufactured excitement. Test current ElevenLabs v4 against the existing v3
path where the account supports it; [the provider now recommends that comparison](https://elevenlabs.io/docs/overview/capabilities/text-to-speech/best-practices).
Verify timestamp compatibility and pronunciation before integration. With a
visible Grace avatar, regenerate matching lip-sync for any replacement audio;
do not dub unrelated speech over existing mouth movement. A voice-only concept
is an alternative experiment, not a silent presenter change. No audition was
generated or voice selected during this audit.

**Writer:** test `gpt-6.1-sol` at low reasoning for one bounded creative pass
containing three hooks, one script and one shot list. Use the model for
storytelling and source-linked interpretation; keep numeric/date checks in
code. Reserve Astra for occasional difficult concepts or failed editorial
reviews, and reuse the approved script when re-rendering layout.
[Official Sol documentation](https://developers.openai.com/api/docs/models/gpt-6.1-sol)
lists standard USD $2 input / $10 output per million tokens;
[Astra](https://developers.openai.com/api/docs/models/gpt-6-astra) lists $10/$50.
At 4,000 input and 2,000 total billed output tokens, that is about $0.028 versus
$0.14 per pass, or $2.80 versus $14 per 100 passes. This illustration includes
reasoning only if it fits within that output allowance; it excludes retries,
tools, audio, rendering, storage and tax. Availability in Walnut's API project
is unverified. Neither text model directly generates the finished video/audio.

Do not simply remove existing statement validation. Add a separate source-bound
creative draft contract with claim IDs, source dates, shot instructions and
manual approval. Unmatched names/numbers or unsupported causal conclusions
must block rendering. Keep source/media hashes and immutable approved versions.

### Concrete next concept and experiment

Tutorial script (draft, approximately 30 seconds; record real navigation):

> That fund owns Nvidia. But did it buy more—or is it just a big existing
> holder? Those are different stories. Open Nvidia in Walnut and check
> Ownership. Look for changes in shares, then check the reporting quarter and
> filing date. Thirteen-F filings are delayed snapshots, not live trades.
> Now you know what the headline leaves out. Check the company you're
> researching at Walnut Markets.

Shot plan: question over a real ownership heading → date/quarter close-up →
same-holder comparison if available → filing-date limitation → short CTA.
Capture only fields the actual page supports. This tutorial does not claim
who bought recently; a "top three buyers" research story must name the three
verified managers and the ranking metric early in the video.

Test two opening hooks with the same body, footage and voice, then test voice
separately on the stronger concept. Rotate story structures (a surprising
comparison, a misconception, a named finding, a useful tutorial), rather than
only changing tickers. Inspect 24-hour and seven-day retention, average watch
time as a fraction of duration, completion, saves/shares per view, qualified
visits and attributable signups. Separate platforms and paid from organic;
report denominators. A 0–200-view sample is exploratory, not proof of a winner.

### Local deliverable and validation

`scripts/render_social_evidence_review.py` adds a reusable offline comparison
compositor with explicit screenshot crop bounds and scene times. It changes
only the upper evidence panel, preserving the retained lower presenter/callout
band and the original audio stream. The local Google example uses eight
evidence scenes and is prominently labeled historical / not for posting.
This is an editing comparison, not fresh research or a completed voice upgrade.
Its source's presenter defect remains visible; fixing source tracking and a
fresh phone-size audiovisual review are prerequisites for publication.

Output: `artifacts/video-creative-audit-2026-10-04/walnut-evidence-edit-review.mp4`.
Reference and comparison contact sheets are beside it. FFmpeg fully decoded
the export and verified the copied audio's SHA-256 matches the original.
Sampled frames were visually inspected. No claim of full listening review or
measured performance improvement is made. The artifact directory is ignored;
this tracked document preserves conclusions, while media remains local.

## October 4 readability feedback

The October 4 zoom review is saved in `artifacts/social-closeups-2026-10-04/`:
`walnut-zoomed-review.mp4` and `before-after.jpg`. It reuses the retained,
dated September Microsoft research footage and narration as a layout comparison,
not a fresh research publication. No social post was published by the preview.

The opt-in `closeup_v1` renderer (template 7) expands the evidence panel from
824×490 to 824×800 and moves captions from y1060 to y1360. The final footer
moves down 240 pixels, reducing the unused space above the native account area.
Essential content stays within x48–912, y270–1640. These are review bounds based
on the supplied screenshots, not a guarantee across every platform display.
Every recorded frame requires an explicit, bounded `evidence_camera` crop;
the renderer rejects missing or out-of-source rectangles before encoding.
For this sample, the complete Quick answer and What changed cards occupy a
638-pixel source column, enlarging their text about 1.9× relative to the earlier
wide view. Navigation scenes retain broader context where necessary. Captions,
audio, action alignment and research provenance are retained.

The owner's subsequent commit-and-deploy approval activates `motion_v2`
(template 7) for new renders. It uses the same expanded panel and lower captions
as the reviewed sample. New research captures frame the search control, the
published brief title and its takeaway from their actual browser bounds; the
takeaway zoom starts as soon as the evidence has scrolled into view. Complete
element widths are retained and rectangles stay inside the source viewport.
Old recordings without focused camera metadata retain their source context in
the taller layout; they are not cropped around a guessed cursor position.
`motion_v1` and the strict per-frame `closeup_v1` review mode remain available.
Existing approved/published media and schedules are not rewritten.

Release validation: 153 tests passed and the opt-in browser integration test was
skipped. Seven research-suite failures match the independently reproduced
pre-change baseline recorded in the Boeing correction verification package;
all video layout/navigation checks and new contract safeguards passed. The
28.32-second default-render sample retains the original narration, action timing,
caption count and source ID, and is fully decoded before release.

Owner-reported baseline: Instagram 35 followers; TikTok 17, with most videos
at 0–200 views. Treat these as an early audience baseline, not a measured
verdict on the product or a reason to increase posting volume.

The ASML Reel made the app too small to read. For the next review version,
use tightly cropped real evidence screens, one claim per scene, and narration
that names the visible figure, its date, source, and meaning. Briefly establish
the page, then enlarge the relevant rows or metric. Preserve enough headers
and date labels to explain the evidence. Review at phone size; if the evidence
cannot be read without pausing or pinching, reframe before approval. Reuse the
configured narrator; a presenter change needs an explicit owner request.

For Reddit source questions, prepare a direct answer naming the actual source
for the claim and linking the original record. Explain reporting delays and
distinguish source data from Walnut's interpretation. Do not claim all feeds
are real time. The owner has not requested posting replies in this feedback.

The Boeing brief's contract-date correction must be reflected in any new
social draft. Do not reuse the $1.55B “recent awards” hook or its bullish
conclusion. See the verified record package in
`artifacts/boeing-date-correction-2026-10-04/`.

Owner feedback, September 26, 2026: native TikTok/Instagram controls obscure
the masthead and lower captions; repeated account branding is distracting.
Keep the content and voice, recompose the scene instead of scaling the full
video, and interleave useful app tutorials with search-led research videos.

## Composition

- `social_v1` was the September 26 layout (template version 5), followed by
  `motion_v1` (template version 6). The October 4 release defaults to `motion_v2`
  (template version 7), with the expanded bounds documented above.
- Full-bleed atmosphere remains 1080×1920. Essential content occupies
  x48–912, y270–1390 in the older layouts. These bounds derive from the supplied mobile
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
Financials, analysts, and insiders version 2 reached `READY_FOR_REVIEW` and
passed full MP4 decoding plus beginning/middle/closing frame review. Their
durations are 23.70, 24.80, and 25.06 seconds. The insider capture includes real
Activity View, Buys, and Sells clicks. Congress version 2 is `BUDGET_WAITING`
until `2026-10-04T00:00:00+00:00` (October 3, 5 p.m. Pacific). No new video has
been approved or scheduled by this change.

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
