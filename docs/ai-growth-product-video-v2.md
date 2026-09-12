# Walnut product video V2

## Creative decision

The first render proved the pipeline, but did not demonstrate the product. V2 is a founder-directed product campaign: **Find what deserves your attention. See why. Keep score.** The owner selected a natural, confident male founder voice.

The reviewed script opens with “Everyone has an opinion on NVIDIA. I want to see why.” It then moves through the actual NVIDIA chart, Confirmation Score, disagreement and risks, and Outcomes, ending with “Try Walnut Markets free.” It does not recite a share price without showing its context. Three selectable hooks test opinion, score explanation and accountability against the same demonstration.

This first V2 script is reviewed editorial copy, not an Astra-generated financial finding. The existing evidence-checked research-video pipeline remains available. The campaign's score of zero explicitly means no measured search-demand score has been assigned. Future keyword-led campaigns should use actual connected search data, rather than infer it from a topic's popularity.

## Observed references

Inspected Walnut's own social pages in the owner's Chrome session in September 2026:

| Reference | Observed creative | Observed result |
| --- | --- | --- |
| [TikTok: Screen before the crowd](https://www.tiktok.com/@walnutmarkets/video/7682469159460015380) | Person in a car, lifestyle motion, dense overlay text; no demonstrated Walnut interface | 16 seconds, 227 views, 3 likes, zero comments and saves |
| [Instagram: Terminal energy](https://www.instagram.com/walnutmarkets/reel/DdD5XDWE9SS/) | Architecture/lifestyle background, reaction cutout and meme text; no Walnut interface | Insights: 113 views, 102 viewers, 98.2% non-followers; zero likes, comments, saves, shares, profile activity and follows |

These are small snapshots, not evidence of virality or a measured signup conversion rate. Retention, bounce rate and watch-time data were not exposed in the inspected insights. The useful creative lesson is immediate personality and motion. The next hypothesis is that **recognizable product proof and a specific CTA** can turn attention into qualified interest.

## Research informing the execution

- [TikTok's finance creative guidance](https://ads.tiktok.com/business/creativecenter/quicktok/online/Finance_SEA/pc/en) recommends an early, product-related hook, benefits and a clear CTA. Test different hooks rather than treat a formula as a guarantee.
- [TikTok's creative guide](https://ads.tiktok.com/business/en/guides/what-is-ad-creative-guide) supports native-feeling problem/solution demonstrations and iteration using both attention and conversion metrics.
- [Meta Reels guidance](https://www.facebook.com/business/ads/facebook-instagram-reels-ads) informs vertical framing, audio and safe placement of important text.
- [ElevenLabs TTS best practices](https://elevenlabs.io/docs/overview/capabilities/text-to-speech/best-practices) informs voice selection, natural stability and punctuation. V2 uses Chris with Eleven v3 in one continuous take. This is a synthetic narrator in a founder-like style, not a clone of the owner.

No video can be promised to go viral. Compare initial hold, watch time, completion, profile/link actions, landing-page conversion and activated signups. Preserve each hook's UTM and eventual post ID. Do not declare a winner based on a handful of views.

## Implementation

- Real Playwright recordings of the NVIDIA and Outcomes product pages, with the owner's authorized short-lived admin session. Credentials remain in memory, expire after five minutes and are never stored in job assets. Capture navigation is fixed; unrelated APIs, mutations and tracking requests are blocked.
- Product sections are cropped; no generated UI, redrawn numbers or account/admin panels appear in the final composition. Gated or empty captures fail rather than become substitute slides. Source text, timestamps and hashes accompany captures.
- A single ElevenLabs narration supplies character timing for shot boundaries and short phrase captions. Audio is normalized in the final encode.
- The existing worker renders 1080×1920 H.264/AAC using FFmpeg. This avoids a Creatomate upgrade; existing hosting, storage and narration usage still apply.
- Original lifestyle background, restrained gold/navy typography, motion inside product shots and subtle background movement. The backdrop conveys a premium setting without claiming investment-related wealth.
- Existing job leases, quotas, retry handling, private storage and manual review/download gates remain in force. No automatic social publishing.
- Founder brief summaries live in `backend/app/assets/growth/brief-v2.json` and are imported into the versioned Growth Brief. Treat supplied audience quotes as projections and competitor comparisons as unverified until researched.

## Original background asset

`backend/app/assets/growth/investor-study-v2.png`

Generation prompt: Create a single premium editorial photograph in vertical 9:16 format: a quietly luxurious investor's study overlooking the Manhattan skyline at blue hour. Dark walnut desk in the lower third, a black leather chair, warm brass lamp, subtle emerald reflections, city bokeh and a closed unbranded laptop. Cinematic 35 mm photography, warm gold against midnight blue, ample dark negative space in the upper center. No people, text, logos, charts, numbers, cars, yachts or montage. The image is an original generated background, not product evidence.

## Verification

Focused tests cover reviewed-copy integrity, one narration call, actual timing alignment, job ownership, stage transitions and review/download gates. Frontend TypeScript validation covers the new campaign and hook selector. Real capture and encoded-output checks are required before treating a job as review-ready; final production job details are recorded after rendering.
