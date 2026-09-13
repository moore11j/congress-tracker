# Walnut cinematic presentation V5

Owner request: dim graphics/background imagery and cinematic scene transitions,
inspired by the car commercial linked below, while keeping readable narration
captions and the real Walnut navigation.

Reference: https://www.reddit.com/r/vibecoding/comments/1wc5822/i_asked_gpt6_astra_to_make_a_new_car_brand_and/
The creator describes combining image generation, a storyboard, and Veo clips.
Walnut's treatment uses an original generated background, deterministic motion,
and the existing genuine product recordings. No reference video assets are reused.

## Treatment

- Dark glass architecture, mint rim light, navy shadows and slow background parallax.
- Dimmed background with a darker caption/footer area.
- Short eased scene arrivals and edge-light sweeps, with subtle panel shadows.
- Existing website font, supplied logo, continuous voice and phrase captions.
- Original footage order, pointer trace and narration/action alignment are retained.
- 1080x1920, 24 fps, H.264/AAC, 48 kHz audio at 128 kbps.

`render_navigation_video` now defaults to `cinematic_v2`; `presentation='classic'`
retains the previous layout, and `cinematic_v1` retains the first cinematic cut. Existing stored MP4s are not replaced. The render
metadata records the presentation and illustrative background. This is a visual
revision, not new research: the existing numerical obscuration and source QA
limitations still apply. No new narrator request or generative video subscription.

Local preview: `backend/artifacts/product-ad-v5/walnut-nvda-cinematic-v5.mp4`.
The source is V4 job `gv_fead5f65959844e4a4559202eac6e244`; every reused audio and
capture file is verified against its recorded SHA-256 before rendering.

## Background provenance

Built-in ImageGen, new image; no input image or third-party commercial copied.
Saved asset: `backend/app/assets/growth/walnut-cinematic-atrium-v1.png`.

Exact prompt:

> Use case: ads-marketing. Create a premium cinematic background plate for a vertical 9:16 Walnut Markets stock research product video. Ultra-detailed photorealistic architectural macro: smoked glass fins and dark brushed graphite towers, like an abstract modern financial district at night, with restrained mint-teal rim lighting (#6ee7b7) and deep navy shadows (#020617). Sculptural glass curves, polished reflections, depth of field, soft volumetric light, expensive studio cinematography. Composition: tall portrait, architecture framing left and right edges, large dark calm negative space in the middle for a real website recording to be overlaid later. Delicate brighter accents at the edges and lower third; center stays uncluttered. Sophisticated, dimensional and tactile, not a generic glowing sci-fi tunnel. No text, no letters, no logo, no charts, no numbers, no people, no cars, no gold/yellow, no watermarks. This is atmospheric decor, never a depiction of financial data. Highest available detail, portrait image.

## Validation

Tests verify the background changes over time, the lower text area remains dark,
panel styling preserves the supplied product pixels, and scene arrival settles
without delaying or skipping the captured UI actions. Existing navigation tests
continue to cover phrase timing, source provenance and the manual review gate.

Seven focused tests passed. All 701 encoded frames decoded successfully; eight
scene checks had mean RGB error below 2/255 compared with their raw frames.
The final file is 59,330,892 bytes, 29.21 seconds, 1080x1920 at 24 fps.
Review job: `gv_7ffde05d836d4caea7258ada8e7b55fb`, revision 4, ready for review.
Captures and narration were copied into the child job's private asset prefix,
verified by hash, and passed through the normal render budget and review stages.
No video, caption or comment was published.

## V6 refinement

The owner requested a background at least 50% dimmer and a much tighter caption
box. `cinematic_v2` applies 45% brightness to the completed background only (55%
dimmer than V5), before adding product footage, logos and text. Caption borders
follow measured glyph bounds with 16px horizontal and 10px vertical padding,
including wrapped phrases; there is no full-row panel or decorative side bar.
Existing narration, timings, captured product screens and numerical masks remain.
Output: `backend/artifacts/product-ad-v6/walnut-nvda-cinematic-v6.mp4`.
