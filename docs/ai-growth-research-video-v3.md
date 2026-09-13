# NVIDIA research video V3

Owner direction: use Walnut's website typography and colors, the supplied logo, a finer disclaimer, and narration-matched footage of actual NVIDIA/institutional research. V2's gold palette and lifestyle background did not match the brand.

## Brand and visual treatment

Inspected the live marketing homepage and logged-in NVIDIA page on September 12, 2026. The body and H1 use `ui-sans-serif, system-ui, sans-serif`; Windows resolves this to Segoe UI. The export uses the installed Segoe UI regular and semibold fonts without redistributing them. Rendering on another OS resolves to that host's available system font; a licensed explicit font can be configured through the renderer's environment variables.

Colors follow the website: slate-950 `#020617`, mint/emerald `#6ee7b7`, slate-100 `#f1f5f9`, muted slate `#94a3b8`. The owner-supplied 09_27_25 PM logo is embedded unchanged, scaled for the composition. No gold styling or generated lifestyle backdrop. The disclaimer is one small regular-weight line in the lower safe area. Product shots use original browser pixels in close-up windows sized for mobile viewing.

## Narrative and source

Source: approved/scheduled brief `rb_1789221682029_a265ac`, **Who is buying NVIDIA stock in the latest 13F filings?** An immutable copy and hash are attached to the new job. The source article is not modified or published by this workflow.

Sequence: real NVIDIA chart → Ownership holder names → institutional activity names/actions → filing dates → Hightower profile → filing history → the brief's question → explore NVIDIA CTA. One continuous Chris / Eleven v3 narration drives cuts and phrase captions. This is a reviewed editorial adaptation of the source question, not a newly model-generated financial conclusion.

### Source QA limitation

The live activity feed reports Hightower's NVDA position at approximately $1.5B, while its profile shows approximately $1.5M. The ownership table also shows an internally inconsistent CalSTRS value/share combination. These are product-data issues requiring separate reconciliation. This video does not narrate or display those dollar/share values or assert the brief's aggregate accumulation counts. The activity shot magnifies the original name/date and action columns from the same rows; the ownership shot magnifies holder names. No values are rewritten or fabricated. Full source captures and notes are retained privately for review. 13F quarter-end positions are distinguished from current trading.

## Creative research

Observed examples in the owner's Chrome session; engagement counts are snapshots, not conversion or causal evidence:

- [Barebone AI NVIDIA explainer](https://www.tiktok.com/@barebone.ai/video/7682789817003871520): two-minute educational narrative, animated valuation diagrams, short subtitles; 120 likes, 72 saves and eight shares observed. Useful lesson: each sentence receives a relevant visual. Financial claims in that video were not used as Walnut evidence.
- [JT Capital NVIDIA explainer](https://www.tiktok.com/@jtcapitall/video/7684358743135407367): a topical NVIDIA question, a presenter and supporting visual references. Search showed 2,329 likes; the detail page showed 393 shares. These counts alone do not establish virality or signup conversion.
- [Tread Master product ad](https://ads.tiktok.com/business/creativecenter/topads/7673724882404573200): problem-led opening, a visible product and presenter, seven thousand likes observed. The oversized permanent branding limits usable demo space; Walnut keeps the logo smaller.
- [12-second walkthrough ad](https://ads.tiktok.com/business/creativecenter/topads/7672784571506622485): continuous first-person location walkthrough with a concise overlay; 222 likes, six shares observed. Useful lesson: show the viewer the experience instead of describing an unseen product.

Application: start with the specific research question and real chart immediately; show original records as the narration names them; keep captions brief and readable; end with one relevant destination. No promise of virality. Test retention, saves, link visits and activated signups after the owner approves distribution.

## Validation

56 focused backend tests passed, one browser-specific test skipped. Frontend TypeScript validation passed. Tests cover research provenance/tamper rejection, six captures, a single narration request, manual review, and existing video behavior. Live source captures and final encoded frames are checked separately. Existing V2 jobs remain supported.

The first V3 export uses authorized local capture/render recovery with a short-lived admin session held only in memory. No new subscription or automatic social publishing is introduced. The shared Fly worker's unattended capture throughput remains unproven.
