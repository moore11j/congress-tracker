# AI Growth Thumbnails

AI Growth X and Reddit drafts should use real AI-generated thumbnails, not deterministic SVG cards.

The ChatGPT API still returns concise JSON for `social_card` and `visual_brief`, but that data is now treated as art direction: ticker, visual emphasis, and the market story. When `AI_MARKETING_IMAGE_GENERATION_ENABLED=true`, the backend sends that art direction to the image generation endpoint, then overlays the official Walnut Markets logo lockup before attaching the JPEG thumbnail.

The target look is a premium 16:9 finance-media visual: dark studio background, teal/emerald Walnut glow, official Walnut Markets logo lockup in the reserved upper-left area, a primary ticker, and one large market metaphor such as a semiconductor package, filing archive, bank tower, terminal glow, disclosure folder, or market infrastructure object. Keep text minimal so it stays legible on X and Reddit.

Do not ask the model to invent or render a Walnut logo, icon, tree, brain, or wordmark. The backend owns logo consistency by compositing `backend/app/assets/walnut-markets-logo-lockup.png` after image generation.

Do not render visible source/footer text in thumbnails. Source context belongs in metadata, captions, post copy, or review notes, not in the image.

Thumbnail headlines must be complete market statements. Avoid vague fragments like `bearish confirmation is leading`; use clear claims such as `Bearish trend confirmed` or `Bearish signal identified`, then name the underlying data that supports the claim.

Do not attach deterministic SVG cards as a fallback for generated X/Reddit drafts. If image generation fails, the draft should surface without the old card rather than posting a cramped dashboard-style graphic.

Social thumbnail art direction should use the shared SEO language in `docs/seo_keyword_language.md`: Congress trades, congressional stock trades, insider activity, insider trading tracker, stock research, ticker intelligence, market signals, institutional activity, government contracts, fundamentals, technicals, confirmation score, and underlying data.

Keep the distinction clear: confirmation score is Walnut's proprietary score. Underlying data means price/volume, fundamentals, reported institutional activity, Congress/insider activity, contracts, technicals, and other cited evidence. Use data, underlying data, or data sources instead of stack in public-facing thumbnail and post language. Options Flow is still coming soon and should not be described as available.

Implemented art-direction types:

- `article_reactive`: fast news and article reaction thumbnails.
- `ticker_signal`: confirmation score and ticker signal thumbnails.
- `congress_insider_activity`: Congress and insider disclosure thumbnails.
- `research_cover`: Reddit/DD cover thumbnails.

Environment controls:

- `AI_MARKETING_IMAGE_GENERATION_ENABLED`: set to `true` to attach generated thumbnails.
- `AI_MARKETING_IMAGE_MODEL`: defaults to `gpt-image-2`.
- `AI_MARKETING_IMAGE_SIZE`: defaults to `1536x1024`.
- `AI_MARKETING_IMAGE_QUALITY`: defaults to `high`.

The legacy internal demo set remains available from `ai_growth_social_card_demo_assets()` and `/api/admin/ai-growth/card-demo`, but those SVG demos are not the publishing path for generated X/Reddit drafts.
