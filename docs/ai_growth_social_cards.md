# AI Growth Social Cards

AI Growth social cards use structured LLM output plus deterministic rendering.

The ChatGPT API generates concise JSON: card type, ticker context, headline, subheadline, bullets, key stats, chips, CTA, source label, tone, and visual emphasis. The backend validates and normalizes that object, then renders a branded 1600x900 Walnut Markets SVG card. Existing asset download logic converts SVG data URIs to PNG for email attachments.

This is better than raw image generation for X and approval emails because layout, contrast, text wrapping, brand treatment, and attachment format are predictable. The model supplies judgment and copy, while code owns pixels, spacing, safe margins, and fallbacks.

Social cards should use the shared SEO language in `docs/seo_keyword_language.md`: Congress trades, congressional stock trades, insider activity, insider trading tracker, stock research, ticker intelligence, market signals, institutional activity, government contracts, fundamentals, technicals, and signal stack. Keep `confirmation stack` as supporting language, not the primary headline. Options Flow is still coming soon and should not be described as available.

Implemented templates:

- `article_reactive`: fast news and article reaction cards.
- `ticker_signal`: confirmation score and ticker signal cards.
- `congress_insider_activity`: Congress and insider disclosure cards.
- `research_cover`: Reddit/DD cover cards.

To extend templates later, add the new `card_type` or `template` option to `SOCIAL_CARD_TYPES` / `SOCIAL_CARD_TEMPLATES`, update `_social_card_json_schema`, add a branch in `_social_card_type_for_context` if needed, and adjust `_social_card_data_uri` or split it into a dedicated renderer function. Keep LLM output as JSON only; do not ask the model to generate the final image.

The internal demo set is available from `ai_growth_social_card_demo_assets()` and the admin endpoint `/api/admin/ai-growth/card-demo`.
