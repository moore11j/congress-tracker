# HeyCatch SEO review — October 6, 2026

## Source and scope

Read the entire [public audit](https://app.heycatch.ai/audit/hck_pk_tat6ODp81Q4kpJWd9iqoXYLr1-9JETej/seo) in Chrome, including all eight findings and their affected URLs. No separate action plan was present. Overall score: 85/100. Crawlability 23/23 and Authorship 6/6 have no open fixes; Page clarity is 14.9/22 and Quotability 9/11. Reviewed Page clarity first.

The actual report uses `c_*` and `g_*` codes, not D1.1 codes. It exposes passing-page counts and recoverable points for individual findings, **not an earned/max point score**. The table preserves those exact values rather than inventing point scores.

| Code | Original passing pages | Points described as “worth” | Decision and implementation |
|---|---:|---:|---|
| c_thin | 0 of 1 | 2.0 | Rejected the thin-page diagnosis. The live NBIS/CRWV inner article main contained 1,464 whitespace-delimited words before edits. No padding, merge or deletion. |
| c_title | 16 of 26 | 0.8 | Shortened the ten flagged titles: both calculators, stock research app, stock analysis platform and six competitor comparisons. All resulting complete titles are 30–60 characters. Kept the passing StockAnalysis comparison title. No global title truncation or changes to unrelated metadata. |
| c_question_headings | 0 of 1 | 2.0 | NBIS/CRWV now asks what to compare, which company had greater Q1 scale, what the Confirmation Score means and how to check the tickers now. Each heading is followed by a direct answer. |
| c_meta_description | 17 of 26 | 0.7 | Rewrote nine flagged descriptions in seoLandingPages, commercialFeaturePages and the two calculator routes. Each is 110–160 characters and describes the actual page. |
| c_internal_links | 16 of 26 | 0.4 | Added contextual workflow links on seven comparison pages, four descriptive FAQ guide links, and three NBIS/CRWV methodology/research links. Improved the generic ticker anchor. Research hub already has three article links and two topic guides from October 5; did not duplicate those. |
| c_image_format | 0 of 5 | 1.0 | The five commercial feature pages share a picture element with a WebP source and original-image fallback. Corrected intrinsic dimensions to 1265×712. Original .png is actually JPEG, 74,283 bytes; final quality-88 WebP is 48,638 bytes (34.5% smaller). Lossless trial was larger and was replaced. |
| g_definition | 13 of 26 | 1.0 | Added/reworked plain definitions in three commercial landing intros, seven comparison intros, FAQ, research hub and the NBIS/CRWV intro. Existing competitor positioning retained without new pricing/coverage claims. |
| g_sentence_length | 16 of 26 | 0.8 | Split dense opening copy on comparison pages, FAQ and research hub; shortened several NBIS/CRWV explanatory paragraphs. No padding of short labels or arbitrary splitting of source names. |

## Boundaries and flags

- Audit-only length thresholds are editorial heuristics, not Google ranking gates. [Google explicitly has no preferred word count](https://developers.google.com/search/docs/fundamentals/creating-helpful-content). [Titles](https://developers.google.com/search/docs/appearance/title-link) and [snippets](https://developers.google.com/search/docs/appearance/snippet) can be rewritten/truncated by Google according to context and display width.
- The audit's claim that the root layout appends a brand suffix does not match current source or the observed NBIS/CRWV browser title. Full literal titles already contain the intended brand. Shortened the specific flagged titles; did not introduce a global clamp or duplicate suffix.
- Existing comparison sidebars/footer links and research hub links mean “failed internal links” is not proof those pages are orphans. Added useful in-context routes where justified.
- Historical NBIS/CRWV Q1/July figures, sources and original publication date remain intact. October 6 describes reading-guide changes, not a refreshed financial assessment. Static sitemap dates and research update resolvers reflect only edited routes.
- No testimonial, credential, new data claim, paywall change, homepage hero rewrite, crawlability change or authorship-policy change. No audit rerun credits spent, Search Console submission, email or production content mutation.

## Checks and delivery

Implementation was reviewed locally on top of `ae29641b`. The owner approved commit and frontend deployment on October 6; release verification is pending. Unrelated local changes are excluded from this release.

- 17 existing focused commercial/comparison/research discovery/distribution checks passed.
- Source metadata length inspection: all ten revised titles and nine descriptions within the report's requested bands; descriptions remain distinct.
- Standalone TypeScript and final production build passed, including all 67 static pages.
- Expanded SEO run: 19/21 pass. Two unchanged baseline failures: ticker snapshot description expects old “temporarily unavailable” wording; middleware test loader cannot resolve `./tickerSeo`. The relevant test/source/middleware files match HEAD. No full-suite pass claimed.
- WebP visual inspection completed; sitemap XML has 32 unique URLs, with exactly 21 edited-route dates updated. `git diff --check` passed. Final built HTML verified all seven comparison titles/descriptions within the requested lengths, unique titles, one canonical each and new contextual links. The article has the question headings, guide links and retained July/Q1 dates. Chrome local production preview verifies the commercial-page definition, description, desktop layout and successful selection/loading of the 1265×712 WebP. Local environment links use its existing localhost app setting; production configuration was not changed.
