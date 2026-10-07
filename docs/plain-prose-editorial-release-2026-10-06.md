# Plain prose editorial release, October 6, 2026

The owner requested research briefs and site copy without AI style dash punctuation, unnecessary hyphenated prose or canned transitions such as "this matters because", then authorized commit and deployment.

## Changes

The shared drafting and revision contract now states this preference explicitly. Writer prompt version is `research_brief_v11_plain_prose`. Validation flags even one prose dash, common avoidable compounds and canned emphasis. Exact source names, negative values, numeric ranges, URLs, identifiers, code, Markdown tables and blockquotes are preserved. Existing manual publication override behavior is unchanged; text is not silently stripped or financially rewritten.

Site copy replaces prose separators with sentences, commas or colons across the homepage, pricing, FAQ, research archive and static articles, directory labels and relevant research/admin UI. The Research Memory heading is now "Changes in the evidence". Numeric placeholders and required syntax remain. The stable hero, product promises, financial figures, access and prices are unchanged.

Existing published database briefs are not rewritten. The next generated draft still needs editorial review; prompt and validator tests cannot establish improved live output quality.

## Checks

- Python 3.14.2, isolated SQLite: 26 editorial tests and four confirmation integration tests passed.
- Seventeen focused frontend research/distribution/commercial/comparison checks passed.
- Standalone TypeScript passed; diff whitespace check passed.
- Production Next.js build passed with 67 static pages. Existing stale Browserslist data warning only. No full-suite claim; deployment checks follow below after rollout.

## Release state

Approved release prepared from `1586bb24`. Unrelated study scripts, shared documentation history and generated TypeScript build state are excluded. No content generation, article publication, social posting, email, new subscriptions or production data edits are part of this release.
