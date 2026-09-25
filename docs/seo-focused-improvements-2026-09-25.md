# Focused search growth pass — September 25, 2026

## Fresh baseline and selection

Read directly from the existing signed-in Google Search Console domain property. Web search, 28 days, August 27–September 23, 2026: 4 clicks, 299 impressions, 1.3% CTR, average position 47.6. The interface reported an update five hours earlier. These are property totals, not non-brand totals or signup counts; anonymized queries mean the visible query rows do not reconcile to all impressions.

The indexing report is dated September 20: 113 indexed, 3,854 discovered but not indexed, 20 excluded by noindex, 4 redirects, 4 crawled but not indexed, 12 redirect errors, 6 robots-blocked, and 1 duplicate without a selected canonical. These lagging classifications are not proof of live defects. No indexing requests, validation restarts, or sitemap resubmissions were made in this pass.

| Page | Observed 28-day impressions / position | Reason for selection |
|---|---|---|
| `/departments/department-of-energy` (app host) | 23 / 16.3 | Existing visibility; needs an explanation of laboratory operators, recipient attribution, and value definitions. |
| `/departments/nasa` (app host) | 29 / 54.2 | Existing visibility and an existing related research article; clarify contract vehicles versus funded orders. |
| `/departments/department-of-defense` (app host) | 23 / 63.0 | Existing visibility; property queries include “dod contracts awarded today” (3 impressions). This is not a claim that every such impression belongs to this page. |
| `/government-contracts` | Not extracted individually | Connect the agency pages, explain comparable amounts, and connect existing research. Prior Keyword Planner evidence showed 10–100 monthly searches for “government contract stocks”; this is market demand, not Walnut performance. |
| `/institutional-filings` | Not extracted individually | Explain share counts versus market values, reporting delays, and short-position limitations, with SEC source guidance. |
| `/institutional-activity-tracker` | Not extracted individually | Connect the product workflow to the existing NVIDIA ownership article, matching the September 21 keyword evidence. |
| `/research` | Not extracted individually | Give two useful topic entry points to the existing archive without replacing or duplicating chronological pagination. |

Important qualification: Energy's page-level position of 16.3 is not a proven first-page opportunity for its target terms. Its visible query rows were “department of energy contracts” (1 impression, position 62) and “doe contracts” (1 impression, position 69). The remaining impressions are not explained by the visible query table. The pass therefore improves relevance and discovery rather than promising a move from position 16 to 8.

For context, the homepage had 3 clicks / 37 impressions / position 15.9; ALV 67 impressions / 69.7; ANET 37 / 53; Insights 24 / 25.3; ACI 19 / 70.1; comparison index 15 / 4.5. No broad ticker or homepage rewrite was justified by this small sample.

## Implemented changes

- Three distinct agency guides with official sources, recipient/parent-company caveats, amount definitions, date context, organizational attribution, and relevant research links.
- Changed the department header from “Last updated” to “Latest tracked award.” The backend computes this field from record dates, not ingestion time. Removed the unsupported daily-refresh promise.
- Expanded the contracts hub with three agency links, a practical explanation of award values, and links to NASA, Defense, and Boeing research.
- Expanded the filings guide with a concrete 13F comparison workflow and an SEC source link. Linked the existing NVIDIA ownership article from the guide and institutional-activity page.
- Added contextual return links on four existing generated article slugs, closing the link paths between the articles and the relevant guides/profiles. No stored article text, financial figures, entitlements, publication dates, or publishing schedule changed.
- Added two first-page topic links to the research archive, preserved canonical pagination, and replaced internal campaign terminology in the archive description.
- Added the research archive itself to the marketing sitemap. Updated modification dates only for the three edited landing pages and three substantive agency guides, plus the newly listed archive.

Existing organizational authorship, About, and editorial policy remain in place. A named contributor profile needs a real contributor's preferred public identity and accurate role; no name, qualification, review, or social profile was invented.

## Sources checked

- https://www.energy.gov/management/doennsa-major-site-facility-management-contracts
- https://www.nasa.gov/news-release/nasa-awards-solutions-for-federal-enterprise-procurement-contracts/
- https://www.defense.gov/News/Contracts/
- https://www.usaspending.gov/data/Federal-Spending-Guide.pdf
- https://www.sec.gov/rules-regulations/staff-guidance/division-investment-management-frequently-asked-questions/frequently-asked-questions-about-form-13f

## Validation and measurement

45 focused frontend tests passed covering archive discovery/pagination, article paywall rendering, metadata failure handling, canonicalization, and sitemap eligibility/availability. A clean frontend release copy was prepared from committed base `24732054` with only this pass's files, avoiding unrelated workspace changes. Its Next.js production build passed compilation, lint/type validation, and generation of all 65 static pages. The only build warning was an outdated Browserslist dataset.

All 11 affected URLs passed public pre-release status/canonical/indexability checks. All 11 built local pages then passed rendered content, exact canonical, and expected-link checks using real public backend data. The local host intentionally sends a noindex response header; the verifier requires that preview protection locally and rejects it on production. The initial local check needed network access for backend reads and corrected local-host handling; neither required a product change. The contracts landing page was also visually inspected in the browser. Evidence: `artifacts/seo-focused-release-2026-09-25/baseline-public.json`, `local-public.json`, and `verify_public.py`.

Use the same seven-page cohort when comparing subsequent 28-day periods. Assess relevant non-brand query impressions and clicks, individual indexing, and real organic signup/activation outcomes. Separate market keyword estimates from site performance. Do not claim a gain from a changing sitewide average or a single impression. External outreach remains a proposed next step; no messages or posts were sent.
