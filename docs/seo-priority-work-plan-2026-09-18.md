# Walnut search growth: prioritized implementation and editorial plan

This follows the September 18 Search Console audit. Application changes are local pending deployment; this file is a work plan, not a claim that rankings or live pages have changed.

## 1. Discovery and broken navigation — implemented locally

- Research and Insights load public research cards on the server. The archive has six briefs per page and normal next/previous links. Each archive page has its own canonical and Open Graph URL. Invalid and out-of-range page numbers return not found; page=1 redirects to the root archive.
- Public cards retain existing Premium/Pro labels. No full article or session credentials enter the shared public card loader. Provider failures are surfaced instead of silently serving an incomplete static-only archive.
- Published generated briefs repair the two known legacy earnings/financials links at rendering time. Both reach the ticker's Financials tab using a fragment; access checks remain in effect. This avoids rewriting stored historical articles merely to repair navigation.
- Generation/publication sanitization repairs those known paths. Other invented nested ticker paths trigger a publication hard stop. These checks validate the known ticker route structure, not the existence of every ticker or every external URL.
- Generated briefs link back to the canonical research archive and, on public article routes, up to three related public research titles for the same tickers. An archive outage does not prevent an otherwise available article from rendering.
- Published dates, update dates, organizational authorship and AI-assistance disclosure are visible. No human reviewer or credentials are invented. Draft previews are identified as drafts. Organization authorship is also included in Article structured data.
- Generation guidance now calls for question-specific evidence, exact primary-source documents, and a distinction between holdings dates and filing dates. A specific earnings-release label pointing only to a website root gets a review warning.

The current daily process already excludes close keyword duplicates and exposes potential-overlap review warnings. It has not been disabled or replaced. Automatic rewriting, consolidation and deletion of published research are not part of this release.

## 2. Priority page cohort

Keep a stable cohort rather than judging progress only from a sitewide average that changes as new pages appear. The table below is an editorial selection, not a claim of measured keyword volume or conversion performance. “Check” means individual indexing status was not verified in the latest audit.

| Page | Baseline evidence | Next concrete improvement |
|---|---|---|
| https://walnutmarkets.com/ | 39 recent impressions; position 19.4 | Preserve positioning; make useful research accessible through the existing navigation and research section. Assess relevant query CTR before title experiments. |
| https://walnutmarkets.com/research | Live archive contains 27 briefs | Deploy server-loaded cards and linked pagination; inspect rendered HTML and page 2 canonical. |
| https://app.walnutmarkets.com/insights | 34 recent impressions; position 31.9 | Deploy server-loaded research links; evaluate search queries before changing its broader market purpose. |
| https://walnutmarkets.com/research/nvidia-how-much-of-nvidias-story-is-strategic-investments-versus-core-ai-chip-sales | Indexed; crawled September 18 | Verify financial periods and exact sources; add an operating-business versus investment-exposure table; repair the two broken destinations. |
| https://walnutmarkets.com/research/who-is-buying-nvidia-stock-in-the-latest-13f-filings | Check; overlaps the next question | Compare query intent and evidence with the adjacent brief before publishing another variant. Candidate primary ownership article. |
| https://walnutmarkets.com/research/are-institutions-still-buying-nvidia-stock-after-q2-2026-filings | Check; similar filing period and intent | Retain only if it offers a distinct analysis. Decide consolidation from content/query evidence; no automatic deletion or redirect. |
| https://walnutmarkets.com/research/public-companies-winning-nasa-contracts | Indexed; crawled September 18 | Make reporting period, recipient attribution and contract-value definitions easy to verify; connect to NASA profile. |
| https://walnutmarkets.com/research/nbis-vs-crwv-ai-neoclouds | Indexed; crawled September 10 | Update the existing comparison when new evidence arrives; show like-for-like metrics with dates and caveats. |
| https://walnutmarkets.com/research/who-is-buying-nebius-stock-in-the-latest-sec-filings | Check; September 15 archive entry | Named holder changes and source filings; related link to the existing comparison. |
| https://walnutmarkets.com/research/who-is-buying-applovin-stock-in-the-latest-sec-filings | Check; September 16 archive entry | Review the claims and denominator; shorten repetitive introduction; support additions and reductions with precise filings. |
| https://walnutmarkets.com/research/are-institutions-still-buying-apple-stock-after-the-latest-filings-aapl | Check; September 18 archive entry | Let the new page be discovered; verify evidence and link it from relevant AAPL research. |
| https://app.walnutmarkets.com/departments/nasa | 37 recent impressions; position 53.2 | Link the specific research answer to the underlying contracts page and vice versa. |
| https://app.walnutmarkets.com/departments/department-of-defense | 32 recent impressions; position 66.4 | Identify specific recipient/award questions from GSC before drafting broad generic defense content. |
| https://app.walnutmarkets.com/ticker/NVDA | Relevant product destination; check | Verify research and financial deep links for guests and paid users; retain current access controls. |
| https://walnutmarkets.com/congress-trades | 30 impressions in the three-month report | Evaluate actual queries and connect relevant sourced research to the product workflow. |

The Department of Energy profile is another candidate: 18 recent impressions near position 12.2. Inspect its actual query mix before prioritizing an article; this is a very small sample.

## 3. First editorial revision: NVIDIA investments versus chip sales

Proposed title: **NVIDIA Strategic Investments vs. AI Chip Sales: What Drives the Business?**

Revision instructions for an editor or the existing draft revision workflow:

1. Keep the same canonical article URL. Answer the question in the first paragraph using verified, dated evidence.
2. Check the latest available reporting period and update the article's as-of context honestly. Do not call an older quarter the latest merely because it exists in a cached source packet.
3. Create a comparison table with columns: measure; reported amount; reporting period/date; what it tells us; exact source. Separate operating revenue, operating cash flow, equity-investment carrying value, valuation gains and investment commitments. These are not interchangeable financial measures and should not be summed into a misleading total.
4. Link exact documents. The audit's generic NVIDIA homepage link should point to the release it actually names. Verified primary-source candidates include the [Q1 FY2027 release](https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-first-quarter-fiscal-2027) and the [Q2 FY2027 release filed with the SEC](https://www.sec.gov/Archives/edgar/data/1045810/000104581026000073/q2fy27pr.htm). Check each claim against the document before changing figures. This plan does not validate all article figures.
5. Separate reported financial facts from Walnut's interpretation. Include evidence that would weaken the thesis, without filling missing data with invented estimates.
6. Link Financials and related ticker research using supported routes. Preserve existing paywalls and the article's free/paid scope.
7. Review against the two related ownership briefs; this article's question is business exposure, not another ownership roundup.

## 4. Repeatable daily editorial review

- Does the article answer a distinct investor question that an existing page does not already answer?
- Is the main answer near the top, supported by named entities, dates and original Walnut evidence?
- Are all financial figures, dates and source labels checked against exact primary documents?
- For holdings, does it distinguish quarter-end positions, disclosure dates and actual transaction timing?
- Would updating an existing URL serve readers better than publishing another variant?
- Do the ticker, research and source links reach the destinations described?
- Are authorship, publication dates, assistance disclosure, limitations and paid access represented accurately?
- Does the reader have a relevant next action, such as opening the ticker or saving a watchlist item?

Daily drafting stays useful when there is new evidence. No new article is preferable to a near-duplicate whose only difference is wording. This is an editorial decision; this change does not impose a new automatic daily limit.

## 5. Earned exposure — drafts only

Select recipients based on their actual coverage and community rules. No outreach was sent, no contact list was purchased, and no public post was published.

### Newsletter/editor note

Subject: A sourced NVIDIA research breakdown for your readers

Hi [name] — your coverage of [specific relevant topic] made me think this might be useful. We are preparing a Walnut Markets breakdown that separates NVIDIA's operating business from its investment exposure, with a dated comparison table and direct primary-source links. Once the revised piece is reviewed, I'd be happy to share the table and methodology for your assessment. If you reference it, please link to the source analysis so readers can check the evidence. No obligation to cover it.

Use this only after the revision is complete. Personalize the relevance; do not mass-send placeholder messages.

### Community post outline

Disclose the Walnut affiliation. Lead with a useful, verified finding and one dated table or chart that stands on its own. Explain the biggest limitation. Add a source link only where community rules allow it. Ask a substantive question about the analysis rather than requesting upvotes or making return promises. Prepare the text from the reviewed article, not from unverified generated claims.

## 6. Deployment and measurement checklist

Local validation: 36 focused frontend tests passed (including rendered archive HTML, pagination/canonicals, current public cards, link repairs, byline metadata, related links and the existing paywall branch). Four focused backend route/source tests passed. Python syntax checks passed. Frontend TypeScript checks and the full Next.js production build passed, including all 62 static pages. The production build used a separate QA output directory; its temporary TypeScript configuration changes were restored. One obsolete test assertion for a removed ticker lookup component was updated to the existing contextual CTA, after verifying that the baseline already used that CTA. The entire backend suite and live post-deployment behavior have not been tested in this pass.

After deployment, verify the research archive and page 2 on the canonical marketing host: successful response, rendered article anchors, distinct content and self-canonical. Check that invalid/out-of-range pages are not indexable. Verify the two NVIDIA links open Financials and that paid fields remain gated for guests/Free users. Check the generated article's byline, dates, source links and related links on desktop and mobile.

Do not repeatedly resubmit successful sitemaps. Record a fresh cohort baseline after deployment. At 30 days, review discovery/indexing and relevant impressions. At 60–90 days, compare the same pages/queries for non-brand clicks, top-20/top-10 appearances and verified organic signup/activation events. These are review checkpoints, not guaranteed ranking timelines. No scheduled monitoring automation was created.
