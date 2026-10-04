# Research writing and video direction — October 4, 2026

Status: deployed October 4 as `fdafb86fd855552ebc897b6da64397d098c93c1c`, built against `c7bb0b56`. The owner approved the prior creative proposal and requested a better research model, stronger finance writing, current search relevance and useful demonstrations of Walnut's native data.

Release verification: Vercel succeeded and both public domains reported the release SHA. Fly workflow [37240643427](https://github.com/moore11j/congress-tracker/actions/runs/37240643427) succeeded; all four machines run the release image. Readiness and anonymous Premium-route denial checks passed. API/cron/video effective configuration returns Sol and the v9 prompt; authenticated read-only Sol model access returned HTTP 200. No content generation, new voice selection or publication was performed in this rollout. A documentation receipt may subsequently advance the frontend revision.

## What the evidence says

The writer default was `gpt-5.4-mini`. Manual editing examples, institution-name normalization, named-buyer validation and topic-family diversity already existed. They were not model training, and a repeated template could still produce generic writing. Recent live daily history already showed Congress, insider, stock-analysis and contract topics; do not characterize the current scheduler as exclusively generating 13F articles.

Reviewed finance examples:

- Nick Maggiulli's [Just Keep Buying](https://ofdollarsanddata.com/just-keep-buying/) starts with one memorable proposition, addresses fear of buying at a peak, uses familiar comparisons and data graphics, then discusses the psychological objection. Transfer: one reader problem, evidence and a real objection. Do not copy categorical investment advice or the author's voice.
- Morgan Housel's [The Psychology of Money](https://collabfund.com/blog/the-psychology-of-money/) opens with contrasting outcomes and explains their significance before broadening the argument. Transfer: a concrete contrast followed by interpretation, rather than a metric list. Do not invent biographical anecdotes for Walnut.
- The author's [popular-posts index](https://ofdollarsanddata.com/popular-posts/) and [self-reported audience history](https://www.linkedin.com/posts/nickmaggiulli_my-blog-ofdollarsanddatacom-pageviews-activity-7279528833391706112-peE-) support studying this publication. They do not prove an individual device caused virality or that Walnut will reproduce its reach.

Live Walnut admin Keyword Planner evidence, inspected October 4, US/English/Google Search:

| Phrase | Approximate average monthly searches |
|---|---:|
| stock screener | 14,800 |
| microsoft ownership | 6,600 |
| nvidia valuation | 5,400 |
| microsoft stock analysis | 480 |
| boeing contracts | 480 |
| nvidia free cash flow | 320 |
| asml valuation | 260 |
| nebius stock analysis | 90 |
| who is buying nvidia stock | 20 |

These are historical estimates including close variants, not current-day searches, unique users, guaranteed traffic or organic keyword difficulty. The latest saved Keyword Planner sync was October 4; a bounded manual lookup refreshed additional phrases. Search Console's latest sync covered finalized September 4–October 1 data. Its sparse impressions do not justify a reliable topic-level conversion forecast. No advertising campaign or budget changed.

The practical implication is to explore valuation, earnings/cash conversion, contracts, ownership, insider and Congress angles according to native evidence and the reader's intent. “Microsoft ownership” cannot be relabeled “who is buying” simply to borrow its volume. A short measured query can support a specific article question; the title need not be a 25-word keyword bundle. Broad “stock screener” demand belongs primarily to the product workflow, not an unrelated ticker brief.

## Implementation

- Research default and admin fallback: `gpt-6.1-sol`; mini remains an explicit budget choice. Existing environment overrides and saved explicit selections are honored. No silent model fallback. Drafts, revisions and default discovery use low reasoning with existing output bounds.
- Shared editorial contract for drafting and repair: direct answer with a decisive fact; observation → interpretation → limitation; relevant counterevidence; an observable condition that could change the conclusion; specific headings and varied sentence rhythm. Thin evidence does not justify padding. Existing claim/source, naming, entitlement and buyer-detail guards remain.
- Approved navigation evidence now explicitly includes ticker Financials, Ownership and Research hash links, verified against the frontend tab parser. Ask for one or two practical sentences explaining what to compare, without inventing access, alerts or trial promises. The article must answer the reader without requiring signup.
- Keyword discovery asks for a dated catalyst or evergreen problem, the needed Walnut dataset and a useful product check. Annual averages/crawl dates cannot establish a current trend. Planning now receives a compact comparison of the latest three completed months with the preceding three; missing, stale, gapped and zero-base histories are handled explicitly. Historical monthly estimates are not live search interest. Diversity and original-evidence gates remain unchanged.
- New daily research videos use version 3: one bounded Sol selection from eligible exact published excerpts and three permitted hooks; show the finding before navigation; demonstrate ticker → Research → brief; link the analysis in the caption. The model cannot introduce financial prose or figures. Missing key/provider failure yields a documented deterministic selection without automatic retries. Existing creative budgets apply. The call is capped at 900 output tokens; input is at most eight short excerpts plus metadata.
- Old version 1/2 video boards remain reproducible. Feature tutorials retain their previous flow; existing approvals, renders and scheduled posts are not modified. The production voice, presenter and render design are not replaced by a text-model change.

Model and cost sources: [official Sol model documentation](https://developers.openai.com/api/docs/models/gpt-6.1-sol), [migration guide](https://developers.openai.com/api/docs/guides/latest-model/gpt-6-astra.md#migration-quickstart). The usage estimator now recognizes Sol's standard $2/M input, $0.10/M cached input and $10/M output token rates. An illustrative 12,000 input + 3,000 output tokens is $0.054 before search, cache-write adjustments or repairs. This is an estimate, not an observed draft invoice. Production account model access and effective defaults were verified during deployment; actual generated-output quality remains a follow-up.

## Voice comparison

Two completed, voice-only review samples use the same approximately 200-character script about price versus business performance and the Financials workflow. Generated through the connected Runway workspace using 10 existing free credits; no subscription or purchase. Lara is 11.12 seconds (task `1848e8d5-bdff-4d58-ae91-0ba257aa3533`); Maya is 12.64 seconds (task `1a3b5b5a-6b40-4ab5-b78b-35bbee41b3c9`). These are audition outputs, not a verified naturalness improvement or a selected production voice. Do not dub a different script over Grace's existing mouth movements. Signed asset URLs are intentionally omitted from repository memory.

## Checks and release boundary

- Python 3.14.2 with isolated SQLite and workspace pytest temporary directories: 108 focused editorial, keyword, scheduler, creative, navigation and social-layout tests passed.
- Broader research run: 137 passed, seven failed. All seven failures reproduced against an isolated archive of unchanged `c7bb0b56`: schema test double missing `bind`; quality-repair wording expectation; campaign ticker replanning; NBIS revision identity expectation; external source revenue expectation; CXW fallback multiple; cash-flow period derivation. These require separate diagnosis and are not a full-suite pass.
- Frontend: six of seven selected static tests passed; the unchanged public archive test expects the removed `getPublishedResearchBriefs` helper. Its inputs and test are unchanged from HEAD. TypeScript `tsc --noEmit --incremental false` passed.
- During implementation, new audition media generation succeeded at the provider. No full listening review, live Sol-generated brief evaluation, full end-to-end video render or conversion experiment was performed. Commit/deployment subsequently completed as recorded above; no email or social publication occurred.

Before declaring production quality improved: review three diverse native-data briefs for factual accuracy and unnecessary edits, render one new research video, listen to both auditions, and inspect mobile captions/navigation/presenter crop. Measure editing effort, qualified product clicks, saves/signups and paid conversions with the existing analytics safeguards. Keep the separate retained-Zeely crop repair open.
