# October growth execution and four-week cohort

Owner approved all five recommendations from the [October 5 audit](seo-analytics-audit-2026-10-05.md). Work began October 5 Pacific; some production timestamps are October 6 UTC. Scope: measurement, ten-page SEO cohort, stronger existing research, owned Reddit distribution and weekly observation. Keep the homepage hero, pricing and entitlements unchanged.

## Measurement baseline and limits

The live admin report, with **Include admin unchecked**, was read for its rolling 30-day window on October 5 evening Pacific. This is not the same window as GA4 September 7–October 4 or GSC September 6–October 3.

| First-party measure | Observed |
|---|---:|
| Eligible external accounts currently | 19 |
| New accounts in the 30-day window | 3 |
| Email-verified new accounts | 3 |
| Active external accounts | 4 |
| Live paying accounts | 0 |
| Positive live paid invoices | 0 |
| Test invoices / zero-payment invoices / unknown live mode | 0 / 4 / 0 |
| Signup-completed telemetry events | 1 |
| Upgrade clicks | 4, across 2 sessions |
| Checkout starts / completions | 0 / 0 |

Two new accounts have a qualifying acquisition session (one direct, one Google organic); one lacks that link. Both attributed accounts returned; neither saved/followed in the observed funnel. Google organic had 44 sessions and four stock-research sessions; Reddit had 34 and four respectively, with no attributed new account. Known signed-in/admin exclusion is not complete anonymous-owner or bot detection. Large direct traffic must not be treated as verified humans.

Production reporting says paid forwarding is enabled and GA4/HeyCatch configuration is present. Google-login unwanted-referral exclusion already includes `accounts.google.com`; it was verified, not recreated. Existing first-party acquisition tests preserve shared consented cookies across hosts/authentication. No customer identity, credential or payment was manufactured for this check. Provider ingestion of the next real consented payment remains unverified because there are no positive live payments to reconcile.

Created event-scoped GA4 custom dimension **Internal activity**, parameter `is_internal`. Release `1ebd92dc` sends `true`, `false` or `unknown` on Walnut manual pageviews/product events, preserves unknown identity, overrides caller-supplied classification and checks analytics consent. This does not backfill old data or classify every GA automatic event. No irreversible exclusion filter was applied. Use first-party records as the signup/payment authority and label GA's consented subset separately. Recheck the custom dimension after processing; configuration alone is not proof of receipt.

## Fixed ten-page cohort

Public HTTP audit on October 5/6: all ten returned 200 without a redirect, one self-canonical, no blocking meta/header/robots directive, and canonical sitemap membership. NBIS comparison omits a robots tag, which permits indexing by default. Script: [audit_growth_cohort.py](../backend/scripts/audit_growth_cohort.py). Public-only receipt: `artifacts/growth-cohort-2026-10-05.json` (local, ignored).

| Canonical URL | GSC individual inspection on October 5 |
|---|---|
| https://walnutmarkets.com/insider-trading-tracker | Indexed |
| https://walnutmarkets.com/stock-analysis-platform | Indexed |
| https://app.walnutmarkets.com/ticker/NVDA | Indexed; last recorded crawl August 6 |
| https://app.walnutmarkets.com/ticker/ANET | Indexed |
| https://app.walnutmarkets.com/ticker/NBIS | Indexed |
| https://app.walnutmarkets.com/ticker/ALV | Indexed |
| https://app.walnutmarkets.com/departments/nasa | Indexed; last recorded crawl August 19 |
| https://app.walnutmarkets.com/departments/department-of-energy | Indexed |
| https://walnutmarkets.com/research/who-is-buying-nvidia-stock-in-the-latest-13f-filings | Unknown/no recorded crawl; fresh live test eligible and corrected table present in Google's rendered HTML |
| https://walnutmarkets.com/research/nbis-vs-crwv-ai-neoclouds | Indexed |

Requested indexing once for the corrected NVIDIA buyers brief and once for the materially updated NBIS/CRWV comparison; both requests accepted. Acceptance is not indexing or ranking. No repetitive sitemap resubmissions. Nine of ten priority URLs already being indexed argues against treating bulk submission as the whole growth strategy.

Research hub now links directly to the reviewed NVIDIA comparison, NBIS/CRWV and NASA evidence. `/explore` links to all ten canonical destinations. Corrected stale commercial-page sitemap dates using September 30 source history. Public research cards now include `updatedAt` independently of `publishedAt`, so future saved corrections reach the existing sitemap last-modified resolver. NASA and the overlapping NVIDIA article report October 6 UTC updates while retaining their original publication dates.

## Existing research, not more duplicates

- NBIS/CRWV retains its Q1 and July 23 numbers. Added Financials → Ownership → Research instructions, a dated snapshot warning and questions about cash, financing, guidance and backlog. No October valuation refresh is implied.
- NASA brief explicitly names its August 31 snapshot, distinguishes historical award totals from revenue/backlog, removes the unsupported blanket assertion that an omitted contractor is private, and adds a department-record-to-ticker workflow. Preserved figures and publication date.
- Found a newer overlapping NVIDIA article reintroducing an Amundi exit and an unsupported top-three-buyer ranking despite the September 27 reviewed correction. Replaced those claims with a transparent withdrawal and the reviewed four-filer comparison, linking to its original SEC sources. A missing position in a NEW HOLDINGS supplement is not an exit; market-value change is not buying cash flow. The comparison is not a complete institutional ranking.
- Body/card edits left separate public subtitle/sidebar claims unchanged. Follow-up `9d1cb490` exposes a collapsed **Headline and sidebar corrections** panel (subtitle, search description, catalysts, risks and watch items) so factual corrections can cover all visible surfaces. Used the deployed panel to align NASA's subtitle/search description and both NVIDIA corrections, including sidebar claims. Access controls are unchanged.
- The older `are-institutions-still-buying-nvidia-stock-after-q2-2026-filings` article also withdrew its Amundi exit, approximately $74 billion T. Rowe Price reduction and inconsistent count-based net-accumulation conclusions. It now explains the interpretation problem and links to the reviewed comparison rather than publishing another competing dataset. Both corrected NVIDIA pages verified publicly; original public bodies saved locally for correction history.

The review also found other-ticker institutional briefs (including APP, AAPL and MSFT) with potentially related amendment/holder-normalization problems. Do not interpret this targeted work as a full editorial audit or promote unverified aggregate exit/ranking claims. Source reconciliation in the generator and review of the wider backlog remain priority follow-ups; changing models alone is not sufficient. Existing automatic validation still flags some style/keyword/readiness requirements on these historical drafts; this is not a claim that all generator validators pass.

## Owned Reddit distribution

Saved and verified concise, plain-text links in three existing r/walnutmarkets posts. Original commentary and source links retained; no new post or paid promotion.

| Post | Destination | `utm_content` |
|---|---|---|
| [Power and financing](https://www.reddit.com/r/walnutmarkets/comments/1wpi583/ais_next_bottleneck_might_not_be_memory_or_chips/) | NBIS/CRWV dated comparison | `power_financing_1wpi583` |
| [Risk-off day / NVIDIA](https://www.reddit.com/r/walnutmarkets/comments/1wsuvps/today_finally_looked_like_a_real_riskoff_day/) | Reviewed four-filer NVIDIA comparison; explicitly not same-day buying | `nvda_riskoff_1wsuvps` |
| [META disclosure context](https://www.reddit.com/r/walnutmarkets/comments/1wuhaad/zuckerberg_sold_214m_of_meta_the_same_day_he_was/) | Insider tracker | `meta_disclosure_1wuhaad` |

All use `utm_source=reddit&utm_medium=organic_social&utm_campaign=october_research`. No bold promotional paragraph or “tool I'm building” introduction. Saved screenshot receipts under `artifacts/growth-october/` are local-only; public post links provide durable verification.

## Four-week observation

Created active thread heartbeat `walnut-four-week-growth-review`: October 12, 19, 26 and November 2, 10:00 America/Los_Angeles. Read-only monitoring; no automatic deployment, article/post edits, repeated indexing requests, emails or spending. Notify on meaningful progress/regression/blockers and at final assessment, not unchanged state. Chrome/account availability may require an owner reconnect; missing data is not zero.

Use equal complete windows, preserving these baselines:

- GSC September 6–October 3: 7 clicks, 182 impressions, 3.8% CTR, 36.1 average position. Prior 28 days: 2, 285, 0.7%, 49.7. Latest complete week September 27–October 3: 3 clicks/47 impressions/28.3 position. Report branded/nonbranded known queries separately; anonymized queries prevent a complete partition.
- GA4 September 7–October 4: 240 active users, 418 sessions, 206 engaged sessions, 49.28% engagement and one recorded signup. Historical all-user figures include admin activity; do not compare them uncritically with the new classification.
- Cohort inspection: nine indexed, one unknown/eligible/requested. Watch fresh crawl dates and query impressions on these same ten pages rather than adding thousands of new targets.
- Track tagged landing → stock/evidence view → verified signup → first save/follow → return → checkout → verified positive live payment. Report numerator, denominator, date window and consent/identity gaps.

Decision rule: if fresh crawls improve but relevant impressions remain flat, improve query fit and original evidence. If qualified visits increase but signup/save does not, repair that journey. If signups activate but do not return/pay, investigate recurring value and offer. Four weeks is a diagnostic interval, not a promise of Google rankings.

## Checks and release

- 36 focused frontend analytics/funnel/consent/sitemap tests passed; TypeScript passed; production build passed at `1ebd92dc`.
- One focused backend legacy-card regression passed with separate publication/update dates, Python 3.14.2 and isolated local temporary data. The installed 3.12 environments reference a missing interpreter; no Python 3.12 local test claim. Initial pytest temp-directory permission failure resolved with a workspace-local test directory.
- `1ebd92dc` pushed; Vercel success and both public version endpoints verified. [Fly workflow 37406217867](https://github.com/moore11j/congress-tracker/actions/runs/37406217867) succeeded; all four machines run the release image and readiness/database pass. Public cards expose correct update dates.
- `9d1cb490` is a frontend-only editor follow-up; TypeScript passed, Vercel succeeded and both public version endpoints verified the exact release. Live panel saves and resulting public subtitle/sidebar updates verified. Backend remains `1ebd92dc`. The research hub, directory links and dated NBIS workflow were verified in Chrome; NASA/NVIDIA sitemap dates were independently confirmed after backend rollout.
- Unrelated strategy/forward-study work and shared memory edits preserved. No full-suite claim, new spend or email delivery.
