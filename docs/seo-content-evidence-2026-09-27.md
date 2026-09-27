# Walnut SEO content and indexing audit — September 27, 2026

This is a bounded, read-only audit of seven priority pages, using signed-in Google Search Console, anonymous production HTML, the live sitemaps, source code, and competing public pages. No production code, entitlements, articles, or homepage copy were changed.

## Conclusion

The evidence supports a combination of incomplete discovery/recrawling and weak performance on pages that are already indexed. It does not establish an AI-content penalty, a YMYL sandbox, or a fixed ten-impression ceiling. Removing noindex was necessary but does not by itself make the pages competitive.

The strongest new technical finding is that Google's **live rendered NVDA page cannot load the Congress and insider activity tables**: their client-side `/api/events` requests are blocked by robots.txt. Google's stored NVDA page is also an old, very short snapshot, not the richer page currently served. The most actionable content gap is that the insider-tracker landing page describes a tracker without displaying transactions. Research freshness and claim-level evidence also need work.

## Search Console evidence

Observed September 27. The performance report covers **June 25–September 24, 2026**, so it cannot measure the September 25–26 releases yet. Totals: **8 clicks, 510 impressions, 1.6% CTR, average position 43.2**. All reported clicks went to the marketing homepage (7) and app homepage (1). Page-level impressions should not be summed against the property-level total because aggregation differs.

Selected reported landing pages:

| Page | Clicks | Impressions |
|---|---:|---:|
| Marketing homepage | 7 | 107 |
| App homepage | 1 | 26 |
| App Insights | 0 | 79 |
| ALV ticker | 0 | 67 |
| ANET ticker | 0 | 37 |
| NASA department | 0 | 37 |
| Defense department | 0 | 33 |
| Congress trades landing page | 0 | 30 |
| Insider trading tracker | 0 | 20 |
| NBIS–CRWV research, marketing canonical | 0 | 5 |
| NVDA ticker | 0 | 4 |
| Boeing contracts research | 0 | 2 |

The seven-page URL Inspection sample:

| Canonical URL | Current Google Index report | Anonymous HTTP/metadata | Sitemap membership |
|---|---|---|---|
| https://app.walnutmarkets.com/ticker/CART | URL unknown to Google; no recorded crawl | 200; index,follow; one self-canonical | **Absent** from ticker sitemap |
| https://app.walnutmarkets.com/ticker/NVDA | Indexed | 200; index,follow; one self-canonical | Ticker sitemap |
| https://walnutmarkets.com/insider-trading-tracker | Indexed | 200; index,follow; one self-canonical | Marketing sitemap |
| https://walnutmarkets.com/research/who-is-buying-nvidia-stock-in-the-latest-13f-filings | Discovered, currently not indexed; no recorded crawl | 200; index,follow; one self-canonical | App-hosted research sitemap, containing the marketing canonical |
| https://walnutmarkets.com/research/nbis-vs-crwv-ai-neoclouds | Indexed | 200; no restrictive robots tag/header; one self-canonical | Marketing and research sitemaps |
| https://walnutmarkets.com/research/boeing-government-contract-backlog-ba-stock | Indexed | 200; index,follow; one self-canonical | Research sitemap |
| https://walnutmarkets.com/institutional-activity-tracker | Indexed | 200; index,follow; one self-canonical | Marketing sitemap |

All seven return directly to their requested URLs. None returned an X-Robots-Tag restriction. An absent robots meta tag, as on the comparison article, is not a noindex problem. The institutional tracker has progressed from its September 21 discovered state to indexed.

Google reports no referring sitemap for the NVIDIA buyers article even though the current research sitemap includes it. That is a difference between Google's recorded discovery information and today's XML, not proof the XML is broken. No duplicate indexing request was made for that article; its September 21 request was already accepted.

### What Google actually stored for NVDA

URL Inspection reports the last crawl as **August 6, 2026, 19:36:21**, Googlebot smartphone. The crawled-page HTML contains:

- A “Ticker Research Snapshot” heading and NVIDIA company identity.
- A generic description of Walnut's stored market/disclosure research.
- One stored closing price dated August 6.
- Counts of seven Congress items and four insider items, without the underlying transaction rows.
- One contextual comparison link, NVDA versus MU.

It is a very short public snapshot. The old inspection also says no user-declared canonical; today's anonymous HTML has the correct canonical. This demonstrates delayed recrawling of a materially changed page. It does **not** establish why Google has not revisited it or prove that the old content is the sole ranking problem.

### Live Google render: confirmed missing public activity

A fresh GSC live test completed September 27 at 09:47. Google reported **HTTP 200, URL available to Google, page can be indexed**. Its tested HTML contains the richer ticker interface and summary cards, but also says:

- “Congress activity is temporarily unavailable.”
- “Insider activity is temporarily unavailable.”
- Top Congress traders and top insiders are temporarily unavailable.
- Research Memory access could not be checked (an account feature, not a reason to expose private data).

The resource report lists 12 of 37 resources failing. Six are `/api/events` requests for NVDA Congress/insider activity, with `recent_days=365`, limits 20/100 and price enrichment 0/1. Each is explicitly marked **Googlebot blocked by robots.txt**. Other failures are auth/entitlements/app-version requests, analytics, and a HeyCatch tracing script; those are not all SEO defects and should not all be unblocked.

This establishes a real rendered-content gap. It does not prove that it causes the entire site's impression count. The preferred repair is a reliable server-rendered preview of already-public activity from stored data, with the same entitled content for anonymous humans and Google. Avoid a blanket `/api/` robots allowance or paid-data exposure. Verify client hydration does not replace a useful server-rendered preview with an unavailable state when Google cannot fetch the API.

## Public content findings

### 1. CART discovery still depends on snapshot coverage

The current ticker sitemap returns 200 and lists 1,079 URLs, including NVDA but not CART. `frontend/app/sitemap-tickers.xml/route.ts` builds its list from `getSeoSnapshotIndex("ticker")`. The backend list filters snapshots to `indexable=True`. Correcting the ticker page robots setting did not change this separate sitemap mechanism.

**Next implementation:** repair/backfill the cached public snapshot inventory for valid ticker pages with existing data, add CART, and check for other eligible omissions. Keep crawler requests cache-backed. Do not list fabricated/unknown symbols or trigger live provider work on each crawl. Verify exact sitemap membership after deployment, then request CART indexing once. Do not add noindex back to valid ticker pages.

### 2. The insider-tracker page does not yet satisfy the tracker promise

The anonymous page explains Form 4, terminology, limitations, and Walnut's workflow, but contains no actual insider transaction examples. It has 20 recorded impressions and no clicks in the selected period. That does not prove conversion behavior, but it establishes that indexing alone is not sufficient.

**Next implementation:** add a compact, server-rendered table of real transactions already permitted by public entitlements. Include company/ticker, insider and role, purchase/sale/other classification, transaction date, filing date, and a source filing link. Include a clear data-as-of label, meaningful empty/unavailable states, and direct ticker links. Keep existing paid analysis and filters gated. The public preview should be equally available to visitors and crawlers.

The institutional tracker similarly explains the workflow rather than answering a specific ownership question. Its recent contextual link to the NVIDIA article is useful; strengthen the destination article before expanding the landing page.

### 3. Research claims need traceable evidence and explicit time periods

The NVIDIA buyers article gives a direct answer and names managers, but its linked sources are largely NVIDIA company facts, an NVIDIA results page, a general EDGAR search, and Walnut's ticker page. There are **no direct manager 13F accession/information-table links** for the named additions, reductions, and exits. NVIDIA company XBRL facts do not substantiate another manager's holdings change.

The text mixes a Q2 2026 group with a Q4 2025 group and calls holder-count breadth “net accumulation.” Counts of managers adding versus reducing are not the same as net share accumulation or net money flow. It also describes an approximately $74B reduction without a visible prior/current share bridge. This audit does not prove that number is false; the page does not provide enough evidence to reproduce it.

**Next implementation:** verify the named examples against the actual filings; show manager, report period, prior/current shares, share change, filing date, and source link. Distinguish holding value from value sold and breadth from aggregate share changes. Label the dataset's coverage and any incomplete comparison. Correct unsupported statements rather than merely changing their wording.

The Boeing article includes one specific USAspending award link, which is stronger evidence. Its claims about eight awards totaling $1.55B and “funded” work still need a concise supporting award table identifying obligations, award ceilings/modifications, dates, and avoiding double counting. Do not infer recognized revenue from contract value.

### 4. The NBIS–CRWV article has useful analysis but an old snapshot

It includes an actual comparison table, bull/bear cases, source links, and ticker CTAs. It explicitly says market data was queried July 23 with quotes through July 22; its financial comparison uses Q1 2026. Phrases such as “currently” and “latest” can therefore imply more freshness than the dated snapshot supports in late September.

**Next implementation:** update the existing URL using verified available company releases and refreshed Walnut evidence, or clearly label the article as a historical July comparison throughout. Preserve original publication date; add a truthful reviewed/updated date only after substantive verification. Do not simply advance sitemap lastmod.

### 5. Repair the activity rendering dependency

Today's raw NVDA HTML contains zero-event Congress/insider table summaries alongside cached cards reporting eight Congress trades and twenty net insider sells. The live Google render subsequently identifies the tables as unavailable, and its resource report confirms blocked activity API requests. The table requests use 365 days whereas summary cards use 30 days; their counts are not directly comparable. Source code also has fetch-error paths returning empty event responses. The exact server-side path that produced the initial empty response still needs tracing.

**Next implementation:** make the initial public HTML include the permitted activity rows from stored data and preserve that preview if client revalidation fails. Check matching windows and as-of dates. Use an unavailable/stale state when a fetch fails rather than genuine-zero wording. Do not invent rows or unlock paid data. Raw HTML text includes CSS-gated elements, so this extraction is not an entitlement audit and does not establish that every extracted number is visibly public.

## Competing pages: useful patterns, not proven ranking causes

- [SECForm4 insider purchases](https://www.secform4.com/all-buys) provides transaction-level columns and filing links. Its accessible sample is explicitly six months delayed; do not copy its real-time framing into a free Walnut preview. The transferable pattern is a useful, transparent data sample with a paid upgrade.
- [FinanceCharts NVIDIA ownership](https://www.financecharts.com/stocks/NVDA/ownership) presents an immediate holder table with period, shares, changes, ownership, and value. Walnut's specific article can differentiate by explaining changes and evidence rather than repeating a generic ownership overview.
- [Stock Analysis CRWV versus NBIS](https://stockanalysis.com/stocks/compare/crwv-vs-nbis/) provides side-by-side metrics and performance. Walnut already has a more narrative comparison; maintaining its evidence and timing is the higher priority than adding more paragraphs.

These are competing public resources found during research, not verified Google-US rank positions. Their financial figures were not used to replace Walnut's data.

## Priority and measurement

1. **Rendering and discovery reliability:** first make public activity rows survive Google's render without blocked client API dependencies, then repair ticker sitemap omissions. Validate in GSC's tested HTML before requesting a recrawl of the changed pages.
2. **Research evidence:** verify and correct the NVIDIA institutional article; refresh or consistently date NBIS–CRWV. Add reproducible supporting rows to Boeing where needed.
3. **Useful insider landing page:** show an entitled, cached transaction preview that immediately demonstrates the product, with a direct next step into a ticker.
4. **Distribution:** send relevant readers to the exact useful article/ticker, rather than a generic homepage; measure the path to signup and paid conversion. No posts were edited during this audit.
5. **Evaluate a fixed cohort:** retain these seven pages and the September 25 agency cohort. Check discovery/indexing and crawl dates first, then compare equivalent 28-day page/query impressions, clicks, engaged visits, signups, and paid conversions. This is an evaluation window, not a promise of ranking improvement within 28 days. Do not change the hero during the observation period.

Google's guidance emphasizes original, useful, well-sourced content and asks whether pages add substantial value compared with alternatives: [helpful content guidance](https://developers.google.com/search/docs/fundamentals/creating-helpful-content). Recrawl requests can take days to weeks and do not guarantee inclusion; repeating the same request does not accelerate crawling: [recrawl guidance](https://developers.google.com/search/docs/crawling-indexing/ask-google-to-recrawl).

## Evidence and limits

- Public fetch script and results: `artifacts/seo-content-audit-2026-09-27/audit.py` and `public-audit.json`; per-page extracted text beside them.
- Googlebot-labelled and browser-labelled anonymous fetches of CART/NVDA had equivalent substantive content; differences were title streaming/placement. Spoofing a user agent is not the same as a verified Google crawler. The GSC stored-crawl viewer provides the actual historical Google evidence.
- Three sampled sitemaps returned 200: marketing (32 entries), tickers (1,079), research (34). This was not a complete 4,000-page crawl.
- The web research fetch tool returned 403 for some Walnut URLs; independent anonymous HTTP checks returned 200. That tool-specific result is not evidence that Googlebot is blocked.
- Current GSC URL statuses are recorded above. The performance report ends September 24, before the latest technical/content releases.
- Prior keyword-demand ranges remain in `docs/keyword-indexing-priorities-2026-09-21.md`; no new Google Ads volumes were inferred from competitors.
- The audit itself made no production deployment or paid subscription change.

## Approved implementation — September 27

- Root cause confirmed: `is_inactive_logged_out_api_request` intentionally returned an empty events payload for anonymous ticker SSR. A new bounded `/api/public/activity` route reads persisted public Congress/Form 4 disclosures without providers, enrichment, scores, outcomes or institutional data. Anonymous ticker HTML now uses this preview; authenticated recovery and entitlements remain intact. No robots API allowance was added.
- Insider tracker gains a cached server-rendered five-row sample with traded/filed dates, officer role, filing links and direct ticker links. Empty and unavailable states are distinct.
- Ticker snapshot candidates now include named, priced companies without an index membership or disclosure event; existing snapshots are excluded before the batch limit. This addresses CART and candidate starvation. Sitemap cache lifetime is aligned to its 30-minute refresh.
- NBIS–CRWV explicitly identifies the historical July 23 comparison, Q1 financials and July 22 prices. September 27 reflects a substantive date/context correction, not refreshed financial figures.
- NVIDIA correction is prepared as an idempotent, explicitly applied maintenance job. It preserves publication date, canonical slug and access settings, backs up the original payload, and uses an optimistic concurrency guard. It does not publish social posts or trigger a new publication transition.

### NVIDIA SEC verification

Common-stock CUSIP 67066G104, excluding put/call rows; sum matching rows within each original information table. These four filers are a focused sample, not a market-wide net-flow measure.

| Filer | March 31 shares | June 30 shares | Change |
| --- | ---: | ---: | ---: |
| FMR LLC | 993,852,968 | 1,026,051,548 | +32,198,580 |
| Amundi | 133,768,018 | 129,523,550 | -4,244,468 |
| Price T Rowe Associates | 370,102,688 | 369,829,781 | -272,907 |
| T. Rowe Price Investment Management | 19,437,830 | 16,865,766 | -2,572,064 |

Each quarter's official information-table URL is retained in `backend/app/jobs/data/nvda_ownership_correction_20260927.json`. Downloaded XML and calculation evidence are in the ignored audit artifact directory. Amundi amendment 0001172661-26-004047 is explicitly **NEW HOLDINGS**, not a replacement; absence of NVDA from that supplement cannot establish an exit. The correction withdraws unsupported holder rankings, mixed-period net accumulation, the Situational Awareness exit, stale earnings assertions and the misuse of holding value as value sold.

### Pre-deployment validation

- Next production build completed; TypeScript checked.
- 16 targeted frontend SEO tests passed.
- Public preview, sitemap discovery and correction-preservation regression tests passed.
- An unrelated existing institutional test fixture passes `object()` as a database session; the unchanged institution route calls `.get()` and fails. That test was excluded from the focused inactive-SSR rerun; other guard tests pass. No unrelated institution behavior was altered.

### Production deployment and live validation

- Deployed commits `91a2dc23`, `81e91697`, and `06e785d5`. The frontend reported `06e785d52b17a6de27c0e0309d108354134428a5` through its live version endpoint. Fly's existing four machines are running the updated backend; application health checks pass. No compute plan or subscription changed.
- Live anonymous requests returned HTTP 200 for NVDA, CART, the insider tracker, the corrected NVIDIA article, and NBIS–CRWV. Each has its expected self-referencing canonical and no `noindex` header or meta directive.
- NVIDIA's initial HTML now includes named insider rows. Production verification caught provider identity fields nested in stored payloads; the follow-up normalization fix exposes only the explicitly permitted public fields and has a regression test.
- The insider tracker shows five dated transaction rows with five direct SEC filing links. The NVIDIA article displays all nine cited SEC sources; the old six-source display cap was removed.
- The NVIDIA correction was applied at `2026-09-27T17:16:41.587633+00:00`. Its original publication date, slug and access settings were preserved. The original payload is backed up on the backend volume at `/data/editorial-backups/rb_1789221682029_a265ac-d72578ec35cbc271.json`.
- Google's actual live URL Inspection test for NVDA at 10:21 Pacific on September 27 reported **URL is available to Google / Page can be indexed**. Tested HTML contains both activity tables, including Timothy S Teter and Mark A Stevens, without the unavailable state. It also retains the Premium score restriction.
- CART's fresh Google live test at 10:24 Pacific also reported **URL is available to Google / Page can be indexed**. This establishes live eligibility, not that Google has added the URL to its index.
- The NVDA indexing request was rejected with **Quota exceeded** for the account's daily submissions. No new request was accepted, and no further manual submissions were attempted after that response. The existing submitted sitemap index remains the discovery route; repeated resubmission is unnecessary.
- CART is now present in the live ticker sitemap. Its snapshot plus 250 additional missing ticker snapshots were committed. The first batch committed 83 entries before the rolling deployment disconnected its session; the resumed batch completed the remaining 167 with `status: ok`, `failed: 0`. Existing nightly batches continue filling eligible stored-data pages.
- Final freshly generated sitemap XML returns HTTP 200 with **1,330 URLs**, up from 1,079, including CART and no `www` entries. At the same check, the normal cached sitemap URL still returned its earlier valid 1,263-entry response, also including CART. The final 67 additions will appear there after the existing cache expires; this is cache propagation, not a failed backfill. Both responses are recorded in `artifacts/seo-content-audit-2026-09-27/final-sitemap-validation.json`.
- Additional newly discovered ticker spot checks (PHIN, IBEX and SATL) returned HTTP 200, a single self-referencing canonical, `index, follow`, and no `X-Robots-Tag` restriction.
- The homepage hero and paid-access rules remain unchanged. This deployment fixes observed crawl/render and evidence problems; it does not establish a ranking recovery or a guaranteed indexing date.

Live fetch evidence is saved in `artifacts/seo-content-audit-2026-09-27/release-validation.json` and the accompanying `*-after.html` / `*-after.txt` files. The Google-tested HTML was inspected separately in Search Console.
