# SEC press collection past individual size holds

October 14, 2026 UTC, October 13 local. Continuation of the FMP replacement goal.

## Current migration evidence

Read-only production checks at06:27UTC find no pending serialized process jobs or database blockers. This does not inspect the separate database enrichment queue; see the later backlog evidence below. Insights0.817s, feed0.455s, news0.235s, research0.578s, fresh AAPL2.605s and NVDA2.438s. This is another healthy sample, not sustained-capacity proof after the earlier portfolio timeout.

Scheduled receipts distinguish incomplete coverage from process crashes: institutional references stage two scopes per bounded run with remaining work, SEC research has unsupported securities such as funds, and Finnhub recommendation identity mismatches remain held. No source guards were relaxed. House remains official, Senate uses the selected guarded community backup, and Form4/earnings releases use SEC. Institutions publication remains disabled; core fundamentals, transcripts and final prices still have migration gates.

Fresh paired preparation audit covers1,751 symbols against1,751 retained FMP rows:1,097 have usable SEC ratios,255 have verified issuer identity but incomplete current revenue coverage,1,134 combine with eligible Finnhub metrics. The audit reports380 symbols absent from the SEC directory,14 missing companyfacts, three missing and two expired SEC caches. Supplemental coverage includes79 missing and112 expired Finnhub caches among the eligible combination cases. No eligible SEC period was older than the available FMP comparator. This is field-presence evidence, not numerical/definition parity. Market-derived metrics and candidate-only SEC leverage are excluded from this audit, so absent values here must not be described as proof that no replacement calculation exists.

Evidence: primary `artifacts/direct-feeds/free-replacements-2026-10-09/{incident-load-1791959229,insights-incident-1791959229,migration-jobs-1791959315,paired-free-coverage-1791959347}.json`.

## Defect and repair

ONDS's production preparation contained one release, one classification hold and an oversized submission `0001213900-26-033852`, which stopped collection before three other selected filings were checked. The size bound concerns an individual filing; it should not suppress independently verifiable releases in the same bounded batch.

[PR628](https://github.com/moore11j/congress-tracker/pull/628), source `c2b65174`, continues after a size refusal within the existing maximum of five filings. Every oversized accession is recorded, with attempted/unchecked counts and incomplete coverage. HTTP refusal/transport and parser failures still stop the issuer; a later failure cannot be hidden by an earlier size hold or overwrite the previous public cache. The byte bound, source identity, filing acceptance, deduplication and publication guards are unchanged.

Only old cache payloads that stopped at a size hold become due. Unaffected cache versions and their six-hour lifetime are retained. No new schedule, global refresh or provider selection.

## Validation and release

127 focused SEC press/materials/store/warming/events/research tests pass on Python3.14.2. New cases cover a leading size hold followed by valid releases, multiple size holds, later parse failure, later429, repeat caching and selective old-cache invalidation. During implementation an obsolete receipt variable and a test comparison against a paginated wrapper were corrected; final suite passes.

Bounded actual official ONDS downloads and isolated SQLite replay prepare four distinct verified releases, with the one oversized source still held. All five selected filings are attempted, four checked, zero classification holds in the isolated fixture, one source-size failure and one unchecked original. Repeat with source transport forbidden returns identical items. Zero production writes, Events or ResearchSourceDocuments. Public-source bytes and hashes are retained in primary `artifacts/onds-press-1791959635/{sources,result}.json` and numbered source files. The current total discovery is eight filings; the existing five-filing selection limit remains explicit.

PR628 merged as `e286018d`; backend workflow `38523406988` completed successfully. The deployed press module SHA256 matches `a8fdda5a10a92c1b2ea3631402f20910a31f7c075f0d3ad98f3484ce4f74de4e`. Vercel preview and secret scan pass. Both dependency audits fail in unchanged packages; no all-CI pass claimed. PR attachment was attempted but the app rejected its 100-attachment limit. At06:43UTC the normal public request had queued existing enrichment job22918 for ONDS, with zero attempts. Public response is temporarily unavailable while the old size-limited cache is due; ordinary live recovery remains pending. Existing source limitations and the larger financial/institutional/transcript/consumer/Massive/global-FMP/billing gates remain open. Previous Outcomes turn made a retained-product access repair; this turn adds current coverage evidence and a concrete replacement-feed repair. Goal remains active.

## Leverage coverage follow-up

Read-only audit at06:38UTC covers1,746 fresh rows:408 have complete current EBITDA components,678 incomplete,651 missing or older leverage versions, eight negative-expense holds and one incompatible-filing hold. Missing component counts overlap:interest510, depreciation/amortization263, tax43, net income47. Complete EBITDA alone does not establish a comparable debt ratio. AAPL lacks interest; GOOGL/MSFT/TSLA lack depreciation/amortization; NVDA and ON have complete EBITDA but no eligible debt candidate. LIN retains the source-defined reported-borrowings candidate1.7242279576309116. These are preparation findings, not activated public values or broad numerical parity. Receipt:primary `artifacts/leverage-readiness-1791959893.json`.

## Ordinary worker receipt and remaining backlog

At 06:48 UTC, the ordinary enrichment worker completed 120 jobs successfully in 44.84 seconds (10.987 CPU seconds), but ONDS job 22918 remains queued with zero attempts. Read-only aggregate evidence finds 2,952 queued enrichment jobs plus one running quote: 1,300 ticker metadata, 860 CIK metadata, 467 quotes and smaller groups, including 26 press jobs. The earlier empty-queue finding concerned the serialized process queue only; it did not establish an empty database enrichment queue. The public ONDS press response remains unavailable (HTTP 200, 0.701 seconds) pending refresh. No production four-release recovery or full reliability claim is made. Next: verify ordinary SEC earnings rotation and address enrichment queue fairness/capacity without adding load or losing high-priority freshness. Receipts: primary `artifacts/press-size-live-1791960502.json`, `artifacts/press-queue-1791960539.json`, and `artifacts/direct-feeds/free-replacements-2026-10-09/scheduled-job-cpu-1791960488.json`.

## Ordinary production recovery, 07:09 UTC

The ordinary SEC earnings run at 07:07:32 UTC completes all five planned scopes. ONDS now prepares three distinct releases instead of one, attempts all five selected filings, checks four, and retains one oversized original plus one existing-material reconciliation hold. The fourth isolated replay release, accession `0001193125-26-347973`, is held in production as `ambiguous_existing_material` with three existing candidates. That explains the isolated/live difference; no guard was relaxed. The previously visible release keeps its canonical key and source hash; two additional releases have distinct keys and hashes. Public response is partial/HTTP 200 in 0.743 seconds with an identical repeated item array. The scheduled receipt records zero canonical writes, emails and model calls. Demand queue job22918 remains pending, but the independent ordinary preparation has restored the public cache. Receipt: primary `artifacts/press-size-live-1791961754.json`. This verifies bounded recovery, not complete coverage of all eight discovered filings.
