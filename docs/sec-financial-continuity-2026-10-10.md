# Reviewed Exxon financial continuity, October 10

ExxonMobil's new SEC registrant, CIK2115436, currently reports quarterly facts without the annual history needed for trailing calculations. The predecessor CIK34088 holds that history. The successor's [filed Q2 statement preparation note](https://www.sec.gov/Archives/edgar/data/2115436/000003408826000093/xom-20260630.htm) explicitly documents the July1,2026 reorganization and unchanged consolidated reporting basis. This repair applies only to the reviewed XOM registrant pair, after the recorded review time.

## Source and identity controls

The exact filed original is packaged at backend/config/financial_continuity/xom-reporting-basis.htm with SHA256 4dc6e188aa85dd6320818f3465931ba7e00d3ddef101357f218031ca5ec8cd69. The captured predecessor companyfacts SHA256 is 9b95535419e5662e3deed01de803e83deead477a4c81316e1c45186368fdd8b8. New background preparation reads current predecessor facts from its fixed SEC URL, validates CIK and entity name, and records the actual hash on every use. Original source files and their manifest are retained locally in xom-financial-continuity-oct10.

Each metric requires the same standard concept and currency and at least one identical overlapping financial period. Conflicting or absent overlaps cannot fill the metric. Successor facts remain authoritative; predecessor additions end no later than June30,2026 and retain original CIK, hash, filing accession and availability date. No generic ticker aliasing or merger accounting inference is introduced. Unsupported metrics remain absent.

## Results and consumers

Original22-company offline preparation/repeat uses48 captured source reads and zero external requests, production writes or emails. XOM recovers seven current metrics; all values and statuses for the other21companies match the previous cash-flow replay. XOM trailing revenue368.757bn, operating cash59.727bn and capex29.174bn produce free cash30.553bn. Historical financial panels recover six annual and twelve quarterly rows. Price/valuation/forecasts and unsupported gross/operating margins remain absent. Public core fundamentals stays FMP; the already-selected SEC historical reader uses the reviewed history once its background cache refreshes.

Only XOM's preparation/cache version changes. Existing unrelated caches, batch limits, schedules and the serialized data lane are preserved. The financial reader now closes its pooled database session before source I/O; a detached session-level advisory lock preserves refresh exclusion without holding an open transaction or pooled slot across requests. Source failures preserve previous evidence. Public misses continue to enqueue background work.

## Validation and release state

Python3.14.2: initial54 existing financial/preparation checks and18 new continuity/connection checks pass. Final expanded continuity, warming, comparison and monitoring-methodology group:51 passes, overlapping the earlier groups. No full-suite claim. Actual PostgreSQL16.15 replay with a one-slot pool proves zero pooled connections or idle transactions during source I/O, six overlapping refresh refusals, identical cached repeat, cache preservation on429, and lock release after exceptions. It uses the captured XOM originals and a disposable schema, removed afterwards; local server stopped. No production data writes or customer test emails.

The previous NVDA release0373fbaf is independently verified at22:31UTC: all four exact release hashes match; ordinary22:21preparation now supplies126.886bn FCF and all ten retained metrics. AAPL v3 refreshed ordinarily at22:26. Two earlier read-only SSH receipts timed out; the bounded retry succeeded, not evidence of job failure.

This package still requires PR/release checks, deployment hashes, actual scheduled XOM refresh and public reader validation. Broader institution/core/transcript/source coverage and scheduled monitoring/digest gates remain. Massive remains the final subscription step; FMP is not disabled.


## Release and initial production receipt

PR546 merged as a4989c009a3a52e588c5a77447f08e34a363e23a; workflow38091993340 succeeded. Six deployed source/job/proof hashes match at22:37UTC. Secret scan passes; dependency audits fail in unchanged Next.js ecosystem and Pillow dependencies, so no clean full-CI claim. The independent22-company historical-panel replay additionally proves21 unchanged full responses; XOM expands from0annual/2quarterly to6annual/12quarterly. The selected SEC historical reader initially returns a fast loading state on both backend and frontend because XOM had no prepared cache. Its deduplicated ticker_financials refresh is queued with0attempts. The existing bounded enrichment job b6268a9ce4ddfb678d92c12f was explicitly enqueued once through the serialized data lane for verification; no new schedule or parallel import. Cache completion/public validation pending at this receipt. Core remains defaultFMP.

Two-service receipt1791671995 verifies exact release image/seven runtime hashes,1545MB available and empty queue before the validation enqueue. Six public responses200: feed0.40–0.47s, NVDA profile0.53s and new AAPL/NVDA context1.67/1.12s. No further resize.

Next free-source diagnosis: current NEE submissions lists the July24 June-quarter filing, but freshly captured companyfacts still endsMarch31. Official original nee-20260630.htm is captured (3,077,754bytes,SHA256 b77bdb6bb1815103f47126dbebadd164be5525c177f6f419a01b60340e4e686f). It contains both NEE CIK753308 and subsidiary CIK37634, and total regulated/unregulated revenue differs from the smaller contract-revenue component. A fallback needs exact entity/context and total-concept checks; none is implemented or selected by this release.
