# Idle prior-collection startup, October 12, 2026

Deployed as PR605/9ce01d59; exact runtime and ordinary idle run verified below. This reduces startup work while existing 13F prior sources are cooling down. No schedule, source selection, retry deadline, collector limit, canonical data or alert changes.

## Diagnosis and repair

Ordinary prior discovery at06:13 uses1.3193CPU seconds even with no due work. Other recurring jobs also retain startup cost; this repair is one measured reduction, not sustained capacity proof. Current06:14metrics show API busy CPU approximately0.142 versus0.125baseline and8941balance, with no sampled throttling after a recent deployment. The30-second process sample is limited to surviving processes and cannot attribute all short-lived job cost.

The CLI now checks automatic prior selection before importing SQLAlchemy or application models. The preflight uses bounded, read-only PostgreSQL queries with two-second connection timeout,1.5-second statements and500ms lock timeout. Its pure receipt selection is shared with the existing ORM planner: identical source/metadata hashes, verified prior status, seven-day successful revisit, configured held-source retry, admission bound and ordering. A new/changed source, expired retry, invalid receipt, database error, explicit selection, preview or global pause uses the existing full worker. The full worker retains its collector lock and background guard. No uncertain result suppresses work. No original byte payload is read or exported.

## Validation

85 focused tests pass on local Python3.14.2, covering exact retry boundaries, changed current/prior hashes/status, priority order, admission bounds, DB failure, read-only driver options and the idle CLI refusing heavy imports. Existing original-source collection/repeat/canonical preservation and preview tests also pass. Diff whitespace passes.

Three cold read-only production comparisons against deployed79a30e49 yield identical full selections:308candidates,308cooling down, zeroeligible. Baseline CPU1.038568/1.027817/1.018245 seconds; candidate0.177858/0.178751/0.176120. Candidate does not import SQLAlchemy or application models. Median component reduction82.70percent. This is selection/startup cost, not total application throughput or proof of capacity for active institutional publication.

An eight-day simulated clock on the same actual saved PostgreSQL rows yields308eligible and an identical selected20-document batch and whole-result hash. No future observations are claimed. Both comparisons are read-only, with zero source requests/public writes/emails. Receipts: prior-preflight-probe-1791785927.json and prior-preflight-future-1791785973.json. Both handles terminal; no local PostgreSQL started.

## Remaining gates

Release exact tested source, verify runtime hashes and an ordinary idle collector run; retain due-source behavior. Continue sustained CPU and complete source/consumer validation before institutional activation. Core remains FMP, institutional publication remains disabled, and finalMassive/globalFMP/billing gates remain. Scheduled Senate06:27 and SEC quarantine06:47 observations remain separate. No purchase or cancellation.


## Deployed and ordinary run verified,06:29UTC

[PR605](https://github.com/moore11j/congress-tracker/pull/605), finalheadd862d11f, merges9ce01d59e0d0008e54ec55882c81be2008252f89. Backend workflow38212265635 succeeds.59runtimehashes match, both approved host sizes unchanged, queue39has no pending/running work and1448.7MB available. Eight public responses succeed; feeds0.511/0.413s and freshly generatedAAPL1.707/NVDA1.208s are samples, not sustained capacity proof. Core remainsFMP, institutional publicationfalse, already selected Senateflags true. Receipt prior-preflight-runtime-1791786317.json.

Ordinary06:28 prior collector completesexit0 in0.4wall/0.2399CPU seconds, versus the earlier06:13 ordinary1.3193CPU. Receipt scheduled-job-cpu-1791786516.json. No cadence or retry change; due-source/failure fallback is covered by the exact PostgreSQL and regression checks above. This reduces idle overhead only, not active institutional publication cost. Secret and Vercel checks pass; unchanged Pillow and five frontend dependency vulnerabilities still fail audits. Direct diff review completed; no automated review pass claimed. Attachment attempted but task100-item limit reached.

Current SEC financial rotation continues on PR604: due1033/1023/1013 over the06:11/06:16/06:21 batches, each10SEC/8Finnhub and zero canonical writes/emails. Full rotation and source coverage remain open. First selected Senate scheduled publication is independently verified in the Senate report; due SEC quarantine06:47 remains pending. All new helper handles terminal; no local PostgreSQL started.

Explicit ordinary stdout receipt prior-idle-receipt-1791786579.json also confirms status idle,308cooling-down candidates, zero eligible/document IDs and zero public writes.
