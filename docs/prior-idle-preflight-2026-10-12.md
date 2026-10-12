# Idle prior-collection startup, October 12, 2026

Local implementation and real PostgreSQL read-only parity validated; release pending. This reduces startup work while existing 13F prior sources are cooling down. No schedule, source selection, retry deadline, collector limit, canonical data or alert changes.

## Diagnosis and repair

Ordinary prior discovery at06:13 uses1.3193CPU seconds even with no due work. Other recurring jobs also retain startup cost; this repair is one measured reduction, not sustained capacity proof. Current06:14metrics show API busy CPU approximately0.142 versus0.125baseline and8941balance, with no sampled throttling after a recent deployment. The30-second process sample is limited to surviving processes and cannot attribute all short-lived job cost.

The CLI now checks automatic prior selection before importing SQLAlchemy or application models. The preflight uses bounded, read-only PostgreSQL queries with two-second connection timeout,1.5-second statements and500ms lock timeout. Its pure receipt selection is shared with the existing ORM planner: identical source/metadata hashes, verified prior status, seven-day successful revisit, configured held-source retry, admission bound and ordering. A new/changed source, expired retry, invalid receipt, database error, explicit selection, preview or global pause uses the existing full worker. The full worker retains its collector lock and background guard. No uncertain result suppresses work. No original byte payload is read or exported.

## Validation

85 focused tests pass on local Python3.14.2, covering exact retry boundaries, changed current/prior hashes/status, priority order, admission bounds, DB failure, read-only driver options and the idle CLI refusing heavy imports. Existing original-source collection/repeat/canonical preservation and preview tests also pass. Diff whitespace passes.

Three cold read-only production comparisons against deployed79a30e49 yield identical full selections:308candidates,308cooling down, zeroeligible. Baseline CPU1.038568/1.027817/1.018245 seconds; candidate0.177858/0.178751/0.176120. Candidate does not import SQLAlchemy or application models. Median component reduction82.61percent. This is selection/startup cost, not total application throughput or proof of capacity for active institutional publication.

An eight-day simulated clock on the same actual saved PostgreSQL rows yields308eligible and an identical selected20-document batch and whole-result hash. No future observations are claimed. Both comparisons are read-only, with zero source requests/public writes/emails. Receipts: prior-preflight-probe-1791785927.json and prior-preflight-future-1791785973.json. Both handles terminal; no local PostgreSQL started.

## Remaining gates

Release exact tested source, verify runtime hashes and an ordinary idle collector run; retain due-source behavior. Continue sustained CPU and complete source/consumer validation before institutional activation. Core remains FMP, institutional publication remains disabled, and finalMassive/globalFMP/billing gates remain. Scheduled Senate06:27 and SEC quarantine06:47 observations remain separate. No purchase or cancellation.
