# FMP cutover execution

October 8, 2026. The owner directs: keep going until FMP is off. This is the execution checklist for the existing approved replacement and conditional cutover, not a claim that production is ready. Quiver is excluded. Massive personal Stocks remains the selected price source. Avoid repeated approval requests for work already authorized.

## Retained product and release gates

| Area | Current evidence | Required before FMP off |
|---|---|---|
| Prices, charts, actions, benchmark returns | Local Massive adapter, observation-bound cache, action deduplication; daily bars/actions sampled live. Benchmarks and market-pressure refresh now select Massive. | Starter activation; live delayed snapshots, representative history/action coverage, all price displays and no-send price alerts. Last account snapshot response was 403. |
| Congress | [Collection](congress-collection-2026-10-08.md): 12 Senate downloads and 16 complete digital House reports; 27 revisions/264 distinct identities repeat. [Live canonical baseline](congress-canonical-reconciliation-2026-10-08.md) exposes duplicate/date drift: one five-row source has 18 transactions/15 public events. Local repair preserves original pairs/history; [guarded publisher](direct-congress-worker-2026-10-08.md) now replays three new filings/61 trades/44 stock events and 44 synthetic alerts with identical repeat state. Shared writer ownership implemented locally; 133 regressions plus 33 final overlapping checks pass. | Production repair preserving IDs, exact active-job disposition, scanned forms, Senate exceptions and one member alias, guarded canonical publication, shadow rollout and scheduled coverage. No Quiver. |
| Insiders | 414 Form 4s parsed; [guarded publication worker](direct-feed-publication-worker-2026-10-08.md) now implemented locally with atomic receipts/enrichment, source ownership and repeat safety. Populated replay adds five events, preserves 323 existing filings and holds 86. | Production baseline repair, remaining ambiguous holdings/owners, real scheduled collection/publication. No production activation yet. |
| Institutions | Direct SEC discovery, 35 recent filings and five real quarter pairs; [guarded worker/batch job](direct-13f-worker-2026-10-08.md), dated N-PORT mapping and unit-conflict holds implemented locally. Real replay inserts 9 filings/77 positions with identical repeat state; 205 checks pass. | Complete retained-manager coverage, real qualifying public alert/no-send preview, baseline repair, remaining writer review, amendments/corporate actions and scheduled rollout. Job remains disabled. Do not publish Lakehouse's held unit conflicts. |
| Directory, fundamentals and screener | [Opt-in SEC acquisition](sec-fundamentals-replacement-2026-10-08.md) now feeds selected cache/screener/ranking paths with saved evidence. Five live samples replay exactly; three reach existing fundamental-score coverage. Default/production remains FMP. | Independent current identity/fundamental acquisition with units, periods, availability and provenance; rebuild fresh universe and score inputs with FMP blocked. Prepared-cache parity is insufficient. |
| Research materials | Issuer pilot and SEC sources exist; broad news, consensus and transcripts remain FMP dependencies. | Publish supported issuer/SEC material with explicit coverage. Reduce optional unavailable features consistently across routes, schedules, strategy inputs and plan copy; do not call guidance consensus. |
| Monitoring/watchlists/daily/weekly Top Stocks | Real builders exercised on isolated prepared input; IDs and subscription/access preferences preserved. | Replacement-driven fresh prices, scores, activity and retained content; complete no-send previews and repeat checks. Rebaseline a versioned score change if needed, without rewriting history. |
| Operations | Shadow collectors and canonical ownership guards deployed: c2b97e80, 29c354c5, 94a9e07f and 2cea0aae, all four workers/health/schema verified October 8. Public publishers disabled; FMP-dependent jobs retained. | Scoped release, schema before updated workers, source schedule changes, backups/rollback and production health checks. Verify subsequent scheduled source runs and no duplicate records. |
| FMP shutdown and billing | Global `FMP_PROVIDER_DISABLED` exists but audit found unguarded callers. | All retained paths tested while disabled; stop retired schedules; verify runtime requests stop on every worker. Then turn off renewal/cancel as appropriate and verify account receipt/access date and data disposition. Exact November renewal date remains unverified. |

## October 8 transport and price repair

Audit of direct request sites found the existing off switch was bypassed by House/Senate page fetches, insider freshness, marketing articles, fundamentals rows/diagnostics, market previous-close requests and ticker metadata diagnostics. These now use the shared guard. New requests are counted where response accounting was also absent. This is a source audit plus targeted executable checks, not a production zero-egress observation or proof of all configurable endpoints.

Benchmark helper calls select Massive without a fallback to FMP or a substitution of an ETF for an unavailable index. Market-pressure refresh uses the selected batch quote service, rejects unverified/stale/future/other-session observations, preserves source timestamps and quote metadata, and computes change from the corresponding previous close. Missing prices stay unavailable. The prior minute-price writer no longer writes a purported daily close into `PriceCache`; this fixes a risk to return and alert reference calculations. Historical contaminated closes, if any, have not been audited or repaired in production.

Market-pressure tiles retain delayed/source fields; the view and stock detail display delayed-price timing. The quote's timestamp remains the observation time. A previous-close date absent from the provider response remains absent rather than an invented date.

Validation receipts and final checks are recorded in the context log. No schema migration, release, production data change, paid plan activation, email, FMP request or subscription cancellation occurred in this checkpoint. The Massive activation question is pending while independent work continues.

## Execution order

1. Finish transport and caller audit, selected-price coverage and delayed labels. Recheck live Massive entitlement after activation.
2. Replace core directory/fundamental/universe acquisition; establish a representative fresh ranking baseline with FMP disabled.
3. Promote direct feeds from isolated rehearsal to an opt-in production-capable worker with source receipts, atomic publication, conservative holds and repeat safety. Complete unattended Congress discovery.
4. Make the reduced research-feature scope explicit in application/jobs/strategy inputs and customer copy. Run complete no-send alert and digest previews from freshly replaced inputs.
5. Release the scoped migration, observe scheduled execution and reconcile canonical identity/coverage. Disable FMP runtime access only after retained-product gates pass, then verify billing cancellation and access dates.

Detailed evidence: [Massive](massive-stock-adapter-2026-10-07.md), [SEC alerts](direct-sec-alert-safety-2026-10-07.md), [13F pairs](direct-13f-publication-2026-10-08.md), [Senate](senate-source-validation-2026-10-07.md), [research feeds](free-research-feeds-2026-10-07.md), [exit scope](fmp-core-product-exit-plan-2026-10-07.md).

## October 8 live rollout progress

[Shadow collectors](direct-feed-shadow-rollout-2026-10-08.md) and [canonical ownership guards](feed-ownership-rollout-2026-10-08.md) are deployed. Nineteen committed code hashes, directory, disabled publishers and twelve source revisions verified. Public cutover remains unselected. Exact 13F whitespace correction 94a9e07f committed with 97 passing tests, live retry pending. First hourly collection observation remains pending. FMP account is signed out; login requested for precise billing verification. No cancellation or Starter purchase.


**Later October 8 evidence:** 94a9e07f deployed/all four workers verified. Actual scheduled Congress collection now observed; four scans held. SEC backlog run processes 278 filings, four cover-total conflicts held and remaining 120 Form 4 processing. Queue re-enqueue retirement guard committed as 2cea0aae with 57 passing checks; deployment waits for collection. Public source activation remains disabled.


**Final 21:37 UTC receipt:** 2cea0aae verified on all four workers, repeat schema/health/access checks pass. All 411 October 7 SEC entries attempted; 175 Form 4 parsed/150 quarantined and 77 13F parsed/five quarantined/four failed. All 418 source-byte hashes verify; repeat preserves 426 document identities/418 revisions with zero new work or public writes. Canonical publication remains disabled. Massive snapshot still 403; Starter and account login pending. FMP active, goal active.


## October 8 Senate production cutover, 22:43 UTC

[Repair/activation receipt](direct-congress-repair-rollout-2026-10-08.md) supersedes the earlier all-publishers-disabled state for Senate only. `8db83b2d` deployed, both indexes valid, exact five source equity rows retained after ten duplicate events/thirteen transactions removed and five queued jobs retired. Portfolio history unchanged; repeat no-op. Official Senate generation 2 selected from October 1; legacy request path skips with HTTP forbidden and no recorded successful Senate requests since selection. Bond-only conflict held. First hourly :27 publication remains pending; House/SEC publishers remain disabled/default FMP. Public APIs correct; member page cache correction b57c1af5 verified on both sites/rendered page. Five-event no-send replay and three-score/four-tile refresh pass, prices/history and Top Stocks items unchanged. Broader score/digest freshness, remaining feed coverage, Massive Starter and billing still gate complete FMP shutdown.


**23:01 live receipt:** same-day Congress release `9079343c` deployed/all four workers/readiness verified, 65 checks pass. Five stock events and corrected score views retained; zero successful recorded Senate FMP calls since switch. First 23:27 scheduled publisher still pending. SEC hourly run 10 processes 200 more and leaves 3,474 older pending Form 4s. Full migration and billing remain active work.


## October 8 House scan deployment, 23:16 UTC

**October 8 House scan release:** [review and live receipt](house-scan-review-2026-10-08.md), `0f3e2171` deployed/all four workers healthy. Exact-source reviewed transcription resolves four stock rows with separate ticker provenance; 102 tests pass in both checkouts and live source/repeat checks pass without public changes. Four date-only candidate filings/12 trades require preservation of observed availability before correction; remaining scans/canonical conflicts keep House on FMP. Senate remains official; first scheduled publisher pending. Full shutdown, Starter and billing remain unfinished.

## October 8 Congress availability preparation

[date/availability report](congress-availability-dates-2026-10-08.md) rehearses four House filings/12 trades without backdating entry dates or changing saved history/alerts. Six isolated PostgreSQL cases and 378 exact-release checks pass, deployment pending; no public House corrections. First scheduled Senate publication now observed at 23:29 with zero inserts/emails and known bond filing held (partial/exit 1), superseding the earlier pending status. House ownership/source conflicts, fresh replacement-driven rankings/all alerts, Starter and billing remain gates.
