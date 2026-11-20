## November 19 approved release installed

At 2026-11-19 21:49 MST, PR805 is merged as `280037c5fa7aa8985475066ee3f04b187e64b2d0` and deployed by guarded workflow44675805381. All15 changed backend runtime hashes match, the nullable schema is verified and the isolated captured-source/cache repeat passes. The normal workflow readiness and Premium-route access checks pass. Massive price selection remains off. Receipt: `artifacts/price-compatibility-installed-1795149647.json`.

Eight bounded read-only provider requests through the installed adapter verify 1,254 daily bars each for SPY/QQQ from November22,2021 to November19,2026; NVDA adjusted/unadjusted bars around June10,2024 plus its0.1 split factor and0.01 dividend; and14 recent daily bars each for BRK-B/NBIS. Adapter identity/adjustment checks pass. These establish sampled source access, not full-universe coverage or ordinary consumer acceptance. Receipts: `artifacts/massive-price-source-*-nov19.json`. No production quote writes, provider selection change or emails.

Current institutional inspection is12,334 prepared of16,662 originals, cursor59031, run215successful/idle, no pending request. The old window expired. A new bounded window is prepared but unapproved/unstarted. A read-only five-minute post-release memory/readiness observation is running. Full price activation/warming, consumer/scheduled checks, sustained capacity and remaining financial acceptance still gate the migration. Independent Walnut2.0 evidence-contract preparation continues.

# Massive price consumer preparation

This draft reconciles the existing default-off price core onto production base `5865b09c` and closes additional public-reader and absolute-alert compatibility gaps. It is not deployed or activated. No subscription, production schema change, source selection or customer message occurs in this preparation.

Market quote, ticker context, ticker shell and chart readers retain selected-provider observation time and delayed-price metadata. Foreground cache misses queue existing refresh work without waiting for Massive HTTP. Selected chart and financial price paths cannot fall back to FMP prices or a stale fundamentals price. Cached daily chart observations are explicitly daily closes; minute observations retain their UTC timestamp. Research exports and chart labels disclose the delay when supplied.

Absolute-price rules require a current-session verified Massive observation. Legacy, future, stale and prior-session prices cannot trigger them. Provider transitions initialize the rule baseline, and a later genuine crossing triggers once. A missing comparison price preserves the last valid rule state. Daily percent rules retain their separate reference-date and durable duplicate protections.

The prepared alert schedule adds 16:20 and 16:25 New York closing sweeps because a 15-minute delayed regular close arrives after the existing 16:10 cutoff. Those calls return before refresh/evaluation/delivery when FMP is selected. The Massive cutoff is 16:30. This schedule is part of the undeployed draft and has not begun running.

The dedicated selected price lane also evaluates Pro absolute-price rules against numeric thresholds. It otherwise only evaluated daily percentage rules, leaving absolute thresholds without the later sweep. Free/Premium retain their existing built-in-rule restriction and the FMP lane retains its existing scope. Mixed technical/financial rules remain on their existing full monitoring path, and their final live timing remains an acceptance gate. The price lane does not recalculate confirmation scores.

Intraday emails and the daily signal digest share the recorded price timing description. A later quote or provider rollback cannot change that historical observation. Daily-close evidence remains dated rather than receiving an invented intraday timestamp. Custom-rule delivery settings, subscription opt-outs and the separate legacy monitoring category restrictions remain unchanged.

The current production prerequisite read confirms `asof_ts` is `TIMESTAMPTZ`. A separate owned PostgreSQL16.15 rehearsal exposed rejection of valid persisted metadata because its naive UTC text did not equal the database's aware timestamp text. Binding now compares the exact normalized UTC instant and still rejects different prices or timestamps. The repaired rehearsal verifies additive migration/repeat, unchanged legacy rows, cross-session metadata recovery, stale-writer protection, rollback-writer invalidation and daily price basis/repeat. Its database is removed and separate server stopped; the institutional replay server is untouched. Fourteen timestamp regression cases and an explicit CI PostgreSQL contract cover naive and aware schemas. No production schema change occurred.

## Current validation

At preparation, 76 focused backend tests pass, plus all eight close-window job tests after the schedule correction. These counts overlap. Frontend typecheck passes and 16 selected frontend consumer checks pass, including executing research exports with delayed/missing metadata. The original research test expected an obsolete follow component and single-line JSX; assertions now follow the established page without changing its product layout. An unrelated broader chart test still expects an old backtest gradient color; no full frontend-suite pass is claimed. Earlier core verification had 172 passes and one Windows temporary-directory permission error; the affected test passes with an explicit workspace temporary directory.

Draft revision932e8b51 passes428 Linux price/consumer checks,16 frontend checks/typecheck and1465 broad SEC source checks in workflows42546700242/42546700100. Public ticker, provider-backoff and security checks also pass. The subsequent production-timestamp correction passes108 local affected checks and the current owned PostgreSQL rehearsal; final exact-head CI remains required. The historical October PostgreSQL proof used a naive timestamp column and did not cover the current production timestamp type.

## Before any deployment or activation

Recheck the nullable `quotes_cache.quote_metadata_json` column and apply/verify its additive schema migration before updated ORM readers are deployed. Keep the old code rollback compatible. Complete broad consumer and installed-release checks, then live entitlement, history/actions/benchmark coverage, cache warming, delayed closing sweeps, alert/digest/ranking observation and zero-FMP transport acceptance. Massive subscription and live selection remain the final migration step. This draft does not authorize publishing incomplete financials or institutional evidence.


## November15 current-release reconciliation

Merged current production55c17443 into the existing draft after verifying its worktree was clean. The automatic overlaps preserve native cash-flow currency display, common-equity/operating/debt method rebaselines and delayed-price behavior. Focused local36custom-alert, Massive absolute-alert, digest observation and price-freshness checks pass in31.63seconds against isolated SQLite. The complete custom-rule suite is now included in the price workflow so future merges exercise both financial method and price transition baselines. Full combined-head Linux/PostgreSQL/frontend/SEC checks are pending. No deployment, schema migration, source selection, subscription or delivery-schedule activation.
