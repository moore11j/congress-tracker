# Massive price consumer preparation

This draft reconciles the existing default-off price core onto production base `5865b09c` and closes additional public-reader and absolute-alert compatibility gaps. It is not deployed or activated. No subscription, production schema change, source selection or customer message occurs in this preparation.

Market quote, ticker context, ticker shell and chart readers retain selected-provider observation time and delayed-price metadata. Foreground cache misses queue existing refresh work without waiting for Massive HTTP. Selected chart and financial price paths cannot fall back to FMP prices or a stale fundamentals price. Cached daily chart observations are explicitly daily closes; minute observations retain their UTC timestamp. Research exports and chart labels disclose the delay when supplied.

Absolute-price rules require a current-session verified Massive observation. Legacy, future, stale and prior-session prices cannot trigger them. Provider transitions initialize the rule baseline, and a later genuine crossing triggers once. A missing comparison price preserves the last valid rule state. Daily percent rules retain their separate reference-date and durable duplicate protections.

The prepared alert schedule adds 16:20 and 16:25 New York closing sweeps because a 15-minute delayed regular close arrives after the existing 16:10 cutoff. Those calls return before refresh/evaluation/delivery when FMP is selected. The Massive cutoff is 16:30. This schedule is part of the undeployed draft and has not begun running.

## Current validation

At preparation, 76 focused backend tests pass, plus all eight close-window job tests after the schedule correction. These counts overlap. Frontend typecheck passes and 16 selected frontend consumer checks pass, including executing research exports with delayed/missing metadata. The original research test expected an obsolete follow component and single-line JSX; assertions now follow the established page without changing its product layout. An unrelated broader chart test still expects an old backtest gradient color; no full frontend-suite pass is claimed. Earlier core verification had 172 passes and one Windows temporary-directory permission error; the affected test passes with an explicit workspace temporary directory.

The broad current backend run passes 284 checks. Draft PR805's first Linux backend and frontend jobs pass in workflow42544278696; broader SEC regression is separately running. A subsequent alert evidence correction retains the exact absolute-rule observation in the durable alert and email, and represents a daily close by its source date without inventing an intraday observation timestamp. All22 affected alert/evidence/job checks pass locally; final Linux validation of that correction remains pending. The historical October PostgreSQL proof and tests are recorded in [the core report](massive-price-core-reconciliation-2026-10-31.md), but are not substitutes for current release and schema verification.

## Before any deployment or activation

Recheck the nullable `quotes_cache.quote_metadata_json` column and apply/verify its additive schema migration before updated ORM readers are deployed. Keep the old code rollback compatible. Complete broad consumer and installed-release checks, then live entitlement, history/actions/benchmark coverage, cache warming, delayed closing sweeps, alert/digest/ranking observation and zero-FMP transport acceptance. Massive subscription and live selection remain the final migration step. This draft does not authorize publishing incomplete financials or institutional evidence.
