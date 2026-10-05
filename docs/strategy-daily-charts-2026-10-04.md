# Daily strategy charts — October 4, 2026

## Finding and change

The strategy detail page still graphed a saved research run ending in July or August. Daily evaluations, positions and alerts used a different ledger. The earlier cap/alert release did not join these into a daily performance curve.

The default chart now reconstructs recorded model decisions from activation (August 12 or August 17), marks them each completed exchange session and compares them with SPY. The original multiyear research remains under **Historical research**, with its own metrics and dates. The two series are not spliced. Sunday October 4 has no closing session: the expected endpoint is Friday October 2.

Implementation: `strategy_model_chart.py`, an additive `StrategyModelChart` cache table, the strategy detail projection, evaluation target-weight metadata, the chart refresh job and detail UI. Cron refreshes at :12/:32/:52 from 17:00–22:59 America/Los_Angeles on weekdays. Each attempt hydrates at most 25 missing symbols using a rotating cursor, then publishes all active models; timeout is 1,100 seconds. Missed prices are retried and stale cache dates are exposed on reads. This schedule supplements the existing evaluations, position-price reconciliation and alerts.

## Accounting and coverage

- Start at $100,000 cash. Trade no earlier than the next exchange opening after the recorded evaluation, and never before the run was actually created. Preserve shares between decisions; do not silently rebalance daily.
- Use recorded target weights. Legacy equal-weight resolvers omitted some rebalance rows, so their recorded full candidate membership plus versioned equal-weight rule reconstructs the target. Invalid placeholder allocations stay cash. This repairs accounting without rewriting the old trade/event ledger.
- Use canonical split-adjusted opens and closes, fractional shares and zero interest on uninvested cash. Dividends, fees, spread and slippage are excluded. This is model accounting, not brokerage performance or a fresh validation study.
- An unpriced new entry remains cash. A known opening price with no close cannot silently exclude a position. Missing execution prices on existing holdings skip the whole rebalance, retaining executed shares and cash. Retained positions remain price-refresh dependencies beyond their originally planned exits.
- Existing holdings can use a clearly disclosed last verified close for at most three trading sessions. Longer gaps stop the curve. Public coverage counts disclose unfilled symbols, skipped rebalances and prior-close marks; paid ticker identities and private run IDs are not leaked through chart metadata.
- NRDE's verified successor is SNFI from July 21, 2026: the [SEC 8-K, Items 5.03/8.01](https://www.sec.gov/Archives/edgar/data/1759546/000149315226034092/form8-k.htm) states the name/ticker change and unchanged common-stock CUSIP. The dated mapping reads successor prices while preserving the original ledger symbol. It does not substitute a peer company or overwrite original bars.

The initial canonical-price backfill checked 563 missing symbol histories and populated 554; nine returned no data. Additional bounded repairs populated the successor and prices for retained positions. Final read-only production-data replay reaches **October 2 for all 20 active models**: 17 complete-coverage curves and three explicitly partial-coverage insider curves. The latter are not presented as fully executable historical results. Their original oversized portfolios remain part of the historical decision record; October 4's new capped targets cannot execute before October 5.

## Dwight Evans and annual reports

Dwight Evans has no recorded model trades since activation on August 12. The source table contains 75 transactions, with its newest stored event/ingestion on June 25. An empty model therefore stays cash; it is not evidence that Evans personally owns no investments. Comprehensive source-feed completeness remains unverified.

The 2025 annual reports for Dwight Evans (document 10077377, 34 rows) and Cleo Fields (10074378, 49 rows) were filed May 14, 2026. A year-end snapshot published later is not information available for a tradable January 1 opening balance. It also contains value ranges, not a complete brokerage share ledger. Annual reported holdings remain separate from the disclosure-driven model, and the UI explains the distinction. Cleo's stored ledger contains October 2 filings; the model is not frozen at its historical chart endpoint.

## Checks and release state

- Python 3.14.2: 43 focused chart/evaluation/storage/reliability checks pass. Expanded strategy/replicated-portfolio checks: 182 pass; two failed because the default `/data/app.db` path is not usable on this Windows host, then both pass with isolated in-memory SQLite. No full application suite claim.
- Two frontend strategy route checks pass. The isolated candidate frontend passes the optimized Next.js build, including type checking and all 67 static pages. Shared-workspace generated ticker-preview types caused a separate check failure; concurrent ticker changes are excluded from this release.
- Local browser review verifies daily/historical switching, separate metrics, dates and retained chart selection in record links. Live replay verifies data; the UI fixture is synthetic and is not performance evidence.
- All 20 production-data replays reach the expected session. Daily refresh/cache publication and deployment receipt are pending at this checkpoint.

No subscriber email was sent, no preferences changed, and no actual security trade was placed in this follow-up. Canonical price-cache repairs are the only production writes before deployment. Source ingestion completeness, subsequent scheduled-run observation, closed-trade record pricing and realistic execution costs remain open.
