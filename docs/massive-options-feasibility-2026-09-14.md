# Massive options data: access and scoring feasibility

**We can start an end-of-day options-activity experiment with the existing configured key. We cannot obtain trade-by-trade directional flow, chain snapshots, or quotes with that key. No accuracy improvement has yet been demonstrated.**

This assessment combines official Massive documentation, six rate-limited read-only requests using the configured local `MASSIVE_API_KEY`, and inspection of the current application integration. No subscription, production setting, scoring model, or historical outcome was changed. Credentials were not printed or saved in the research artifacts.

## Verified endpoint access

| Data | Endpoint | Result with configured key |
|---|---|---|
| Contract discovery, including an as-of date | `GET /v3/reference/options/contracts` | **200**; 10 TSM calls returned, with another page available |
| Daily OHLC, volume, VWAP | `GET /v2/aggs/ticker/{optionsTicker}/range/1/day/{from}/{to}` | **200**; 14 daily bars |
| Historical minute OHLC, volume, VWAP | `GET /v2/aggs/ticker/{optionsTicker}/range/1/minute/{from}/{to}` | **200**; 39 minute bars on September 11 |
| Chain snapshot, including open interest/IV/Greeks | `GET /v3/snapshot/options/TSM` | **403 NOT_AUTHORIZED**; not entitled |
| Individual trades | `GET /v3/trades/{optionsTicker}` | **403 NOT_AUTHORIZED**; not entitled |
| Bid/ask quotes | `GET /v3/quotes/{optionsTicker}` | **403 NOT_AUTHORIZED**; not entitled |

The successful aggregate requests used `O:TSM260918C00425000`, discovered through the reference API, rather than an invented contract. It is the September 18, 2026 TSM $425 call. The daily query requested August 3 through September 11 and returned bars from August 24 onward. A successful request does not guarantee a bar for every requested session; Massive produces aggregate bars only when qualifying trades exist. Missing bars are not evidence that a contract was listed and inactive without checking contract coverage. [Aggregate documentation](https://massive.com/docs/rest/options/aggregates/custom-bars)

The reference endpoint supports `underlying_ticker`, `as_of`, call/put type, expiration and strike filters, expired contracts, and pagination up to 1,000 results per page. Historical research must include contracts that later expired, rather than use only today's surviving chain. [Contract reference documentation](https://massive.com/docs/rest/options/contracts/all-contracts)

## What the free plan provides

Massive lists Options Basic at $0, five requests per minute, two years of history, and end-of-day access, including reference data and minute aggregates. Minute granularity is available historically; it does not imply intraday freshness on the free plan. [Options pricing](https://massive.com/pricing?product=options)

The advertised individual tiers are:

| Tier | Monthly price | Relevant additions |
|---|---:|---|
| Basic | $0 | Historical aggregates and reference data, end-of-day access |
| Starter | $29 | Unlimited calls, snapshots, daily open interest, IV/Greeks, flat files; 15-minute-delayed aggregate data |
| Developer | $79 | Individual trades and four years of history |
| Advanced | $199 | Quotes and real-time data |

These prices are explicitly for individual use. Before a customer-facing Walnut rollout, the applicable business and data-use terms need to be established; the individual price is not a quote for the Walnut product. [Pricing](https://massive.com/pricing?product=options), [Business options offering](https://massive.com/business-options)

At five requests per minute, a hypothetical 20-symbol pilot with 40 selected contracts per symbol requires about 160 minutes for 800 aggregate requests, plus contract discovery. Each request can return a history range, so this is not necessarily 800 requests for every historical day. Full-universe ingestion across thousands of tickers is not practical at this limit. Use a bounded, fully documented research universe and cached background downloads; page views should read stored results.

## What this can and cannot measure

Free aggregate data supports call/put contract-volume balance, changes relative to a ticker's normal activity, activity by expiration and strike, and gross premium turnover estimated from `volume × VWAP × contract multiplier`. That turnover counts traded activity; it is not net money entering bullish positions. Adjusted or nonstandard contracts require consistent deliverables and price bases, or exclusion from the initial pilot.

The free data does **not** identify buyer-initiated trades, seller-initiated trades, opening versus closing positions, true sweeps, or the legs of a combined strategy. Even paid trade-plus-quote data would require inference and ambiguity handling; it does not automatically supply trader intent. Every contract has both a buyer and seller, and open interest depends on whether each side is opening or closing. [OIC explanation](https://www.optionseducation.org/referencelibrary/faq/general-information)

Daily open interest is also not equivalent to a historical point-in-time archive. The chain snapshot documents open interest from the end of the last trading day. Snapshot access alone cannot recreate historical daily open interest or IV; we would need a confirmed historical source or snapshots collected prospectively. [Snapshot documentation](https://massive.com/docs/rest/options/snapshots/option-chain-snapshot)

There is a research basis for testing options information: Pan and Poteshman's study found predictive information using **buyer-initiated opening** option volume, with effects over the following day/week. That specialized dataset is materially richer than the unsigned volume available here. It does not establish that Massive's free call/put ratio improves our 30D accuracy, or that a 75% target is attainable. [Original research](https://www.nber.org/papers/w10925)

## Existing integration findings

1. `backend/app/services/options_flow_providers/massive.py:26` fetches chain snapshots, which the configured key cannot access. It has no free aggregate fallback.
2. The same provider accepts `lookback_days`, but retrieves a current snapshot rather than that many days of history. Its two-page/500-contract cap, sorted by expiration, can truncate a chain and overrepresent near expirations.
3. The provider converts daily volume and VWAP/close into gross premium turnover. `backend/app/services/options_flow.py:361` then infers bullish/bearish direction from call-versus-put premium dominance. That is an unvalidated sentiment proxy, not observed buyer/seller flow.
4. The primary confirmation-context path reads local `options_flow_summary` or `options_flow_events` tables through `backend/app/services/intelligence_overlays.py:53`. Replacing the HTTP provider alone would not establish persistent history for this path. This assessment did not audit production table contents.
5. Populating directional options summaries can feed confirmation scoring through `backend/app/services/confirmation_score.py:921`. A research collector must remain separate from those live inputs until its prospective use is reviewed.

## Recommended experiment

Start with the free data as **options activity**, cached after each trading day and usable no earlier than the next session. Use a fixed pilot universe chosen without future performance, date-correct contract metadata, both calls and puts, explicit liquidity/coverage requirements, and complete pagination within the stated filters.

Predefine a small feature set:

- Unusual call and put volume relative to the ticker's own trailing history.
- Changes in call/put volume and gross premium balance, retaining uncertainty about trade direction.
- Separate contracts expiring within 7 days, 8–29 days, and 30–90 days. The longer bucket is a hypothesis for the 30D objective, not a known optimal range.
- Interaction with existing price trend and market-condition inputs: does options activity improve selection among otherwise similar bullish candidates?

Evaluate the existing baseline against the same baseline plus these inputs on identical eligible events. Report 30D first and 7D separately; include direction-specific accuracy, raw return, SPY-relative return, coverage, date/ticker concentration, and missing-data rates. Use chronological training and a 30D label-maturity gap. The already-inspected public ledger remains exploratory; independent later data is needed before promotion. Do not replace historical ledger scores or write research outcomes into the public ledger.

No backtest has been run with these new options inputs yet. The completed work establishes that a free pilot is technically feasible and identifies why the existing snapshot-based implementation does not work with the configured key.

## Reproducibility

Probe: `backend/scripts/research/probe_massive_options_access.py`. Sanitized access results and public contract/bar samples are in ignored local artifacts under `frontend/test-results/confirmation-research/`: `massive-options-access.json`, `massive-options-contracts.json`, `massive-options-daily_bars.json`, and `massive-options-minute_bars.json`. The six requests were spaced at least 13 seconds apart. Authentication and pagination URLs containing credentials are not persisted.
