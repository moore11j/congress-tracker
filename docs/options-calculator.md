# Options Profit Calculator

Public route: `/options-calculator`. Available in both Tools menus and the public tools directory.

The calculator supports standard 100-share equity options with one underlying and a shared expiration, up to six legs, and a long stock holding. It includes eight templates, exact expiration risk, European Black–Scholes scenarios, position Greeks, charts, tables, a price/time heatmap, and CSV scenarios. Initial SPY/$100 inputs are explicitly illustrative. No orders are submitted.

## Market data

Set the existing server-only `MASSIVE_API_KEY` (or legacy `POLYGON_API_KEY`). No key is sent to the browser. The integration only calls:

- `/v3/reference/options/contracts`, exact expiration, up to 1,000 contracts.
- `/v2/aggs/ticker/{ticker}/prev`, a single underlying or selected option.

The Basic plan provides reference/EOD data, not live bid/ask or snapshot Greeks. Ticker autocomplete uses Walnut's existing company/symbol search. Listed dates, strikes, and modeled strategy leg closes load automatically. With Massive alone, optional whole-chain loading is paced at one price request per 13 seconds. Pagination is followed, adjusted/nonstandard deliverables are excluded, and empty bars are unavailable, never zero prices. All actual closes carry their source and date; unavailable premiums remain explicitly labeled estimates. Manually entered premiums are never replaced by background loading. Chart inspection boxes can be dragged or docked below the plot.

### Alpaca historical batch prices

Run `backend/scripts/configure_alpaca.ps1` for a local masked form that writes `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` into the ignored `backend/.env.local`. Alpaca labels the first credential **Key** in its dashboard; it is not the Endpoint. The secret is shown when generating the pair. No trading endpoint is used.

Set server-only `OPTIONS_PRICE_PROVIDER=alpaca` after verifying historical bar access. Production requires separately configuring the host's secrets; saving the local file does not deploy or configure production. The loader must load this env file explicitly when running locally.

`GET /api/tools/options/prices?tickers=O:...,O:...` accepts up to 100 validated contracts. It uses `https://data.alpaca.markets/v1beta1/options/bars`, timeframe `1Day`, seven calendar days ending before the current UTC day (and at least 16 minutes behind now). This excludes the current US session's incomplete daily bar. It follows every `next_page_token` and picks the latest valid bar for each contract. These are historical daily trade closes, **not** bid/ask quotes or current 15-minute-delayed quotes. The adapter never uses Alpaca's indicative snapshot feed.

With Alpaca enabled, chain prices preload automatically in batches of up to 100, selected modeled legs first. Loading pauses when hidden, on context changes, or when the user pauses. Contracts with no valid trade in the seven-day window retain labeled estimates. Failed/incomplete requests pause with a retry control instead of treating unvisited contracts as missing. Expiration and contract reference requests still use Massive's separate allowance.

Alpaca responses reuse the existing cache for one hour, with a separate account-specific 180-request/61-second guard below Basic's published 200/minute allowance. All provider errors are sanitized. Basic historical OPRA access for data older than 15 minutes is described by [Alpaca staff](https://forum.alpaca.markets/t/data-source-for-option-historical-bars/17704); verify entitlement with the configured account. See the [bar endpoint](https://docs.alpaca.markets/us/reference/optionbars). Public display/redistribution permission must be confirmed separately from successful API access.

The existing `ticker_content_cache` table stores sanitized responses for one hour and a rolling five-request/61-second budget. A Postgres advisory transaction lock protects the calculator's budget across workers; a local thread lock handles SQLite development. Other applications using the same provider key can also consume the upstream allowance, so upstream 429s are handled explicitly. No schema migration is needed. Failed provider requests consume budget. Neither provider error bodies nor credential-bearing pagination URLs reach users.

Massive lists Basic as an individual-use plan. Before public distribution of provider data, confirm the account's display/redistribution rights with Massive; free API availability alone does not establish those rights. The manual calculator works without provider access. See [pricing](https://massive.com/pricing?product=options) and [reference documentation](https://massive.com/docs/rest/options/contracts/all-contracts).

## Calculations and boundaries

### Free historical options activity

The Pro ticker context includes an on-demand **Options activity** card, served by `POST /api/tools/options/activity?symbol=SPY`. It reuses the existing server keys and free historical bars; no paid quote feed or new subscription is required. The route enforces `options_flow_feed` access before requesting provider data.

Each sample selects the first listed expiration at least 21 days away and checks all its currently listed standard 100-share contracts, up to 1,000. Other expirations and adjusted contracts are excluded. The expiration appears on the ticker card and screener. Historical data spans 30 calendar days ending before the current UTC day, outside Alpaca's recent-data restriction. The provider adapter follows pagination; missing VWAP, malformed volume, or incomplete pagination fails without saving partial totals.

For each contract/session, estimated premium traded = volume × VWAP × 100. Sum these values separately for calls and puts. More than 1.6× opposite-side premium is labeled call-heavy or put-heavy; otherwise mixed. These unsigned totals cannot identify buying, selling, or hedge intent. `can_confirm=false` and `score=null` prevent the sample from contributing to directional confirmation. The older aggregate-premium summarizer also no longer infers bullish/bearish intent.

The volume comparison divides the latest observed session's total contracts traded by the mean of up to 20 preceding active sessions, requiring at least five. A multiple of 2× or more is labeled an activity spike; it is not sentiment. Historical samples are cached for 24 hours in `ticker_content_cache`, with cache-only profile/screener reads for the 30-day window. Data loads only when requested; provider quotas still apply. Cached samples cover only tickers previously loaded, not the full market.

Validation: `python -m pytest tests/test_options_activity.py tests/test_options_flow_summary.py tests/test_options_alpaca.py tests/test_options_calculator.py -q`. A local live check sampled 442 SPY contracts expiring October 16, 2026 in 3.8 seconds, with the cached result returned immediately. This timing is an observation, not a loading-time guarantee.

Expiration P/L sums signed contract quantities × 100 × (intrinsic − entry premium), adds stock P/L, and subtracts entry commissions. Risk extrema and break-even boundaries are computed from strike breakpoints and the slope of the unbounded upper-price segment, never from the plotted range.

Pre-expiration values use Black–Scholes with continuous dividend yield, continuously compounded rates, and calendar days/365. Volatility is entered, not imported market IV. Position Greeks use numerical sensitivities; theta is one day forward and vega is per one volatility percentage point. American early exercise/assignment, stock dividends received, financing, slippage, margin, exit fees, and taxes are excluded. Entry prices are fixed until deliberately changed. This is an educational scenario tool, not a probability model or recommendation engine.

## Validation

`cd frontend; node --test tests/options-calculator.test.mjs tests/options-market-data.test.mjs tests/chart-system.test.mjs`

With the backend dependencies installed: `cd backend; python -m pytest tests/test_options_calculator.py tests/test_options_alpaca.py -q`.

Verified locally: 13 options math tests, 10 provider/API tests, TypeScript checking, and the production build. Browser checks covered the payoff table, heatmap, strategy rebuild, stock close import, contract selection, and EOD premium import. A narrow viewport had no horizontal page overflow. A live Massive check returned the September 18, 2026 SPY close, 342 standard contracts for October 23, 2026, and an option close. These are validation observations, not hardcoded application data.
