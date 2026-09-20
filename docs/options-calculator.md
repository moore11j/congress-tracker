# Options Profit Calculator

Public route: `/options-calculator`. Available in both Tools menus and the public tools directory.

The calculator supports standard 100-share equity options with one underlying and a shared expiration, up to six legs, and a long stock holding. It includes eight templates, exact expiration risk, European Black–Scholes scenarios, position Greeks, charts, tables, a price/time heatmap, and CSV scenarios. Initial SPY/$100 inputs are explicitly illustrative. No orders are submitted.

## Market data

Set the existing server-only `MASSIVE_API_KEY` (or legacy `POLYGON_API_KEY`). No key is sent to the browser. The integration only calls:

- `/v3/reference/options/contracts`, exact expiration, up to 1,000 contracts.
- `/v2/aggs/ticker/{ticker}/prev`, a single underlying or selected option.

The Basic plan provides reference/EOD data, not live bid/ask or snapshot Greeks. All chain values and Greeks in the UI are modeled; a user must explicitly load a closing premium. No automatic per-contract fan-out. Adjusted/nonstandard deliverables are excluded. Empty daily bars are unavailable, never zero prices. Truncated contract lists are labeled.

The existing `ticker_content_cache` table stores sanitized responses for one hour and a rolling five-request/61-second budget. A Postgres advisory transaction lock protects the calculator's budget across workers; a local thread lock handles SQLite development. Other applications using the same provider key can also consume the upstream allowance, so upstream 429s are handled explicitly. No schema migration is needed. Failed provider requests consume budget. Neither provider error bodies nor credential-bearing pagination URLs reach users.

Massive lists Basic as an individual-use plan. Before public distribution of provider data, confirm the account's display/redistribution rights with Massive; free API availability alone does not establish those rights. The manual calculator works without provider access. See [pricing](https://massive.com/pricing?product=options) and [reference documentation](https://massive.com/docs/rest/options/contracts/all-contracts).

## Calculations and boundaries

Expiration P/L sums signed contract quantities × 100 × (intrinsic − entry premium), adds stock P/L, and subtracts entry commissions. Risk extrema and break-even boundaries are computed from strike breakpoints and the slope of the unbounded upper-price segment, never from the plotted range.

Pre-expiration values use Black–Scholes with continuous dividend yield, continuously compounded rates, and calendar days/365. Volatility is entered, not imported market IV. Position Greeks use numerical sensitivities; theta is one day forward and vega is per one volatility percentage point. American early exercise/assignment, stock dividends received, financing, slippage, margin, exit fees, and taxes are excluded. Entry prices are fixed until deliberately changed. This is an educational scenario tool, not a probability model or recommendation engine.

## Validation

`cd frontend; node --test tests/options-calculator.test.mjs`

With the backend dependencies installed: `cd backend; python -m pytest tests/test_options_calculator.py -q`.

Verified locally: 13 options math tests, 10 provider/API tests, TypeScript checking, and the production build. Browser checks covered the payoff table, heatmap, strategy rebuild, stock close import, contract selection, and EOD premium import. A narrow viewport had no horizontal page overflow. A live Massive check returned the September 18, 2026 SPY close, 342 standard contracts for October 23, 2026, and an option close. These are validation observations, not hardcoded application data.
