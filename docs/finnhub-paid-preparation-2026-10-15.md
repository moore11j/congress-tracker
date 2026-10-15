# Finnhub estimate and target preparation, October 15, 2026

## Scope and status

This package prepares the remaining paid analyst inputs without buying a subscription or switching a public source. It adds a runnable one-scope collector for annual/quarterly EPS estimates, annual/quarterly revenue estimates and current price targets. Collection is off by default. No recurring schedule, public reader selection, price request, canonical analyst event, score, alert, financial record or FMP fallback is added.

This is source preparation, not completed analyst migration. Live entitlement, response coverage, source units/basis, consumer projection, scheduled preparation and publication remain to be verified. No paid Finnhub response has been fetched for this change.

## Primary source contract

Reviewed the official [Python client](https://github.com/Finnhub-Stock-API/finnhub-python/blob/master/finnhub/client.py) and pinned [OpenAPI specification](https://github.com/Finnhub-Stock-API/finnhub-go/blob/8071bcd46ca5101adcef7a844c96b81a3eceb76e/api/openapi.yaml). Public specification saved locally as artifacts/finnhub-official-openapi-oct15.yaml. No private production records were read or exported.

| Retained feature | Endpoint | Requested scopes |
|---|---|---|
| EPS forecasts | stock/eps-estimate | annual and quarterly |
| Revenue forecasts | stock/revenue-estimate | annual and quarterly |
| Analyst price targets | stock/price-target | current |

The EPS and revenue model descriptions include proprietary estimates in their averages. They must not be relabeled pure analyst consensus. Fiscal year/quarter and period are distinct, and each is preserved. Revenue scale is retained without an assumed millions multiplier. Price-target mean and median are distinct. A recommendation month is not a price-target update time or upgrade event.

These response schemas do not establish currency, GAAP/adjusted basis, ordinary-share/ADS basis or target horizon. The staging projection explicitly records those as unverified and publicationEligible=false. Company listing currency alone does not establish financial-estimate currency or share basis. No inferred upside, EPS surprise, forward P/E, earnings forecast from EPS times shares, or DCF is calculated. Existing SEC statements, recommendation panels and score inputs continue unchanged.

## Collection and evidence

The shared Finnhub transport allows only the three additional reviewed paths. They require FINNHUB_PAID_PREPARATION_ENABLED=1 and FINNHUB_SHARED_LIMITER_ENABLED=1 before key lookup, request-budget reservation or network access. Both preparation entry point and transport enforce the gate. Existing free requests retain their behavior. The collector reuses header authentication, no redirects, bounded response/time limits, shared sliding-minute budget, 429 cooldown and stable sanitized errors.

The explicit command is `python -m app.jobs.prepare_finnhub_paid --symbol AAPL --kind eps --frequency quarterly`. Targets omit frequency. The command does not enable its own gate and is not registered with cron or the enrichment queue. When disabled it exits before database imports. Invocation is not authorization to purchase a plan or activate production collection.

Each scope has a separate InsightsSnapshot key/source version, a 24-hour reuse period and PostgreSQL transaction advisory lock. The response and deterministic normalized projection are stored with canonical JSON response hash and actual observation time. A fresh repeat validates source/projection agreement, makes no request, and changes no evidence. Period/target status is reevaluated at read time without advancing observation time. Expired data is refreshed once; a rejected response leaves old evidence untouched. No old cache is silently promoted to current data.

Strict security identity, frequency, fiscal-period, finite-number, range and analyst-count checks reject malformed/conflicting data. Nested estimate rows are capped at 1000. Identical period rows collapse; conflicting dates or fiscal labels fail the scope. Historical-only data stays historical; missing values remain missing. Negative/zero EPS is retained, while negative revenue and nonpositive price targets are held. Date-only source timestamps remain date-only. An aware target timestamp retains its original precision; future or ambiguous naive timestamps are rejected.

The transaction remains open during the bounded single request to serialize duplicate manual preparations. This package is not enabled for bulk collection. Any future scheduled collector must demonstrate connection/load behavior and fit the existing serialized job budget before activation.

## Validation and release

All 203 selected tests pass on isolated Python 3.12.3, including the new staging suite and existing Finnhub news, recommendation, metric, rollout and research-consumer suites. Receipt: artifacts/finnhub-paid-tests-oct15.log. Synthetic fixtures derive field shapes from the official schemas, not a claim about paid-account live data. Tests cover all five scopes, whole-response persistence, identical cache repeat, source conflicts, malformed/absent values, stale/future dates, native scaling, failed refresh preservation, corrupted cache projections, default-off behavior, shared transport budget/auth/error behavior, and isolation from the existing analyst reader.

Initial run: 95 passed and one existing test expected the old unsupported-endpoint exception. It was updated to assert the new explicit disabled-preparation reason and retain a separate unreviewed-endpoint rejection. No live request occurred.

Not yet merged or deployed. Existing paid source selectors and subscriptions are unchanged. Latest previously verified parent-equity release is PR673/83a8c7f9, with ordinary propagation still pending. CAT, institutional and aggregate production-verification permissions remain pending; rejected operations were not retried. The previous percentage-only turn made no implementation progress; this turn adds source-contract evidence and executable replacement preparation. Full financial, institutional, transcript, analyst, valuation, all-consumer, sustained-capacity, final Massive, zero-FMP and billing gates remain open.
