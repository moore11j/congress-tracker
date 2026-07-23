# Market Pressure Scoring

## Audit Findings

Market Pressure is a visualization layer over Walnut's canonical confirmation score. The canonical implementation is `backend/app/services/confirmation_score.py`.

The canonical source order is:

- `congress`
- `insiders`
- `signals`
- `price_volume`
- `fundamentals`
- `options_flow`
- `government_contracts`
- `institutional_activity`
- `macro_positioning`

The batch entry point is `get_confirmation_score_bundles_for_tickers(db, tickers, lookback_days=30)`. It builds one confirmation bundle per symbol from existing database/cache-backed source summaries. The Market Pressure endpoint calls this batch service once for the requested symbol set and does not create a second score.

Canonical score bands are:

- `inactive`: 0-19
- `weak`: 20-39
- `moderate`: 40-59
- `strong`: 60-79
- `exceptional`: 80-100

The existing bundle direction values are `bullish`, `bearish`, `neutral`, and `mixed`. `mixed` is the backward-compatible wire value for the canonical conflicted state. User-facing surfaces should render it as `Conflicted`, not as `Mixed 50`, `Mix 65`, or any other score bucket.

## Canonical Confirmation Direction Classification

Canonical classification lives only in `backend/app/services/confirmation_score.py`:

- `classify_confirmation_direction(...)`
- `_bundle_direction(...)`
- `CONFIRMATION_CLASSIFICATION_VERSION`

Ticker Context, Screener, Signals, monitoring, and Market Pressure consume the canonical confirmation bundle through `get_confirmation_score_bundles_for_tickers(...)`, `get_confirmation_score_bundle_for_ticker(...)`, or `slim_confirmation_score_bundle(...)`. Frontend components may style and label the returned direction, but must not recalculate direction from score bands or source disagreements.

Previous rule:

- Any active non-neutral `mixed` source, or any active bullish source combined with any active bearish source, made the bundle direction `mixed`.

New rule:

- Bullish and bearish source evidence is weighted centrally from strength, quality, freshness, and existing source contribution.
- `mixed`/conflicted is returned only when material bullish and bearish evidence are both present and the directional edge is narrow.
- One weaker opposing layer no longer forces conflict when the other side has a defensible evidence edge.
- Weak or non-directional evidence resolves `neutral`.
- Missing evidence is ignored and never counted as neutral or opposing evidence.

Central constants:

- `MATERIAL_DIRECTIONAL_EVIDENCE_MIN`
- `DEFENSIBLE_DIRECTIONAL_MARGIN`
- `CONFLICT_DIRECTIONAL_MARGIN`
- `CONFLICT_DIRECTIONAL_EDGE_RATIO`
- `MATERIAL_EVIDENCE_MAX_FRESHNESS_DAYS`

Score weights now use `confirmation_direction_v3`. The numeric Confirmation Score remains the public strength/quality score, while direction remains the canonical qualitative classification. V3 gives institutional accumulation a dedicated score lane, rewards newly opened institutional positions, gives bullish fundamentals more constructive weight, and dampens insider selling relative to insider buying. Newly generated bundles and slim summaries include classification version `confirmation_direction_v3`. Historical monitoring and saved-screen rows without this metadata should be treated as legacy snapshots rather than silently equivalent classifications.

## Endpoint

The Phase 2 endpoint is:

`GET /api/market-pressure`

Query parameters:

- `universe=sp500|nasdaq100|all_us|watchlist`
- `period=1d|5d|1m|3m|ytd|1y`
- `view=market_pressure|hidden_accumulation|fragile_winners|crowded_trades|rotation`

Invalid parameters fall back to defaults and are listed in `warnings`.

## Canonical Data Source

Confirmation evidence comes from the existing confirmation score service. Price performance comes from `price_cache.close`, the same cached daily close table used by Walnut price history surfaces. Index membership comes from the reusable `index_memberships` table. The endpoint does not make provider calls.

Company identity and sector hydration use cached `ticker_meta`, `securities`, and `fundamentals_cache` rows. Sector summaries are equal-weight averages of the returned tiles' price change percentages. They are not index returns.

## Market Pressure Classification Rules

Each ticker resolves to:

- `bullish`: canonical direction is bullish and confirmation score is at least the moderate band.
- `bearish`: canonical direction is bearish and confirmation score is at least the moderate band.
- `neutral`: canonical evidence exists, but it does not meet bullish or bearish thresholds.
- `conflicted`: canonical direction is mixed.
- `unavailable`: canonical evidence is insufficient or inactive.

Missing evidence never counts as neutral.

Confirmation strength derives from canonical bands:

- weak band -> `weak`
- moderate band -> `moderate`
- strong or exceptional band -> `strong`
- inactive or unavailable -> `null`

The endpoint does not expose a public pressure score.

## Divergence Rules

Each ticker resolves to:

- `hidden_accumulation`: price change is flat or negative, confirmation is bullish, strength is moderate/strong, at least two active canonical sources exist, and evidence is not stale.
- `fragile_winner`: price change is positive, confirmation is bearish or conflicted, strength is moderate/strong, at least two active canonical sources exist, and evidence is not stale.
- `aligned_bullish`: price change is positive and confirmation is bullish.
- `aligned_bearish`: price change is negative and confirmation is bearish.
- `conflicted`: canonical confirmation layers are conflicted.
- `none`: price and confirmation are sufficient but no divergence rule matches.
- `unavailable`: price or canonical confirmation data is insufficient or stale.

Pressure trend is `null` because the repository does not currently expose comparable historical confirmation snapshots with stable scoring semantics.

## Freshness

Confirmation freshness remains the canonical current window of 30 days. Changing `period` only changes price performance comparison; it does not change confirmation lookback semantics.

Layer freshness is based on the source `freshness_days` already present in confirmation bundles. A source older than 30 days is rendered as `stale`. Exact per-source timestamps are not always present in the canonical bundle, so the endpoint derives an approximate `asOf` from `generatedAt - freshness_days` when freshness is available and otherwise returns `null`.

## Missing Data

Provider/cache gaps are represented as unavailable layer states. A ticker can appear with partial data. Missing optional layers do not exclude a symbol and are not treated as bearish, bullish, or neutral evidence.

## Entitlement and Canonical Score Semantics

Market Pressure is restricted to Pro and Admin users. It uses Walnut's complete canonical Confirmation Score and does not generate entitlement-specific partial scores. Unauthorized tiers receive no Market Pressure result data.

Authorized:

- Pro
- Admin

Unauthorized:

- Logged out
- Free
- Premium
- Suspended users

The backend checks authorization before loading universe membership, cached prices, identities, confirmation bundles, divergences, sectors, or summaries. Unauthorized requests return the application's authentication-required or Pro-required error response rather than an empty successful map.

Pro and Admin users receive the same complete Market Pressure payload. Missing provider data is returned as unavailable and is never treated as active or neutral evidence.

## Supported Modes

Supported views:

- `market_pressure`
- `hidden_accumulation`
- `fragile_winners`

Unsupported views:

- `crowded_trades`: requires defensible positioning, concentration, extension, or overbought data beyond high confirmation plus positive return.
- `rotation`: requires historical sector-level confirmation snapshots or reproducible historical aggregates.

## Index Universe Sources

Repository audit findings:

- No pre-existing security-master index constituent table was found.
- No existing S&P 500 or Nasdaq-100 static dataset was found.
- The screener uses cached fundamentals plus the FMP company screener integration, but that is not canonical index membership.
- Existing FMP/provider guardrails and background-refresh conventions exist and are reused.
- Existing ETF proxy runbooks are for Insights price/index proxy surfaces, not constituent membership.

Canonical source:

- Runtime source: `index_memberships`, keyed by `index_code`, `symbol`, `effective_from`, `effective_to`, `source`, `source_as_of`, `refreshed_at`, and `is_active`.
- Refresh source: existing FMP stable index constituent endpoints, called only by `backend/app/jobs/refresh_index_memberships.py` outside the Market Pressure request path.
- Validation: active membership is available only when records exist, count is defensible, source metadata exists, and the dataset passes validation. S&P 500 accepts defensible share-class variation rather than an exact 500. Nasdaq-100 accepts defensible share-class variation rather than an exact 100.
- Safety: empty or malformed refresh responses are rejected and do not wipe existing active memberships. Removed members are end-dated rather than deleted.

Supported universes:

- `watchlist`: uses the authenticated user's first owned watchlist by existing owner scoping.
- `sp500`: enabled when active `index_memberships` rows pass validation.
- `nasdaq100`: enabled when active `index_memberships` rows pass validation.

Unsupported universe:

- `all_us`: unsupported until Walnut can return the complete eligible universe without hidden sampling.

Unsupported universes return explicit capabilities and warnings rather than sampled substitutes.

## Performance Architecture

The endpoint performs batched work:

- One membership query when `universe=watchlist`, `universe=sp500`, or `universe=nasdaq100`.
- One cached price-history query for the requested symbol set and period window.
- One batch identity hydration pass across cached identity tables.
- One canonical batch confirmation call for the requested symbol set.
- One in-memory classification and serialization pass.

The endpoint does not call FMP, options, filings, or market data providers. There is no per-symbol frontend request and no per-tile detail fetch.

No cross-user payload cache is added in Phase 2. Responses are request scoped and the endpoint returns `Cache-Control: private, no-store` because Market Pressure payloads contain full Pro intelligence. A future shared cache for index universes must check Pro/Admin authorization before cache retrieval and must keep user-specific watchlist keys scoped by user.

## Sorting

Tiles are sorted within each sector by:

1. Divergence relevance
2. Confirmation strength
3. Absolute price move
4. Symbol

Sectors sort alphabetically, with `Unclassified` last.

## Known Limitations

- Broad index universes need canonical membership storage before they can be enabled.
- Rotation and trend views need historical confirmation snapshots with scoring version and timestamp metadata.
- Layer `asOf` is approximate when the canonical bundle exposes only `freshness_days`.
- Price performance uses cached close values from `price_cache.close`; no adjusted-close column is currently available in that table.
- The watchlist universe currently uses the user's first owned watchlist because no separate primary/default watchlist marker was found.
