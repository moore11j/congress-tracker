# Portfolio Data Repair Runbook

Portfolio simulation reads must remain read-only. Use these CLI paths for repair,
diagnostics, and targeted recompute after ingestion or provider outages.

## Daily Trade Outcome Sweep

Retry missing Congress trade outcomes and safe failed statuses:

```bash
python -m app.compute_trade_outcomes --event-type congress_trade --lookback-days 1095 --only-missing --retry-failed-statuses no_data,no_current_price,provider_429 --log-level INFO
```

The command reports scanned, eligible, inserted, updated, skipped, and failed
status counts. `--only-missing` includes disclosures with no `trade_outcomes`
row, while `--retry-failed-statuses` allows known transient failures to be
recomputed in the same sweep.

## Portfolio Price Preflight

Preview missing execution-price coverage before writing portfolio runs:

```bash
python -m app.compute_replicated_portfolios --entity-type congress_member --entity-id E000296 --lookback-days 365 --mode realistic_disclosure_lag --price-preflight --dry-run --verbose --curve-diagnostics
```

Allow the preflight to backfill cacheable prices only in an explicit write run:

```bash
python -m app.compute_replicated_portfolios --entity-type congress_member --entity-id E000296 --lookback-days 365 --mode realistic_disclosure_lag --price-preflight --price-preflight-backfill --replace-existing --apply --verbose
```

Use targeted member runs first. Do not run an all-member recompute until the
targeted diagnostics show acceptable skip categories and curve quality.

## Skip Categories

Portfolio diagnostics separate preventable skips from expected exclusions:

- `missing_execution_price`: valid equity disclosure, but no bounded execution
  price was available after cache/provider fallback.
- `unresolved_symbol`: no safe ticker mapping from symbol or exact local
  security/company-name metadata.
- `non_equity_asset`: options, bonds, private funds, and other non-equity
  disclosures excluded from the equity simulation.
- `sale_without_position`: sale could not be matched to a simulated prior
  holding after warmup reconstruction.

Non-equity disclosures are not simulation failures. User-facing copy should
describe them as non-simulatable assets.

## Warmup Validation

For selected portfolio windows, the simulation may load pre-window disclosures
only to reconstruct opening holdings. The visible chart still starts at the
selected lookback date. Persisted payloads include:

- `warmup_start_date`
- `visible_start_date`
- `opening_positions_count`
- `sale_without_position_before_warmup`
- `sale_without_position_after_warmup`
- `opening_position_estimated`

Estimated opening positions are not enabled by default and must not be blended
into public results without visible disclosure.
