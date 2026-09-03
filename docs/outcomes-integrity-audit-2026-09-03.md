# Walnut Markets Outcomes integrity audit

Audit version: `outcomes-integrity-v1`

Audit date: 2026-09-03

Historical-record writes: **none**

Containment status: **production Outcomes API and frontend route disabled on 2026-09-03 after owner review**

## Executive answer

**No. The currently published Outcomes history cannot be reproduced using only information and prices known at the stated point in time.** Of 13,771 directional events, 10,695 have a stored entry timestamp before the qualifying Confirmation Score timestamp. All 13,771 also lack a durable evidence manifest proving when every score input became public. The stored Outcomes must therefore remain audit-held until they are rebuilt from complete, authoritative price and evidence data.

The audit examined every stored Confirmation Score snapshot and every mature horizon that could be reconstructed from the production export. It did not delete, overwrite, or correct any production outcome row. The original values and the complete row-level findings are preserved in the JSON and CSV artifacts. After owner review, the uncertified Outcomes API was disabled, Vercel CDN/data caches were purged, and the existing production frontend deployment was rebuilt with the Outcomes route disabled. No dirty-worktree source was deployed.

## Immediate production containment

After the audit was reviewed, the reversible `OUTCOMES_LEDGER_ENABLED=false` hold was applied to the production API. All public Outcomes API endpoints returned `404 Outcome Ledger is not enabled`. The frontend initially continued to serve a stale server-rendered table, so the linked Vercel CDN and Data caches were purged and the existing production deployment was rebuilt with `NEXT_PUBLIC_OUTCOMES_LEDGER_ENABLED=0`. A fresh public-browser verification then showed the normal Page not found state and no Outcome prices or performance metrics. Historical database records remain intact for reconstruction and audit.

## Scope and counts

| Measure | Result |
|---|---:|
| Stored score snapshots inspected | 23,489 |
| Directional qualifying events audited | 13,771 |
| Stored mature horizon observations audited | 8,504 |
| Fully reproducible events | 0 |
| Failed or unverifiable events | 13,771 |
| Pass rate | 0.00% |

Failure categories overlap; a record can have more than one.

| Failure category | Count | Meaning |
|---|---:|---|
| Timestamp error | 10,695 | Stored entry timestamp is earlier than the qualifying event timestamp |
| Invalid entry price | 92 | A provider-backed reconstruction did not reconcile to the canonical executable official open |
| Invalid exit price | 0 confirmed | No out-of-range exit was independently established; most rows could not be verified because authoritative OHLC was absent |
| Look-ahead contamination | 10,695 confirmed for entry timing | Pre-event market prices were used; score-evidence look-ahead is separately unverifiable for all 13,771 records |
| Look-ahead evidence unverifiable | 13,771 | No evidence IDs and public-availability timestamps were persisted with the score |
| Corporate-action error | 0 individually proven | The old mixed adjusted/unadjusted basis makes the population unverifiable; split/ADR/action coverage is incomplete |
| Benchmark error | 137 | SPY entry or exit session/price differed from the aligned canonical calculation |
| Calculation difference | 88 | Recalculated return differed beyond rounding tolerance |
| Chart-only defects | 3 defect classes | Wrong price-path window, clipped scatter values, and synthetic fallback data |
| Same-day duplicate snapshots | 6,194 | Multiple snapshots for the same security and date |
| Overlapping same-direction events | 8,944 | Repeated signals inside the proposed 30-day qualification cooldown |
| Missing data | 13,685 | Complete canonical entry/horizon reconstruction was not possible |

The configured provider export requested 2,647 symbols but returned usable authoritative rows for only five symbols (450 rows total); 2,642 symbols were missing. Therefore the audit does **not** claim that every displayed numerical price was outside its daily high/low. It establishes a larger and more fundamental impossibility: 10,695 entries are timestamped before the signal existed. Ninety-two limited-provider cases also fail official-open reconciliation. The remaining never-traded/high-low allegation cannot responsibly be confirmed or dismissed until complete OHLC and corporate-action data are available.

## Architecture map before correction

```text
Raw source tables/caches
  Transaction + Filing (Congress disclosure)
  InsiderTransaction / InsiderTransactionNormalized (Form 4)
  InstitutionalFiling / InstitutionalPosition* (13F)
  AnalystConsensusSnapshot / AnalystGradeEvent / AnalystPriceTargetEvent
  FundamentalsCache / FundamentalsSnapshot
  GovernmentContract / GovernmentContractAction
  Event, signals, options, macro and ticker-context caches
            |
            v
confirmation_score.py builds the ten-source score bundle
            |
            v
capture_live_confirmation_score_snapshot()
            |
            v
ConfirmationScoreSnapshot
  calculated_at = qualifying-event time
  market_date
  legacy reference_price/reference_price_at/reference_price_source
            |
            v
outcome_ledger.py dynamically reads mutable PriceCache rows
  first close on/after market_date + 7/30/90/180/365 calendar days
  SPY close looked up independently
  return and excess return calculated at API request time
            |
            v
/api/outcomes/status
/api/outcomes/summary
/api/outcomes/snapshots
/api/admin/outcomes/*
            |
            v
OutcomeLedgerClient.tsx
  score-band bars, event scatter, table, detail and price-path chart
```

Before this work, there was no immutable outcome-entry ledger and no persisted horizon-observation ledger. Results were derived from `ConfirmationScoreSnapshot.reference_price` and mutable `PriceCache` data. The normal price hydrator retained close and volume but not a complete authoritative OHLC/provenance tuple. HTTP and application caches could therefore preserve old calculations after a cache mutation.

## One real record traced end to end

Production snapshot `6384`, BABA:

```text
SOURCE EVENT                unavailable: evidence identifiers were not persisted
QUALIFYING EVENT            ConfirmationScoreSnapshot id 6384
EVENT TIME                  2026-08-09 04:01:22.283989 UTC
LEGACY ENTRY SESSION        2026-08-04
LEGACY ENTRY TIME           2026-08-04 21:00:00 UTC
LEGACY ENTRY PRICE          128.97 from price_cache
7D SECURITY RETURN          -0.8684190122%
7D SPY RETURN               -0.0998275706%
7D EXCESS RETURN            -0.7685914416%
DATABASE                    no immutable outcome row; calculated from snapshot/cache
API                         public snapshot/summary endpoints
FRONTEND                    table, aggregate cards and event chart
```

The supposed entry is about four days before the score event. Under the canonical rule the earliest eligible entry is the 2026-08-10 official open. That price was unavailable in the exported authoritative data, so the corrected return is `MISSING_DATA`, not an invented substitute.

Other provider-backed examples:

| Event | Ticker | Event time UTC | Stored → audited entry | Stored → audited 7D return | Root cause |
|---:|---|---|---:|---:|---|
| 563 | AAON | 2026-08-05 17:10:12 | 95.55 → 92.05 | -10.2250% → -4.7583% | Pre-event entry, wrong executable price, calculation difference |
| 63 | AA | 2026-08-05 04:54:58 | 46.83 → 47.83 | +15.4815% → +7.0876% | Pre-event entry, wrong executable price, calculation difference |
| 743 | AAPL | 2026-07-20 04:07:09 | 326.59 → 333.51 | +3.1599% → +1.0195% | Wrong executable price, SPY misalignment, calculation difference |
| 757 | MU | 2026-07-20 18:22:27 | 865.46 → 925.35 | +4.0141% → -11.3276% | Next-session rule violated, SPY misalignment, sign-changing correction |

## Current and canonical entry methodology

The old implementation treated a score snapshot's `market_date` as the anchor, selected the latest cached price on or before that date, and stamped it at 21:00 UTC. This was effectively a same-day or stale daily close fallback. It did not establish that the score existed before that close, did not preserve a quote timestamp, and encoded the US close incorrectly during daylight-saving time.

The implemented canonical rule is intentionally conservative:

1. Persist event timestamps in UTC and evaluate US sessions in `America/New_York`.
2. A premarket event uses that session's official open.
3. An event at or after 09:30 New York time uses the next valid trading session's official open. This also covers during-market and after-hours events.
4. Weekend and holiday events use the next valid session open.
5. Entry and SPY use the same session and price type.
6. A row is accepted only when its split-adjusted official open is positive, lies inside the corresponding split-adjusted daily high/low, equals the stored entry within tolerance, and has `entry_timestamp >= qualifying_event_timestamp`.
7. Missing data remains missing. No backward date movement, close-price fallback, clamping, symbol substitution, or favorable-price selection is allowed.

## Horizon, return, benchmark and corporate-action methodology

Horizon labels mean calendar days from the executable entry session. For each of 7D, 30D, 90D, 180D and 365D, the target is `entry_session_date + horizon`. SPY defines the next valid US trading session on or after the target; both the security and SPY must have an official close on that exact session. The system never moves backward.

Returns are high-precision percentage points:

```text
security return = ((security exit / security entry) - 1) * 100
SPY return      = ((SPY exit / SPY entry) - 1) * 100
excess return   = security return - SPY return
```

Rounding occurs only in presentation. Outcomes now explicitly measure **split-adjusted price return**. Regular and special cash dividends are excluded. The price hydrator reconstructs all OHLC fields on one split-adjusted basis and labels that basis. Rows with any other adjustment status are rejected. Ticker changes, mergers, spin-offs, delistings and ADR-ratio changes are never silently mapped or filled; they remain missing until an authoritative security/action mapping exists. This prevents a split from fabricating a gain or loss and prevents adjusted-entry/unadjusted-exit mixing.

## Point-in-time evidence and look-ahead findings

All 13,771 historical directional records lack a durable per-contributor evidence manifest, so their score inputs cannot be proven point-in-time. This is `LOOKAHEAD_UNVERIFIABLE`, not a claim that every score was contaminated.

The confirmed look-ahead defect is price timing: 10,695 records use a market price preceding the score. Contributor queries also lacked a consistent upper bound. The corrected path requires event-time and ingestion-time fields to be no later than the score cutoff, rejects evidence whose `available_at` is later than the qualifying event, and stores contributor key, evidence ID, public-availability timestamp, source timestamp and payload hash. Congress uses disclosure availability rather than transaction date; Form 4 and 13F use filing availability rather than economic/reporting period; the same rule applies to analyst publication, earnings/fundamentals release, contracts, macro releases and technical observations.

Historical snapshots without this evidence cannot be certified retroactively from summary text alone.

## Duplicate-event methodology

The old qualification condition admitted a snapshot when either score or source count passed, rather than requiring both, and treated repeated daily snapshots as separate outcomes. The canonical event rule requires score at least 40 **and** at least one active source. A direction change qualifies immediately. A repeated same-direction signal qualifies only after a 30-day cooldown and at least a 10-point score move. Existing evidence remains visible in the audit export; it is not deleted.

## Aggregate metrics

These are pre-audit production calculations, not certified performance claims:

| Horizon | Observations | Directional accuracy | Avg directional return | Avg SPY | Avg directional excess |
|---|---:|---:|---:|---:|---:|
| 7D | 8,316 | 52.8740% | -1.1135% | -0.0069% | -1.0679% |
| 30D | 175 | 65.1429% | +2.1911% | +2.6521% | +0.8585% |
| 90D | 13 | 38.4615% | -6.9011% | +3.1130% | -8.2696% |
| 180D | 0 | — | — | — | — |
| 365D | 0 | — | — | — | — |

The limited provider-covered reconstruction produced the following **coverage-biased diagnostic subset**, not publishable after-metrics:

| Horizon | Audited subset | Accuracy | Avg directional return | Benchmarked | Avg SPY | Avg excess |
|---|---:|---:|---:|---:|---:|---:|
| 7D | 88 | 65.9091% | +1.1662% | 61 | +1.6060% | -0.3371% |
| 30D | 56 | 58.9286% | +0.7656% | 56 | +2.6146% | -1.2534% |
| 90D | 7 | 42.8571% | -4.6688% | 7 | +3.7942% | -6.4139% |

A valid whole-population “after” metric does not yet exist. Publishing the subset would introduce selection bias. The corrected aggregation excludes neutral calls, duplicates, missing prices and immature horizons from each horizon's denominator, computes mean/median/win-rate from persisted verified observations only, and reports denominators explicitly. Bearish accuracy uses a negative security return; benchmark-relative performance remains a separate measure.

## Frontend and cache audit

The prior detail chart requested a trailing current 30-day window rather than the selected event's actual entry-to-horizon series. The event scatter plotted directional returns and clipped values at ±25%, which could hide magnitude or invert the meaning of bearish raw returns. A demo branch could synthesize returns. These are corrected locally: the detail chart calls the exact snapshot price-path endpoint, the scatter plots raw API returns with a dynamic axis, missing values stay missing, and percentages are formatted exactly once (`18` means `18%`). Desktop and mobile layouts were rendered against a deterministic verified fixture.

Application and frontend cache lifetimes are reduced to five minutes. Rebuilds explicitly invalidate Outcomes status, snapshot, summary, ticker-summary and dependent strategy cache keys. Legacy unverified production returns now fail closed as `requires_reconstruction`; they are not silently served as verified data.

## Strategies cross-audit

Strategies shares `PriceCache`, historical price helpers, market-session resolution and Confirmation Score snapshot data. It was affected. Event-driven strategy candidates previously used the next close, while Confirmation strategies could reuse the legacy snapshot reference price. The local correction resolves strategy entry to the same official-open, split-adjusted canonical basis; Confirmation strategies require a verified `OutcomeEntry`. This prevents Outcomes and Strategies from diverging on executable-entry semantics.

## Database safeguards and audit trail

Four immutable/auditable tables were added locally:

- `outcome_entries`: unique snapshot and deterministic entry key; positive security/SPY prices; non-null event, cutoff and entry timestamps; explicit source, price type, adjustment and methodology versions.
- `outcome_horizon_observations`: unique `(entry_id, horizon_days)` and `(snapshot_id, horizon_days)`; positive prices; horizon restricted to 7/30/90/180/365; aligned security/SPY sessions; persisted returns.
- `outcome_evidence_provenance`: unique evidence record per snapshot/source; public-availability and qualifying-event timestamps plus payload hash.
- `outcome_correction_audit`: append-only previous/corrected payloads, reason, timestamp and audit version.

The reconstruction command is dry-run and transaction-rollback by default. `--apply` is explicit and has not been run. Legacy snapshot values are preserved; corrections create versioned ledger rows and audit records rather than rewriting history in place.

## Tests and validation

Backend: **55 passed** across the Outcomes integrity and ledger suites. The new integrity file contains 30 required cases covering during-market, after-market, premarket, weekend, holiday, DST, split, reverse split, dividend semantics, ticker change, delisting, missing data, invalid daily price, pre-event entry, delayed Congress/Form 4/13F availability, aligned SPY, all five horizons, precision/percentage handling, exact price path/chart payload, duplicates, aggregate win rate/excess return, immature-horizon exclusion and corporate-action adjustment.

Frontend Outcomes: **9 passed**. Production frontend build: **passed** (62 static pages generated). The repository-wide frontend suite was also observed, but has unrelated pre-existing failures in feed/navigation/monitoring/watchlist assertions; all Outcomes tests passed. Python compilation passed.

Local visual validation passed at desktop and 390px mobile widths. No corrected historical ledger was deployed; only the reversible production containment described above was applied.

## Files changed for this audit

Backend additions: `audit_outcomes_integrity.py`, `rebuild_outcomes_integrity.py`, `services/outcome_integrity.py`, and `tests/test_outcome_integrity.py`.

Backend integrations: `models.py`, `db.py`, `services/outcome_ledger.py`, `services/confirmation_score.py`, `services/price_lookup.py`, `services/strategy_candidate_resolver.py`, `ingest_run.py`, `main.py`, and the existing Outcome ledger tests.

Frontend additions: `tests/outcome-integrity.test.mjs` and the local-only visual fixture server. Frontend integrations: `components/outcomes/OutcomeLedgerClient.tsx`, `lib/api.ts`, and the existing Outcome ledger tests.

Audit evidence: the production JSON/CSV exports and desktop/mobile PNG captures in `backend/artifacts/`.

## Proposed public methodology copy

> An Outcome begins when a live, point-in-time Confirmation Score first qualifies, changes direction, or requalifies after the published cooldown. Evidence is frozen at that timestamp. Premarket events use that session's official open; events at or after 9:30 a.m. New York time use the next trading session's official open.
>
> The 7D, 30D, 90D, 180D and 365D labels are calendar-day targets measured from the executable entry session. Each uses the first valid market close on or after the target date. SPY uses the identical entry and exit sessions. Returns are split-adjusted price returns; dividends are excluded. Missing prices remain missing, and incomplete horizons are not counted.
>
> Historical inputs are evaluated by when they became publicly available—not by an earlier transaction or economic date—so disclosures and filings cannot influence an Outcome before publication. Walnut publishes the return methodology but not proprietary Confirmation Score weights.

## Required review gate

Before any production correction or deployment:

1. Restore complete authoritative split and OHLC coverage for the 2,642 missing symbols, including delisted/ticker-change/action mappings.
2. Recover source-specific evidence availability where possible; keep unrecoverable events audit-held.
3. Rerun the full row-level audit and require zero temporal, price-basis, benchmark and calculation failures.
4. Review the correction manifest and coverage-neutral aggregate metrics.
5. Apply the versioned reconstruction to staging, validate caches/API/chart screenshots there, and obtain approval before production.

The current containment does not authorize production overwrite, deletion, commit, merge, re-enabling Outcomes, or publication of provisional metrics.
