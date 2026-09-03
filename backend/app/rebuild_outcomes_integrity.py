from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.db import SessionLocal, engine, ensure_outcome_ledger_schema
from app.models import ConfirmationScoreSnapshot, OutcomeCorrectionAudit, OutcomeEntry, OutcomeEvidenceProvenance
from app.services.outcome_integrity import (
    OUTCOME_AUDIT_VERSION,
    invalidate_outcome_persistent_caches,
    materialize_outcome_entry,
    materialize_outcome_horizons,
    qualifying_event_at,
)
from app.services.outcome_ledger import _project_directional_outcome_events
from app.services.price_lookup import hydrate_split_adjusted_ohlc


def _json_object(value: str | None) -> dict:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: str | None) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, ValueError):
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _persist_legacy_live_capture_provenance(db, snapshot: ConfirmationScoreSnapshot) -> int:
    """Recover the payload actually captured by a live scoring run.

    This is deliberately unavailable to historical-reconstruction snapshots:
    only a live row's database creation time proves the score payload existed.
    """
    if str(snapshot.calculation_type or "").strip().lower() != "live":
        return 0
    active_sources = _json_list(snapshot.active_sources_json)
    if not active_sources:
        return 0
    contributions = _json_object(snapshot.source_contributions_json)
    freshness = _json_object(snapshot.source_freshness_json)
    captured_at = qualifying_event_at(snapshot)
    created = 0
    for source_key in active_sources:
        payload = {
            "source": source_key,
            "contribution": contributions.get(source_key),
            "freshness": freshness.get(source_key),
            "input_hash": snapshot.input_hash,
        }
        payload_text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        payload_hash = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        evidence_id = f"legacy-live-capture:{source_key}:{payload_hash}"
        existing = db.execute(
            select(OutcomeEvidenceProvenance.id).where(
                OutcomeEvidenceProvenance.snapshot_id == snapshot.id,
                OutcomeEvidenceProvenance.source_key == source_key,
                OutcomeEvidenceProvenance.evidence_id == evidence_id,
            )
        ).scalar_one_or_none()
        if existing is not None:
            continue
        db.add(
            OutcomeEvidenceProvenance(
                snapshot_id=int(snapshot.id),
                source_key=source_key,
                evidence_id=evidence_id,
                available_at=captured_at,
                qualifying_event_at=captured_at,
                source_timestamp=None,
                source_payload_hash=payload_hash,
            )
        )
        created += 1
    db.flush()
    return created


def rebuild_outcomes_integrity(
    *,
    apply: bool = False,
    reason: str = "integrity audit reconstruction",
    hydrate_prices: bool = False,
    recover_live_provenance: bool = False,
) -> dict:
    """Rebuild canonical records without mutating immutable legacy snapshots.

    The default is a rollback-only dry run. Applying is deliberately explicit
    and writes a field-level audit trail before exposing new canonical rows.
    """
    ensure_outcome_ledger_schema(engine)
    report = {
        "audit_version": OUTCOME_AUDIT_VERSION,
        "mode": "apply" if apply else "dry_run",
        "snapshots_scanned": 0,
        "qualifying_live_events": 0,
        "symbols_hydrated": 0,
        "symbols_missing_prices": 0,
        "price_rows_hydrated": 0,
        "provenance_rows_recovered": 0,
        "already_materialized": 0,
        "entries_materialized": 0,
        "horizons_materialized": 0,
        "requires_provenance_or_ohlc": 0,
        "corrections_logged": 0,
        "caches_invalidated": 0,
    }
    with SessionLocal() as db:
        snapshots = db.execute(
            select(ConfirmationScoreSnapshot)
            .where(ConfirmationScoreSnapshot.calculation_type == "live")
            .order_by(ConfirmationScoreSnapshot.calculated_at.asc(), ConfirmationScoreSnapshot.id.asc())
        ).scalars().all()
        report["snapshots_scanned"] = len(snapshots)
        events = _project_directional_outcome_events(snapshots)
        qualifying_snapshots = [event.snapshot for event in events]
        report["qualifying_live_events"] = len(qualifying_snapshots)

        if hydrate_prices:
            if not apply:
                raise ValueError("--hydrate-prices requires --apply because provider hydration commits price-cache rows")
            earliest = min((qualifying_event_at(row).date() for row in qualifying_snapshots), default=datetime.now(timezone.utc).date())
            start_date = (earliest - timedelta(days=7)).isoformat()
            end_date = datetime.now(timezone.utc).date().isoformat()
            symbols = sorted({row.ticker_at_time.strip().upper() for row in qualifying_snapshots} | {"SPY"})
            for index, symbol in enumerate(symbols, start=1):
                try:
                    hydrated = hydrate_split_adjusted_ohlc(db, symbol, start_date, end_date)
                except Exception as exc:
                    print(f"price hydration failed {index}/{len(symbols)} {symbol}: {exc}", flush=True)
                    report["symbols_missing_prices"] += 1
                    continue
                report["price_rows_hydrated"] += int(hydrated)
                if hydrated:
                    report["symbols_hydrated"] += 1
                else:
                    report["symbols_missing_prices"] += 1
                if index == 1 or index % 25 == 0 or index == len(symbols):
                    print(f"price hydration {index}/{len(symbols)}", flush=True)

        for snapshot in qualifying_snapshots:
            if recover_live_provenance:
                report["provenance_rows_recovered"] += _persist_legacy_live_capture_provenance(db, snapshot)
            existed = db.execute(
                select(OutcomeEntry).where(OutcomeEntry.snapshot_id == snapshot.id)
            ).scalar_one_or_none()
            if existed is not None:
                report["already_materialized"] += 1
                entry = existed
            else:
                entry = materialize_outcome_entry(db, snapshot)
                if entry is None:
                    report["requires_provenance_or_ohlc"] += 1
                    continue
                report["entries_materialized"] += 1
                if snapshot.reference_price != entry.entry_price:
                    db.add(
                        OutcomeCorrectionAudit(
                            snapshot_id=int(snapshot.id),
                            record_type="confirmation_score_snapshot",
                            record_id=int(snapshot.id),
                            field_name="reference_price",
                            previous_value=None if snapshot.reference_price is None else str(snapshot.reference_price),
                            corrected_value=str(entry.entry_price),
                            correction_reason=reason,
                            audit_version=OUTCOME_AUDIT_VERSION,
                            correction_timestamp=datetime.now(timezone.utc),
                        )
                    )
                    report["corrections_logged"] += 1
            before = len(materialize_outcome_horizons(db, entry))
            report["horizons_materialized"] += before
        report["caches_invalidated"] = invalidate_outcome_persistent_caches(db)
        if apply:
            db.commit()
        else:
            db.rollback()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the canonical audited Outcomes ledger.")
    parser.add_argument("--apply", action="store_true", help="Persist canonical rows and correction audit records.")
    parser.add_argument("--hydrate-prices", action="store_true", help="Hydrate split-adjusted OHLC for every qualifying live symbol and SPY.")
    parser.add_argument(
        "--recover-live-provenance",
        action="store_true",
        help="Recover hashes of payloads captured by live scoring runs; historical-reconstruction rows remain excluded.",
    )
    parser.add_argument("--reason", default="integrity audit reconstruction")
    args = parser.parse_args()
    print(
        json.dumps(
            rebuild_outcomes_integrity(
                apply=args.apply,
                reason=args.reason,
                hydrate_prices=args.hydrate_prices,
                recover_live_provenance=args.recover_live_provenance,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
