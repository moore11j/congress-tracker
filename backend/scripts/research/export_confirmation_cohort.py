"""Read-only export of frozen public event anchors for offline model research.

Run against the application environment. Emits compressed JSON to stdout only;
does not hydrate prices, register models, refresh caches, or modify records.
"""
import base64
import hashlib
import json
import zlib
from datetime import datetime, timezone

from sqlalchemy import select
from app.db import SessionLocal
from app.models import ConfirmationScoreSnapshot, OutcomeEntry, OutcomeHorizonObservation, OutcomeEvidenceProvenance
from app.services.outcome_ledger import _project_directional_outcome_events, _headline_directionally_correct, _directionally_correct


def unpack(value):
    return json.loads(value) if value else {}


with SessionLocal() as db:
    conn = db.connection()
    if conn.dialect.name == "sqlite":
        conn.exec_driver_sql("PRAGMA query_only = ON")
    elif conn.dialect.name == "postgresql":
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
    else:
        raise RuntimeError("No read-only transaction policy for this database")
    snapshots = db.scalars(select(ConfirmationScoreSnapshot).where(ConfirmationScoreSnapshot.calculation_type == "live")).all()
    entries = {e.snapshot_id: e for e in db.scalars(select(OutcomeEntry)).all()}
    observations = {}
    for obs in db.scalars(select(OutcomeHorizonObservation)).all():
        observations.setdefault(obs.snapshot_id, []).append(obs)
    provenance = {}
    for p in db.scalars(select(OutcomeEvidenceProvenance)).all():
        item = provenance.setdefault(p.snapshot_id, {"count": 0, "latest_available_at": str(p.available_at), "late_count": 0})
        item["count"] += 1
        item["latest_available_at"] = max(item["latest_available_at"], str(p.available_at))
        item["late_count"] += int(p.available_at > p.qualifying_event_at)
    events = _project_directional_outcome_events(snapshots, verified_snapshot_ids=set(entries))
    rows = []
    for event in events:
        s = event.snapshot
        if s.id not in entries:
            continue
        e = entries[s.id]
        row = {
            "id": s.id, "security_id": s.security_id, "ticker": s.ticker_at_time,
            "score": s.score, "direction": s.direction, "strength": s.strength,
            "calculated_at": str(s.calculated_at), "market_date": str(s.market_date),
            "created_at": str(s.created_at), "methodology_id": s.methodology_version_id,
            "calculation_type": s.calculation_type, "active_source_count": s.active_source_count,
            "active_sources": unpack(s.active_sources_json), "sources": unpack(s.source_contributions_json),
            "freshness": unpack(s.source_freshness_json), "entry_date": str(e.entry_session_date),
            "entry_price": e.entry_price, "entry_source": e.entry_price_source,
            "evidence_cutoff_at": str(e.evidence_cutoff_at), "qualifying_event_at": str(e.qualifying_event_at),
            "closed_at": str(event.closed_at) if event.closed_at else None,
            "provenance": provenance.get(s.id), "outcomes": {},
        }
        for o in observations.get(s.id, []):
            row["outcomes"][str(o.horizon_days)] = {
                "raw_return": o.security_return_pct, "spy_return": o.benchmark_return_pct,
                "excess_return": o.excess_return_pct, "target_date": str(o.target_date),
                "price_date": str(o.security_session_date), "price_at": str(o.security_price_at),
                "recorded_at": str(o.created_at), "price": o.security_price, "source": o.security_price_source,
                "correct": _headline_directionally_correct(s.direction, o.security_return_pct, o.benchmark_return_pct),
                "raw_correct": _directionally_correct(s.direction, o.security_return_pct),
            }
        rows.append(row)
    payload = {"exported_at": datetime.now(timezone.utc).isoformat(), "events": rows}
    raw = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
    print("COHORT_SHA256=" + hashlib.sha256(raw).hexdigest())
    print("COHORT_BASE64=" + base64.b64encode(zlib.compress(raw)).decode())
    db.rollback()
