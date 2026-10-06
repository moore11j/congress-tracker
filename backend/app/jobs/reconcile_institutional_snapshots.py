"""Guarded SEC snapshot repair. Plan first; apply only the exact reviewed plan.

Backs up source positions and affected derived projections before a transaction
per holder. Never touches alert deliveries, predictions, strategies or returns.
"""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select, text
from app.db import SessionLocal
from app.models import Event, InstitutionalFiling, InstitutionalPosition, InstitutionalPositionChange, InstitutionalActivityEvent, InstitutionalSymbolSummary
from app.services import institutional_activity as svc
from app.services.institutional_sec_snapshot import install_snapshot, snapshot_digest


def serialize(row):
    return {c.name: getattr(row, c.name) for c in row.__table__.columns}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def state(db, filing):
    positions = db.scalars(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id).order_by(InstitutionalPosition.id)).all()
    return {"identity": [filing.id, filing.cik, filing.accession_number, filing.report_year, filing.report_quarter, filing.superseded_by],
            "metadata": filing.raw_metadata_json, "positions": [serialize(r) for r in positions]}


def following(db, filing):
    year, quarter = (filing.report_year + 1, 1) if filing.report_quarter == 4 else (filing.report_year, filing.report_quarter + 1)
    return svc.get_canonical_filing_for_holder_period(db, cik=filing.cik, report_year=year, report_quarter=quarter)


def backup_projection(db, filing, snapshot=None):
    changes = db.scalars(select(InstitutionalPositionChange).where(InstitutionalPositionChange.cik == filing.cik,
        InstitutionalPositionChange.report_year == filing.report_year, InstitutionalPositionChange.report_quarter == filing.report_quarter)).all()
    positions = db.scalars(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)).all()
    symbols = {r.normalized_symbol for r in [*changes, *positions] if r.normalized_symbol}
    if snapshot:
        symbols.update(db.scalars(select(InstitutionalPosition.normalized_symbol).where(
            InstitutionalPosition.cusip.in_({r.get("cusip") for r in snapshot["rows"]}),
            InstitutionalPosition.normalized_symbol.is_not(None))).all())
    activities = db.scalars(select(InstitutionalActivityEvent).where(InstitutionalActivityEvent.normalized_symbol.in_(symbols),
        InstitutionalActivityEvent.report_year == filing.report_year, InstitutionalActivityEvent.report_quarter == filing.report_quarter)).all()
    summaries = db.scalars(select(InstitutionalSymbolSummary).where(InstitutionalSymbolSummary.normalized_symbol.in_(symbols),
        InstitutionalSymbolSummary.report_year == filing.report_year, InstitutionalSymbolSummary.report_quarter == filing.report_quarter)).all()
    sources = [f"institutional:{r.id}:{r.event_type}:{r.report_year}q{r.report_quarter}" for r in activities]
    events = db.scalars(select(Event).where(Event.source_provider == svc.INSTITUTIONAL_EVENT_SOURCE,Event.source_filing_id.in_(sources))).all()
    return {"filing": serialize(filing), "positions": [serialize(r) for r in positions],
            "changes": [serialize(r) for r in changes], "activities": [serialize(r) for r in activities],
            "summaries": [serialize(r) for r in summaries], "events": [serialize(r) for r in events]}


def rebuild(db, filing):
    # Installing an older quarter must not turn missing baseline coverage into
    # thousands of newly opened positions.
    if svc._prior_positions_for_filing(db, filing):
        return svc.process_filing_changes_and_events(db, filing, reset_existing=True)
    symbols = svc._reset_holder_period_changes_and_activity(db, filing)
    symbols.update(db.scalars(select(InstitutionalPosition.normalized_symbol).where(
        InstitutionalPosition.filing_id == filing.id, InstitutionalPosition.normalized_symbol.is_not(None))).all())
    result = {"changes": 0, "summaries": 0, "activity_events": 0, "feed_events": 0,
              "comparison_unavailable": "No stored adjacent-quarter baseline"}
    for symbol in sorted(symbols):
        summary = svc.refresh_symbol_summary(db, symbol, filing.report_year, filing.report_quarter)
        if summary:
            result["summaries"] += 1
            result["activity_events"] += svc.generate_activity_events_for_symbol(db, summary)
            db.flush()
            result["feed_events"] += svc.materialize_feed_events_for_symbol(db, summary)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-plan-sha")
    parser.add_argument("--backup-dir", default="/data/institutional-reconciliation")
    args = parser.parse_args()
    bundle = json.loads(Path(args.bundle).read_text(encoding="utf-8"))
    if not args.apply:
        plan = {"bundle_sha256": digest(bundle), "filings": []}
        with SessionLocal() as db:
            for key, snap in sorted(bundle.items(), key=lambda item: (item[1]["year"], item[1]["quarter"], int(item[0]))):
                filing = db.get(InstitutionalFiling, int(key))
                if not filing or filing.superseded_by:
                    raise ValueError("Filing no longer canonical")
                dependent = following(db, filing)
                plan["filings"].append({"id": filing.id, "cik": filing.cik, "period": [filing.report_year,filing.report_quarter],
                    "state_sha256": digest(state(db, filing)), "snapshot_rows": len(snap["rows"]),
                    "next_filing_id": dependent.id if dependent else None,
                    "next_form": dependent.form_type if dependent else None})
        Path(args.plan).write_text(json.dumps(plan, indent=2), encoding="utf-8")
        print(json.dumps({"status":"dry_run", "plan_sha256":digest(plan), **plan}), flush=True)
        return
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    if digest(plan) != args.expected_plan_sha or digest(bundle) != plan["bundle_sha256"]:
        raise ValueError("Reviewed plan or source bundle changed")
    destination = Path(args.backup_dir); destination.mkdir(parents=True, exist_ok=True)
    for item in plan["filings"]:
        with SessionLocal() as db:
            if db.get_bind().dialect.name == "postgresql":
                db.execute(text("SET LOCAL lock_timeout='5s'"))
                db.execute(text("SET LOCAL statement_timeout='120s'"))
            filing = db.scalar(select(InstitutionalFiling).where(InstitutionalFiling.id==item["id"]).with_for_update())
            snap = bundle[str(filing.id)]
            meta = json.loads(filing.raw_metadata_json or "{}")
            if meta.get("_walnut_position_repair_sha") == snap["sha256"]:
                print(json.dumps({"id":filing.id,"status":"already_applied"}), flush=True); continue
            if digest(state(db, filing)) != item["state_sha256"]:
                raise ValueError(f"Filing {filing.id} changed since reviewed plan")
            dependent = following(db, filing)
            if (dependent.id if dependent else None) != item["next_filing_id"]:
                raise ValueError("Following quarter changed")
            if dependent and svc._filing_is_amendment(dependent) and str(dependent.id) not in bundle:
                svc._require_reconciled_amendment(dependent)
            backup = {"captured_at":datetime.now(timezone.utc).isoformat(), "source_snapshot":snap,
                      "before":backup_projection(db,filing,snap), "following":backup_projection(db,dependent,snap) if dependent else None}
            target=destination / f"filing-{filing.id}-{digest(backup)[:16]}.json"
            with target.open("x",encoding="utf-8") as stream:
                json.dump(backup,stream,sort_keys=True,default=str)
            install_snapshot(db,filing,snap)
            result=rebuild(db,filing)
            # Recompute the next quarter only when it isn't itself awaiting an
            # installation from this ordered plan.
            if dependent and str(dependent.id) not in bundle:
                rebuild(db,dependent)
            meta=json.loads(filing.raw_metadata_json);meta["_walnut_position_repair_sha"]=snap["sha256"]
            filing.raw_metadata_json=json.dumps(meta,sort_keys=True)
            db.commit()
            print(json.dumps({"id":filing.id,"status":"applied","backup":str(target),**result}),flush=True)


if __name__ == "__main__":
    main()
