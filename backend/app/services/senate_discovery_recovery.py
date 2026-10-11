"""Retain undiscovered Senate dates across outages without widening request limits."""
import json
from datetime import date, timedelta

from app.models import InsightsSnapshot
from app.services.feed_source_control import FeedSourceControl

KEY = "senate-discovery:recovery:v1"
SOURCE = "https://efdsearch.senate.gov/search/report/data/"


def plan_window(db, *, start, end, now):
    row = db.get(InsightsSnapshot, KEY)
    if row is None:
        control = db.get(FeedSourceControl, "senate_ptr")
        anchor = min(start, control.publish_since) if control and control.publish_since else start
        state = {"version": 1, "anchor": anchor.isoformat(), "covered_through": None}
        row = InsightsSnapshot(kind=KEY, source="official_senate", fetched_at=now,
                              payload_json=json.dumps(state))
        db.add(row)
        # Save the anchor before HTTP: an interrupted/denied run cannot lose it.
        db.commit()
    else:
        state = json.loads(row.payload_json)
        if state.get("version") != 1:
            raise ValueError("Unknown Senate recovery state version")
        anchor = date.fromisoformat(state["anchor"])
    covered = date.fromisoformat(state["covered_through"]) if state.get("covered_through") else None
    if anchor > end or (covered and not anchor <= covered <= end):
        raise ValueError("Invalid Senate recovery date order")
    first = max(anchor, min(start, covered or anchor))
    last = min(end, first + timedelta(days=93))
    return {"start": first.isoformat(), "end": last.isoformat(),
            "target_end": end.isoformat(), "anchor": anchor.isoformat(),
            "previous_covered_through": state["covered_through"],
            "remaining_days": (end-last).days}


def finish_window(db, *, plan, receipt, errors, run_id, now):
    # A complete HTTP search is insufficient if staging any discovered row failed.
    complete = (isinstance(receipt, dict) and receipt.get("complete") is True
                and receipt.get("source") == SOURCE
                and receipt.get("start") == plan["start"] and receipt.get("end") == plan["end"]
                and type(receipt.get("reported_total")) is int
                and receipt["reported_total"] >= 0
                and type(receipt.get("discovered")) is int
                and receipt["discovered"] == receipt["reported_total"]
                and isinstance(receipt.get("page_sha256"), list) and bool(receipt["page_sha256"])
                and not any(error.get("feed") == "senate_ptr" for error in errors))
    row = db.get(InsightsSnapshot, KEY, populate_existing=True, with_for_update=True)
    state = json.loads(row.payload_json)
    if state.get("anchor") != plan["anchor"] or state.get("covered_through") != plan["previous_covered_through"]:
        raise ValueError("Senate recovery state changed during collection")
    if complete:
        state["covered_through"] = plan["end"]
        state["last_complete_run_id"] = run_id
        state["last_complete_at"] = now.isoformat()
    state["last_attempt_run_id"] = run_id
    state["last_attempt_at"] = now.isoformat()
    row.payload_json = json.dumps(state, sort_keys=True)
    row.fetched_at = now
    return {**plan, "advanced": bool(complete), "covered_through": state["covered_through"],
            "caught_up": bool(complete and plan["remaining_days"] == 0)}
