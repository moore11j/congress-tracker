"""Correct the Boeing brief using verified USAspending records; dry-run by default."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from sqlalchemy import text

from app.db import SessionLocal

DRAFT_ID = "rb_1789740520636_f5a9e2"
SLUG = "boeing-government-contract-backlog-ba-stock"
CORRECTION_ID = "boeing-contract-date-correction-2026-10-04"


def corrected_payload(original, patch, records, now):
    payload = deepcopy(original)
    article = payload["article"]
    if payload["id"] != DRAFT_ID or payload["status"] != "published" or article["slug"] != SLUG:
        raise ValueError("Correction is restricted to the published Boeing brief")
    if any(item.get("id") == CORRECTION_ID for item in payload.get("editorial_corrections", [])):
        return payload
    if {"slug", "premium_required", "required_plan", "access"}.intersection(patch):
        raise ValueError("Correction cannot change URL or access")
    article.update(deepcopy(patch))
    article.setdefault("schema", {}).update(headline=article["title"], dateModified=now)
    payload["updated_at"] = now

    def repair_context(value):
        if isinstance(value, dict):
            if isinstance(value.get("government_contracts"), dict):
                contracts = value["government_contracts"]
                for row in contracts.get("items", []):
                    source = records.get(row.get("source_url"))
                    if not source:
                        raise ValueError("Unverified contract in original context")
                    row["original_stored_date"] = row["award_date"]
                    row["award_date"] = source["date_signed"]
                    row["period_start"] = source["period_of_performance"]["start_date"]
                    row["period_end"] = source["period_of_performance"]["end_date"]
                    row["date_basis"] = "USAspending date_signed; verified 2026-10-04"
                for old, new in [("recent_count", "selected_record_count"), ("recent_award_amount", "historical_snapshot_amount")]:
                    if old in contracts:
                        contracts[new] = contracts.pop(old)
                contracts["date_basis"] = "Historical signing dates, not a latest-awards series."
                contracts["amount_basis"] = "Original stored award-level amounts; not new funding or remaining backlog."
            for child in value.values():
                repair_context(child)
        elif isinstance(value, list):
            for child in value:
                repair_context(child)

    repair_context(payload.get("research_context", {}))
    payload.setdefault("editorial_corrections", []).append({
        "id": CORRECTION_ID, "corrected_at": now,
        "basis": "Eight official USAspending award-detail records; date_signed separated from performance dates",
    })
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-dir", default="/data/editorial-backups")
    args = parser.parse_args()
    package = json.loads(Path(args.package).read_text(encoding="utf-8"))
    with SessionLocal() as db:
        row = db.execute(text("SELECT payload_json FROM research_brief_drafts WHERE id=:id AND status='published' AND slug=:slug"), {"id": DRAFT_ID, "slug": SLUG}).first()
        if not row:
            raise RuntimeError("Expected published brief not found")
        original = json.loads(row[0])
        now = datetime.now(timezone.utc).isoformat()
        corrected = corrected_payload(original, package["article_patch"], package["records"], now)
        if corrected == original:
            print(json.dumps({"status": "already_corrected"}))
            return
        digest = hashlib.sha256(row[0].encode()).hexdigest()
        if not args.apply:
            print(json.dumps({"status": "dry_run", "id": DRAFT_ID, "prior_sha256": digest, "title": corrected["article"]["title"], "records": len(package["records"])}))
            return
        if original["article"]["title"] != package["expected_title"] or original["updated_at"] != package["expected_updated_at"]:
            raise RuntimeError("Brief changed since review; nothing updated")
        backup_dir = Path(args.backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup = backup_dir / f"{DRAFT_ID}-{digest[:16]}.json"
        if not backup.exists():
            with backup.open("x", encoding="utf-8") as stream:
                stream.write(row[0])
        result = db.execute(text("UPDATE research_brief_drafts SET payload_json=:new, updated_at=:now WHERE id=:id AND status='published' AND payload_json=:old"), {"id": DRAFT_ID, "new": json.dumps(corrected, ensure_ascii=False), "now": now, "old": row[0]})
        if result.rowcount != 1:
            raise RuntimeError("Concurrent change; correction not applied")
        db.commit()
        print(json.dumps({"status": "corrected", "id": DRAFT_ID, "backup": str(backup), "updated_at": now}))


if __name__ == "__main__":
    main()
