"""Apply the owner-approved, SEC-verified September 27 ownership correction.

Dry-run by default. Preserves publication state, URL, entitlements and original
publication date. Backs up the exact prior payload before a guarded update.
"""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from app.db import SessionLocal

DRAFT_ID = "rb_1789221682029_a265ac"
SLUG = "who-is-buying-nvidia-stock-in-the-latest-13f-filings"
CORRECTION_ID = "sec-ownership-evidence-2026-09-27"
PATCH_PATH = Path(__file__).parent / "data" / "nvda_ownership_correction_20260927.json"


def corrected_payload(original, now):
    payload = json.loads(json.dumps(original))
    article = payload["article"]
    if payload["id"] != DRAFT_ID or payload["status"] != "published" or article["slug"] != SLUG:
        raise ValueError("Correction is scoped to the published NVIDIA ownership brief")
    patch = json.loads(PATCH_PATH.read_text(encoding="utf-8"))
    assert not {"slug", "premium_required", "required_plan", "access"}.intersection(patch)
    article.update(patch)
    payload["updated_at"] = now
    payload.setdefault("editorial_corrections", []).append({
        "id": CORRECTION_ID, "corrected_at": now,
        "basis": "SEC original Q1/Q2 2026 information tables and Amundi NEW HOLDINGS amendment",
    })
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-dir", default="/data/editorial-backups")
    args = parser.parse_args()
    with SessionLocal() as db:
        row = db.execute(text("SELECT payload_json FROM research_brief_drafts WHERE id=:id AND status='published' AND slug=:slug"),
                         {"id": DRAFT_ID, "slug": SLUG}).first()
        if not row:
            raise RuntimeError("Expected published brief not found; nothing changed")
        original = json.loads(row[0])
        if any(item.get("id") == CORRECTION_ID for item in original.get("editorial_corrections", [])):
            print(json.dumps({"status": "already_corrected", "id": DRAFT_ID}))
            return
        now = datetime.now(timezone.utc).isoformat()
        corrected = corrected_payload(original, now)
        digest = hashlib.sha256(row[0].encode()).hexdigest()
        if not args.apply:
            print(json.dumps({"status": "dry_run", "id": DRAFT_ID, "slug": SLUG,
                              "prior_sha256": digest, "sections": len(corrected["article"]["sections"]),
                              "sources": len(corrected["article"]["source_links"])}))
            return
        backup_dir = Path(args.backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup = backup_dir / f"{DRAFT_ID}-{digest[:16]}.json"
        if not backup.exists():
            with backup.open("x", encoding="utf-8") as stream:
                stream.write(row[0])
        result = db.execute(text("UPDATE research_brief_drafts SET payload_json=:new, updated_at=:now WHERE id=:id AND status='published' AND payload_json=:old"),
                            {"id": DRAFT_ID, "new": json.dumps(corrected, ensure_ascii=False), "now": now, "old": row[0]})
        if result.rowcount != 1:
            raise RuntimeError("Brief changed concurrently; correction was not applied")
        db.commit()
        print(json.dumps({"status": "corrected", "id": DRAFT_ID, "prior_sha256": digest,
                          "backup": str(backup), "updated_at": now}))


if __name__ == "__main__":
    main()
