"""October 5 reviewed filing corrections. Dry-run unless explicitly applied.

Only the twelve listed published articles may change. Preserve publication
dates, URLs, access, and all other draft state. Never send publication emails.
"""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import text
from app.db import SessionLocal

CORRECTION_ID = "sec-matched-ownership-2026-10-05"
PATCHES = Path(__file__).parent / "data" / "institutional_brief_corrections_20261005.json"


def article_digest(article):
    return hashlib.sha256(json.dumps(article, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def corrected_payload(original, correction, now):
    result = json.loads(json.dumps(original))
    if result["id"] != correction["id"] or result["status"] != "published" or result["article"]["slug"] != correction["slug"]:
        raise ValueError("Published article identity mismatch")
    if any(item.get("id") == CORRECTION_ID for item in result.get("editorial_corrections", [])):
        return result
    if article_digest(result["article"]) != correction["expected_article_sha256"]:
        raise ValueError("Article changed since review; no correction applied")
    if {"slug", "premium_required", "required_plan", "access", "published_at"}.intersection(correction["patch"]):
        raise ValueError("Correction cannot change access or publication identity")
    result["article"].update(correction["patch"])
    result["updated_at"] = now
    result.setdefault("editorial_corrections", []).append({"id": CORRECTION_ID, "corrected_at": now,
        "basis": "Matched original SEC Q1/Q2 2026 share tables; NEW HOLDINGS supplement is not a full replacement."})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-dir", default="/data/editorial-backups")
    args = parser.parse_args()
    patches = json.loads(PATCHES.read_text(encoding="utf-8"))
    now = datetime.now(timezone.utc).isoformat()
    with SessionLocal() as db:
        changes = []
        for correction in patches:
            row = db.execute(text("SELECT payload_json FROM research_brief_drafts WHERE id=:id AND slug=:slug AND status='published'"), correction).first()
            if not row:
                raise ValueError("Expected published article missing: " + correction["id"])
            original = json.loads(row[0])
            revised = corrected_payload(original, correction, now)
            if original != revised:
                changes.append((correction, row[0], revised))
        if args.apply:
            destination = Path(args.backup_dir)
            destination.mkdir(parents=True, exist_ok=True)
            for correction, old, revised in changes:
                digest = hashlib.sha256(old.encode()).hexdigest()
                backup = destination / f"{correction['id']}-{digest[:16]}.json"
                if not backup.exists():
                    with backup.open("x", encoding="utf-8") as stream:
                        stream.write(old)
                update = db.execute(text("UPDATE research_brief_drafts SET payload_json=:new, updated_at=:now WHERE id=:id AND status='published' AND payload_json=:old"),
                    {"id": correction["id"], "new": json.dumps(revised, ensure_ascii=False), "now": now, "old": old})
                if update.rowcount != 1:
                    raise ValueError("Concurrent edit; batch rolled back")
            db.commit()
        print(json.dumps({"status": "applied" if args.apply else "dry_run", "changed": len(changes), "ids": [c[0]["id"] for c in changes]}))


if __name__ == "__main__":
    main()
