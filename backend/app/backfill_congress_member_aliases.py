from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from sqlalchemy import delete

from app.db import SessionLocal
from app.models import CongressMemberAlias
from app.main import build_congress_member_alias_rows

logger = logging.getLogger(__name__)


def backfill_congress_member_aliases(*, apply: bool = False) -> dict[str, int | bool]:
    db = SessionLocal()
    try:
        rows = build_congress_member_alias_rows(db, "all")
        result = {
            "apply": apply,
            "generated_rows": len(rows),
            "inserted": 0,
            "deleted": 0,
        }
        if not apply:
            return result

        existing_count = db.query(CongressMemberAlias).count()
        db.execute(delete(CongressMemberAlias))
        now = datetime.now(timezone.utc)
        db.add_all(
            [
                CongressMemberAlias(
                    alias_member_id=str(row["alias_member_id"] or ""),
                    group_key=str(row["group_key"] or ""),
                    authoritative_member_id=str(row["authoritative_member_id"] or ""),
                    member_name=row.get("member_name"),
                    member_slug=row.get("member_slug"),
                    chamber=row.get("chamber"),
                    party=row.get("party"),
                    state=row.get("state"),
                    updated_at=now,
                )
                for row in rows
                if row.get("alias_member_id") and row.get("group_key") and row.get("authoritative_member_id")
            ]
        )
        db.commit()
        result["deleted"] = int(existing_count)
        result["inserted"] = len(rows)
        return result
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill persisted congress member alias snapshot rows.")
    parser.add_argument("--apply", action="store_true", help="Persist the generated alias rows.")
    args = parser.parse_args()
    result = backfill_congress_member_aliases(apply=args.apply)
    logger.info("Congress alias snapshot backfill completed: %s", result)
    print(result)


if __name__ == "__main__":
    main()
