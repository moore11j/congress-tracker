# backend/app/enrich_members.py
from __future__ import annotations

from typing import Any

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Event, Member
from app.services.congress_metadata import get_congress_metadata_resolver


def enrich_members() -> dict[str, Any]:
    metadata = get_congress_metadata_resolver()

    updated_member_fields = 0
    matched = 0
    unmatched = 0
    repaired_events = 0

    db = SessionLocal()
    try:
        members = db.execute(select(Member)).scalars().all()

        for member in members:
            resolved = metadata.resolve(
                bioguide_id=(member.bioguide_id or "").strip() or None,
                first_name=member.first_name,
                last_name=member.last_name,
                full_name=f"{member.first_name or ''} {member.last_name or ''}".strip() or None,
                chamber=member.chamber,
                state=member.state,
                house_district=member.bioguide_id.replace("FMP_HOUSE_", "") if (member.bioguide_id or "").startswith("FMP_HOUSE_") else None,
            )
            if not resolved:
                unmatched += 1
                continue

            matched += 1

            if not member.party and resolved.party:
                member.party = resolved.party
                updated_member_fields += 1
            if not member.state and resolved.state:
                member.state = resolved.state
                updated_member_fields += 1
            if not member.chamber and resolved.chamber:
                member.chamber = resolved.chamber
                updated_member_fields += 1

            if member.bioguide_id:
                events = db.execute(
                    select(Event).where(
                        Event.member_bioguide_id == member.bioguide_id,
                        Event.event_type == "congress_trade",
                    )
                ).scalars().all()
                for event in events:
                    touched = False
                    if not event.party and member.party:
                        event.party = member.party
                        touched = True
                    if not event.chamber and member.chamber:
                        event.chamber = member.chamber
                        touched = True
                    if touched:
                        repaired_events += 1

        db.commit()
        return {
            "status": "ok",
            "matched": matched,
            "updated_member_fields": updated_member_fields,
            "repaired_events": repaired_events,
            "unmatched": unmatched,
        }
    finally:
        db.close()


if __name__ == "__main__":
    print(enrich_members())
