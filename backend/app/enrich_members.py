# backend/app/enrich_members.py
from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Event, Filing, Member, TradeOutcome, Transaction
from app.services.congress_metadata import get_congress_metadata_resolver


logger = logging.getLogger(__name__)

_CANONICAL_BIOGUIDE_RE = re.compile(r"^[A-Z]\d{6}$")


def _needs_canonical_remap(current_bioguide_id: str | None, resolved_bioguide_id: str | None) -> bool:
    if not current_bioguide_id or not resolved_bioguide_id:
        return False
    if current_bioguide_id == resolved_bioguide_id:
        return False
    if current_bioguide_id.startswith("FMP_"):
        return True
    return _CANONICAL_BIOGUIDE_RE.fullmatch(current_bioguide_id) is None


def _repoint_member_identity(
    db,
    *,
    from_member: Member,
    to_member: Member,
    delete_source_member: bool = False,
) -> dict[str, int]:
    filing_updates = db.query(Filing).filter(Filing.member_id == from_member.id).update(
        {Filing.member_id: to_member.id}
    )
    transaction_updates = db.query(Transaction).filter(Transaction.member_id == from_member.id).update(
        {Transaction.member_id: to_member.id}
    )
    event_updates = db.query(Event).filter(Event.member_bioguide_id == from_member.bioguide_id).update(
        {Event.member_bioguide_id: to_member.bioguide_id}
    )
    outcome_updates = db.query(TradeOutcome).filter(TradeOutcome.member_id == from_member.bioguide_id).update(
        {TradeOutcome.member_id: to_member.bioguide_id}
    )
    if delete_source_member:
        db.delete(from_member)
    return {
        "filings": filing_updates,
        "transactions": transaction_updates,
        "events": event_updates,
        "trade_outcomes": outcome_updates,
    }


def enrich_members() -> dict[str, Any]:
    metadata = get_congress_metadata_resolver()

    updated_member_fields = 0
    matched = 0
    unmatched = 0
    repaired_events = 0
    remapped_members = 0
    remapped_links = 0
    remap_collisions = 0

    db = SessionLocal()
    try:
        members = db.execute(select(Member)).scalars().all()
        members_by_bioguide_id = {member.bioguide_id: member for member in members if member.bioguide_id}

        for member in members:
            original_bioguide_id = member.bioguide_id
            resolved = metadata.resolve(
                bioguide_id=(member.bioguide_id or "").strip() or None,
                first_name=member.first_name,
                last_name=member.last_name,
                full_name=f"{member.first_name or ''} {member.last_name or ''}".strip() or None,
                chamber=member.chamber,
                state=member.state,
                house_district=(
                    member.bioguide_id.replace("FMP_HOUSE_", "")
                    if (member.bioguide_id or "").startswith("FMP_HOUSE_")
                    else None
                ),
            )
            if not resolved:
                unmatched += 1
                if (member.bioguide_id or "").startswith("FMP_SENATE_XX_"):
                    logger.info(
                        "Synthetic senate member unresolved: bioguide_id=%s name=%s %s chamber=%s state=%s",
                        member.bioguide_id,
                        member.first_name,
                        member.last_name,
                        member.chamber,
                        member.state,
                    )
                continue

            matched += 1

            should_repoint = _needs_canonical_remap(member.bioguide_id, resolved.bioguide_id)
            if should_repoint:
                logger.info(
                    "Attempting synthetic-to-canonical remap: from=%s to=%s name=%s %s",
                    member.bioguide_id,
                    resolved.bioguide_id,
                    member.first_name,
                    member.last_name,
                )
                canonical = members_by_bioguide_id.get(resolved.bioguide_id)
                if canonical is None:
                    canonical = db.execute(
                        select(Member).where(Member.bioguide_id == resolved.bioguide_id)
                    ).scalar_one_or_none()
                if canonical and canonical.id != member.id:
                    rewired = _repoint_member_identity(
                        db,
                        from_member=member,
                        to_member=canonical,
                    )
                    remapped_members += 1
                    remap_collisions += 1
                    remapped_links += sum(rewired.values())
                    logger.info(
                        "Synthetic-to-canonical remap succeeded via identity repoint: from=%s to=%s rewired=%s",
                        original_bioguide_id,
                        canonical.bioguide_id,
                        rewired,
                    )
                    member = canonical
                elif not canonical:
                    member.bioguide_id = resolved.bioguide_id
                    remapped_members += 1
                    if original_bioguide_id and members_by_bioguide_id.get(original_bioguide_id) is member:
                        members_by_bioguide_id.pop(original_bioguide_id, None)
                    if member.bioguide_id:
                        members_by_bioguide_id[member.bioguide_id] = member
                    logger.info(
                        "Synthetic-to-canonical remap succeeded via in-place bioguide update: from=%s to=%s",
                        original_bioguide_id,
                        member.bioguide_id,
                    )
                else:
                    logger.info(
                        "Synthetic-to-canonical remap skipped: from=%s to=%s reason=already_canonical",
                        member.bioguide_id,
                        resolved.bioguide_id,
                    )

            if not member.party and resolved.party:
                member.party = resolved.party
                updated_member_fields += 1
            if not member.state and resolved.state:
                member.state = resolved.state
                updated_member_fields += 1
            if member.chamber != resolved.chamber and resolved.chamber:
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
        logger.info(
            "Member enrichment repair summary: matched=%d unmatched=%d remapped_members=%d remap_collisions=%d remapped_links=%d",
            matched,
            unmatched,
            remapped_members,
            remap_collisions,
            remapped_links,
        )
        return {
            "status": "ok",
            "matched": matched,
            "updated_member_fields": updated_member_fields,
            "repaired_events": repaired_events,
            "remapped_members": remapped_members,
            "remap_collisions": remap_collisions,
            "remapped_links": remapped_links,
            "unmatched": unmatched,
        }
    finally:
        db.close()


if __name__ == "__main__":
    print(enrich_members())
