# backend/app/enrich_members.py
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

import requests
from sqlalchemy import select

from app.main import SessionLocal, Member

LEGISLATORS_CURRENT_JSON = (
    "https://unitedstates.github.io/congress-legislators/legislators-current.json"
)

# Normalize for fuzzy-ish matching
def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[^a-z\s\-']", "", s)  # keep letters, spaces, -, '
    s = re.sub(r"\s+", " ", s)
    return s

def _pick_current_term(terms: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    # In legislators-current.json, entries are currently-serving; last term is typically current.
    if not terms:
        return None
    # safest: pick the last term record
    return terms[-1]

def _term_chamber(term_type: Optional[str]) -> Optional[str]:
    if not term_type:
        return None
    t = term_type.strip().lower()
    if t == "rep":
        return "house"
    if t == "sen":
        return "senate"
    return None

def _derive_state_from_district_field(district: Optional[str]) -> Optional[str]:
    # e.g. "IL01" -> "IL"
    if not district:
        return None
    d = district.strip().upper()
    if len(d) >= 2 and d[:2].isalpha():
        return d[:2]
    return None

def build_indexes() -> tuple[
    Dict[str, dict[str, Any]],
    Dict[Tuple[str, str, str], dict[str, Any]],
]:
    """
    Returns:
      by_bioguide: bioguide_id -> {party, chamber, state, district, first, last}
      by_name_state_chamber: (norm_first, norm_last, state_upper, chamber) -> same dict
    """
    r = requests.get(LEGISLATORS_CURRENT_JSON, timeout=30)
    r.raise_for_status()
    data = r.json()

    by_bioguide: Dict[str, dict[str, Any]] = {}
    by_name_state_chamber: Dict[Tuple[str, str, str], dict[str, Any]] = {}

    for p in data:
        ids = p.get("id", {}) or {}
        bioguide = (ids.get("bioguide") or "").strip()
        name = p.get("name", {}) or {}
        first = (name.get("first") or "").strip()
        last = (name.get("last") or "").strip()

        term = _pick_current_term(p.get("terms", []) or [])
        if not term:
            continue

        chamber = _term_chamber(term.get("type"))
        if chamber not in ("house", "senate"):
            continue

        party = (term.get("party") or "").strip() or None
        state = (term.get("state") or "").strip().upper() or None
        district = None
        if chamber == "house":
            # district in this dataset is usually an int or str
            dist_val = term.get("district")
            if dist_val is not None:
                district = str(dist_val).strip()
        rec = {
            "bioguide": bioguide,
            "first": first,
            "last": last,
            "party": party,
            "chamber": chamber,
            "state": state,
            "district": district,
        }

        if bioguide:
            by_bioguide[bioguide] = rec

        if first and last and state and chamber:
            key = (_norm(first), _norm(last), state, chamber)
            by_name_state_chamber[key] = rec

    return by_bioguide, by_name_state_chamber

def enrich_members() -> dict[str, Any]:
    by_bioguide, by_name_state_chamber = build_indexes()

    updated = 0
    matched = 0
    unmatched = 0

    db = SessionLocal()
    try:
        members = db.execute(select(Member)).scalars().all()

        for m in members:
            # Skip if already has party
            if m.party and m.party.strip():
                continue

            chamber = (m.chamber or "").strip().lower()
            if chamber not in ("house", "senate"):
                chamber = "house"

            # Try 1: If your Member.bioguide_id is actually a real bioguide id
            # (in future when you add a true directory ingest)
            rec = by_bioguide.get((m.bioguide_id or "").strip())

            # Try 2: Match by (first,last,state,chamber) — works great for your current FMP rows
            if not rec:
                state = (m.state or "").strip().upper() or None
                if m.first_name and m.last_name and state:
                    key = (_norm(m.first_name), _norm(m.last_name), state, chamber)
                    rec = by_name_state_chamber.get(key)

            if rec:
                matched += 1
                # Fill what we can
                if not m.party and rec.get("party"):
                    m.party = rec["party"]
                    updated += 1
                # Also backfill state/chamber if missing
                if (not m.state) and rec.get("state"):
                    m.state = rec["state"]
                    updated += 1
                if (not m.chamber) and rec.get("chamber"):
                    m.chamber = rec["chamber"]
                    updated += 1
            else:
                unmatched += 1

        db.commit()
        return {
            "status": "ok",
            "matched": matched,
            "updated_fields": updated,
            "unmatched": unmatched,
        }
    finally:
        db.close()

if __name__ == "__main__":
    print(enrich_members())
