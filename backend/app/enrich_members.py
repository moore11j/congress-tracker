# backend/app/enrich_members.py
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple, List

import requests
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Member

LEGISLATORS_CURRENT_JSON = (
    "https://unitedstates.github.io/congress-legislators/legislators-current.json"
)

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[^a-z\s\-']", "", s)  # keep letters, spaces, -, '
    s = re.sub(r"\s+", " ", s)
    return s

def _last_variants(last: Optional[str]) -> List[str]:
    """
    Return plausible normalized last-name variants to handle cases like:
      'W. Hickenlooper' (stored as last_name) vs 'Hickenlooper' (dataset last)
    Without breaking common multi-word last names too badly.
    """
    n = _norm(last)
    out = []
    if n:
        out.append(n)
        parts = n.split()
        if len(parts) >= 2:
            # last token fallback (e.g., "w hickenlooper" -> "hickenlooper")
            out.append(parts[-1])
    # de-dupe while preserving order
    seen = set()
    res = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            res.append(x)
    return res

def _pick_current_term(terms: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not terms:
        return None
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

def build_indexes() -> tuple[
    Dict[str, dict[str, Any]],
    Dict[Tuple[str, str, str, str], dict[str, Any]],
    Dict[Tuple[str, str, str], Optional[dict[str, Any]]],
]:
    """
    Returns:
      by_bioguide: bioguide_id -> rec
      by_name_state_chamber: (norm_first, norm_last, state_upper, chamber) -> rec
      by_name_chamber_unique: (norm_first, norm_last, chamber) -> rec OR None if ambiguous
    """
    r = requests.get(LEGISLATORS_CURRENT_JSON, timeout=30)
    r.raise_for_status()
    data = r.json()

    by_bioguide: Dict[str, dict[str, Any]] = {}
    by_name_state_chamber: Dict[Tuple[str, str, str, str], dict[str, Any]] = {}

    # temp collector to detect ambiguity
    name_chamber_bucket: Dict[Tuple[str, str, str], List[dict[str, Any]]] = {}

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

        rec = {
            "bioguide": bioguide,
            "first": first,
            "last": last,
            "party": party,
            "chamber": chamber,
            "state": state,
        }

        if bioguide:
            by_bioguide[bioguide] = rec

        nf, nl = _norm(first), _norm(last)
        if nf and nl and state and chamber:
            by_name_state_chamber[(nf, nl, state, chamber)] = rec

        if nf and nl and chamber:
            key = (nf, nl, chamber)
            name_chamber_bucket.setdefault(key, []).append(rec)

    # collapse to unique-only map
    by_name_chamber_unique: Dict[Tuple[str, str, str], Optional[dict[str, Any]]] = {}
    for k, recs in name_chamber_bucket.items():
        # unique if exactly one record
        by_name_chamber_unique[k] = recs[0] if len(recs) == 1 else None

    return by_bioguide, by_name_state_chamber, by_name_chamber_unique

def enrich_members() -> dict[str, Any]:
    by_bioguide, by_name_state_chamber, by_name_chamber_unique = build_indexes()

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

            rec = None

            # Try 1: real bioguide id stored in Member.bioguide_id
            rec = by_bioguide.get((m.bioguide_id or "").strip())

            # Name normalization
            nf = _norm(m.first_name)
            last_variants = _last_variants(m.last_name)

            # Try 2: match by (first,last,state,chamber) if state exists
            if not rec:
                state = (m.state or "").strip().upper() or None
                if nf and last_variants and state:
                    for lv in last_variants:
                        rec = by_name_state_chamber.get((nf, lv, state, chamber))
                        if rec:
                            break

            # Try 3: match by (first,last,chamber) if UNIQUE in dataset (works when state is missing)
            if not rec:
                if nf and last_variants:
                    for lv in last_variants:
                        candidate = by_name_chamber_unique.get((nf, lv, chamber))
                        if candidate:
                            rec = candidate
                            break

            if rec:
                matched += 1

                if not m.party and rec.get("party"):
                    m.party = rec["party"]
                    updated += 1

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
