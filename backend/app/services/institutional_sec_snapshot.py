"""Reconcile an exact SEC quarter through a known canonical accession.

An HR/A may add holdings rather than replace the original report. Keep the
accession and information-table URL on every row; never relabel a supplement
as a complete filing. Unknown/missing covers fail closed.
"""
from __future__ import annotations

import hashlib
import json


def snapshot_digest(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()


def merge_supplement(base, supplement):
    """Accept new securities or exact repeated rows, not ambiguous replacements."""
    from collections import defaultdict
    def groups(rows):
        result = defaultdict(list)
        for row in rows:
            result[(str(row.get("cusip") or "").strip().upper(), str(row.get("putCall") or "").upper())].append(row)
        return result
    def signatures(rows):
        return sorted(json.dumps({k: v for k, v in r.items() if k not in {
            "accessionNumber", "sourceUrl", "filing_date", "source", "symbol"}}, sort_keys=True) for r in rows)
    before, added = groups(base), groups(supplement)
    for key in before.keys() & added.keys():
        if signatures(before[key]) != signatures(added[key]):
            raise ValueError("Conflicting overlapping holdings in SEC supplement require review")
    return base + [r for key, rows in added.items() if key not in before for r in rows]


def resolve_snapshot(*, cik, year, quarter, accession, client):
    filings = sorted(client.fetch_13f_filing_metadata(cik=cik, report_year=year, report_quarter=quarter),
                     key=lambda f: (f.get("filingDate") or "", f.get("accessionNumber") or ""))
    table, sources = None, []
    found = False
    for filing in filings:
        current = filing.get("accessionNumber")
        kind = "ORIGINAL"
        if filing.get("formType") == "13F-HR/A":
            kind = client.fetch_13f_amendment_type(cik=cik, accession_number=current)
            if kind not in {"RESTATEMENT", "NEW HOLDINGS"}:
                raise ValueError("Unknown SEC amendment semantics; positions not replaced")
        rows = client.fetch_13f_information_table(cik=cik, accession_number=current)
        if not rows or any(r.get("source") != "sec_edgar" or r.get("accessionNumber") != current
                           or not str(r.get("sourceUrl", "")).startswith("https://www.sec.gov/Archives/") for r in rows):
            raise ValueError("Missing exact SEC information-table provenance")
        rows = [{**r, "filing_date": filing.get("filingDate")} for r in rows]
        if kind == "NEW HOLDINGS":
            if table is None:
                raise ValueError("SEC supplement without complete base report")
            table = merge_supplement(table, rows)
        elif kind == "RESTATEMENT" or table is None:
            table = rows
        else:
            raise ValueError("Multiple original SEC reports require review")
        sources.append({"accession": current, "kind": kind, "filing_date": filing.get("filingDate"),
                        "urls": sorted({r["sourceUrl"] for r in rows})})
        if current == accession:
            found = True
            break
    if not found or table is None:
        raise ValueError("Canonical accession unavailable in complete SEC quarter")
    return {"version": 1, "cik": cik, "year": year, "quarter": quarter, "accession": accession,
            "sources": sources, "rows": table, "sha256": snapshot_digest(table)}


def install_snapshot(db, filing, snapshot):
    """Install a verified effective quarter while retaining original source rows.

Callers own transaction, backup and dry-run. Existing CUSIP mappings are used
only when unambiguous; no issuer-name ticker guessing is permitted.
"""
    from sqlalchemy import select
    from app.models import InstitutionalPosition
    from app.services.institutional_activity import upsert_positions_for_filing
    if (snapshot["cik"], snapshot["year"], snapshot["quarter"], snapshot["accession"]) != (
            filing.cik, filing.report_year, filing.report_quarter, filing.accession_number):
        raise ValueError("SEC snapshot destination mismatch")
    if snapshot_digest(snapshot["rows"]) != snapshot["sha256"]:
        raise ValueError("SEC snapshot checksum mismatch")
    cusips = {str(r["cusip"]).strip().upper() for r in snapshot["rows"] if r.get("cusip")}
    mappings = {}
    for cusip, symbol in db.execute(select(InstitutionalPosition.cusip, InstitutionalPosition.normalized_symbol)
            .where(InstitutionalPosition.cusip.in_(cusips), InstitutionalPosition.normalized_symbol.is_not(None)).distinct()):
        mappings.setdefault(cusip, set()).add(symbol)
    rows = []
    for row in snapshot["rows"]:
        cusip = str(row.get("cusip") or "").strip().upper()
        symbols = mappings.get(cusip, set())
        rows.append({**row, "cusip": cusip, "symbol": next(iter(symbols)) if len(symbols) == 1 else None})
    metadata = json.loads(filing.raw_metadata_json or "{}")
    metadata["_walnut_position_snapshot"] = {k: v for k, v in snapshot.items() if k != "rows"}
    metadata["_walnut_position_source"] = "sec_edgar_reconciled"
    filing.raw_metadata_json = json.dumps(metadata, sort_keys=True)
    # Reuse matching position IDs; remove only rows absent from a complete table.
    existing = db.scalars(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)).all()
    result = upsert_positions_for_filing(db, filing=filing, rows=rows, reconciled_snapshot=True)
    keys = {(r.get("cusip"), (r.get("putCall") or "").upper()) for r in rows}
    for position in existing:
        if (position.cusip, (position.put_call or "").upper()) not in keys:
            db.delete(position)
    db.flush()
    return result
