"""Bounded primary-source comparisons for editorial use, not a holdings ingest.

Provider change rows select candidates only. No absence-to-zero inference, no
market-wide totals, and no modification of canonical events or scoring history.
"""
from __future__ import annotations

import math
import time
from collections import defaultdict
from typing import Any

from sqlalchemy import desc, select
from app.models import InstitutionalHolder, InstitutionalPosition, InstitutionalSymbolSummary
from app.utils.institution_names import institution_display_name


def period_table(cik: str, year: int, quarter: int, client) -> tuple[list[dict], list[str]]:
    filings = client.fetch_13f_filing_metadata(cik=cik, report_year=year, report_quarter=quarter)
    filings = sorted(filings, key=lambda f: (f.get("filingDate") or "", f.get("accessionNumber") or ""))
    table, dates = None, []
    for filing in filings:
        accession = filing.get("accessionNumber")
        kind = "ORIGINAL"
        if filing.get("formType") == "13F-HR/A":
            kind = client.fetch_13f_amendment_type(cik=cik, accession_number=accession)
            if kind not in {"RESTATEMENT", "NEW HOLDINGS"}:
                raise ValueError("Unresolved amendment type")
        rows = client.fetch_13f_information_table(cik=cik, accession_number=accession)
        if not rows:
            raise ValueError("Unavailable information table")
        rows = [{**row, "filing_date": filing.get("filingDate")} for row in rows]
        if kind == "NEW HOLDINGS":
            if table is None:
                raise ValueError("Supplement without a base report")
            table.extend(rows)
        elif kind == "RESTATEMENT" or table is None:
            table = list(rows)
        else:
            raise ValueError("Multiple original reports require review")
        dates.append(str(filing.get("filingDate") or ""))
    if table is None:
        raise ValueError("No complete period available in SEC recent submissions")
    return table, dates


def matched_shares(rows: list[dict], cusip: str) -> tuple[float, list[str]]:
    matched = [r for r in rows if r.get("cusip") == cusip and not r.get("putCall")]
    if not matched:
        # Missing may reflect omission, confidentiality or incomplete coverage.
        raise ValueError("No comparable positive holding; absence is not an exit")
    if any(str(r.get("shareType") or "").upper() != "SH" for r in matched):
        raise ValueError("Security is not explicitly share-denominated")
    shares = [float(r["shares"]) for r in matched]
    if any(not math.isfinite(n) or n <= 0 for n in shares):
        raise ValueError("Invalid share count")
    sources = sorted({str(r.get("sourceUrl") or "") for r in matched})
    if not sources or any(not url.startswith("https://www.sec.gov/Archives/") for url in sources):
        raise ValueError("Missing primary-source provenance")
    return sum(shares), sources


def compare_holder(cik: str, name: str, cusip: str, year: int, quarter: int, client) -> dict:
    previous = (year - 1, 4) if quarter == 1 else (year, quarter - 1)
    prior_table, prior_dates = period_table(cik, *previous, client)
    current_table, current_dates = period_table(cik, year, quarter, client)
    before, prior_sources = matched_shares(prior_table, cusip)
    after, current_sources = matched_shares(current_table, cusip)
    # Very large changes need a separate split/reorganization check.
    if max(after / before, before / after) >= 4:
        raise ValueError("Corporate-action or unusually large change needs review")
    delta = after - before
    return {
        "holder_name": institution_display_name(name), "cik": cik, "cusip": cusip,
        "change_type": "increase" if delta > 0 else "decrease" if delta < 0 else "unchanged",
        "prev_shares": before, "curr_shares": after, "shares_delta": delta,
        "shares_delta_pct": delta / before * 100,
        "previous_reporting_period": f"Q{previous[1]} {previous[0]}",
        "reporting_period": f"Q{quarter} {year}",
        "filing_date": max(r["filing_date"] for r in current_table if r.get("cusip") == cusip and not r.get("putCall")),
        "previous_filing_date": max(r["filing_date"] for r in prior_table if r.get("cusip") == cusip and not r.get("putCall")),
        "amendments_reviewed_through": max(current_dates),
        "prior_source_urls": prior_sources, "source_urls": current_sources,
    }


def verified_ownership_detail(db, symbol: str, *, client=None, max_holders: int = 6) -> dict[str, Any]:
    if client is None:
        from app.clients import sec_edgar as client
    summary = db.execute(select(InstitutionalSymbolSummary).where(
        InstitutionalSymbolSummary.normalized_symbol == symbol
    ).order_by(desc(InstitutionalSymbolSummary.report_year), desc(InstitutionalSymbolSummary.report_quarter)).limit(1)).scalars().first()
    result: dict[str, Any] = {
        "verification": "sec_matched_share_pairs_v1", "comparisons": [], "top_accumulators": [],
        "top_reducers": [], "top_holders_in_walnut_set": [], "excluded_candidates": [],
        "coverage": "Bounded sample of matched positive holdings, not market-wide buyers, net flows or proof of trades. Missing holdings are not zero. Related filers may overlap.",
        "accumulator_ranking_basis": "shares added within the verified sample only",
    }
    if summary is None:
        result["coverage"] += " No reporting period is available."
        return result
    year, quarter = int(summary.report_year), int(summary.report_quarter)
    result["reporting_period"] = f"Q{quarter} {year}"
    candidates = db.execute(select(InstitutionalPosition, InstitutionalHolder.holder_name).join(
        InstitutionalHolder, InstitutionalHolder.cik == InstitutionalPosition.cik
    ).where(InstitutionalPosition.normalized_symbol == symbol,
            InstitutionalPosition.report_year == year, InstitutionalPosition.report_quarter == quarter,
            InstitutionalPosition.put_call.is_(None), InstitutionalPosition.cusip.is_not(None))
        .order_by(desc(InstitutionalPosition.value_usd), InstitutionalPosition.cik).limit(100)).all()
    identities = defaultdict(set)
    for position, _ in candidates:
        identities[position.cik].add(position.cusip)
    seen, started = set(), time.monotonic()
    for position, name in candidates:
        if position.cik in seen:
            continue
        if len(seen) >= max_holders or time.monotonic() - started > 90:
            break
        seen.add(position.cik)
        try:
            if len(identities[position.cik]) != 1:
                raise ValueError("Multiple security identities require review")
            result["comparisons"].append(compare_holder(position.cik, name, position.cusip, year, quarter, client))
        except Exception as exc:
            # Operational diagnostics, not invented zero holdings or generated prose.
            result["excluded_candidates"].append({"cik": position.cik, "reason": type(exc).__name__})
    result["top_accumulators"] = sorted([r for r in result["comparisons"] if r["shares_delta"] > 0], key=lambda r: (-r["shares_delta"], r["cik"]))[:3]
    result["top_reducers"] = sorted([r for r in result["comparisons"] if r["shares_delta"] < 0], key=lambda r: (r["shares_delta"], r["cik"]))[:3]
    result["verified_holder_count"] = len(result["comparisons"])
    return result
