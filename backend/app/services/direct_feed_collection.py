"""Bounded, resumable direct collection. All output goes to isolated staging."""
from __future__ import annotations

import json
from datetime import date, timedelta

from sqlalchemy import case, or_, select

from app.clients.direct_sources import (
    DirectSourceError, validated_ownership_xml, parse_house_index, parse_house_pdf,
    parse_issuer_material, parse_sec_index, parse_senate_html, parse_13f_submission,
)
from app.services.direct_feed_store import (
    DirectFeedDocument, DirectFeedRun, discover, dumps, record_document,
    reconcile_congress, reconcile_insider, reconcile_institutional, utcnow,
)
from app.services.official_congress import parse_house_disclosure, parse_senate_disclosure
from app.services.sec_form4 import parse_form4_xml
from app.utils.symbols import canonical_symbol


SOURCES = ("sec_directory", "sec_company", "sec_facts", "sec_form4", "sec_13f", "house_ptr", "senate_ptr", "issuer_earnings")
DIRECTORY_URL = "https://www.sec.gov/files/company_tickers.json"


def validate_directory(data):
    if not isinstance(data, dict) or not data:
        raise DirectSourceError("SEC ticker directory is empty or malformed")
    rows, symbols = [], set()
    for item in data.values():
        symbol = canonical_symbol(item.get("ticker"))
        cik = str(item.get("cik_str") or "")
        if not symbol or not cik.isdigit() or not item.get("title"):
            raise DirectSourceError("SEC directory contains an invalid company identity")
        if symbol in symbols:
            raise DirectSourceError("SEC directory has an ambiguous symbol mapping")
        symbols.add(symbol)
        rows.append({"symbol": symbol, "cik": cik.zfill(10), "name": item["title"]})
    return rows


def discover_sources(db, client, *, sources, start, end, symbols, issuer_registry, senate_reports, senate_client=None):
    counts, errors = {}, []
    directory = None
    if any(source in sources for source in ("sec_directory", "sec_company", "sec_facts")):
        try:
            raw = client.get(DIRECTORY_URL)
            directory = validate_directory(json.loads(raw))
            document = discover(db, "sec_directory", {"key": "us-listed", "url": DIRECTORY_URL})
            record_document(db, document, raw, raw.decode(), {"companies": directory})
            counts["sec_directory"] = len(directory)
            db.commit()
        except Exception as exc:
            db.rollback()
            errors.append({"feed": "sec_directory", "error": str(exc)[:500]})
    if directory is not None:
        lookup = {item["symbol"]: item for item in directory}
        for symbol in symbols:
            item = lookup.get(canonical_symbol(symbol))
            if not item:
                errors.append({"feed": "sec_company", "error": f"Symbol absent from SEC directory: {symbol}"})
                continue
            for feed, path in (("sec_company", "submissions/CIK{cik}.json"), ("sec_facts", "api/xbrl/companyfacts/CIK{cik}.json")):
                if feed in sources:
                    discover(db, feed, {**item, "key": item["cik"], "url": "https://data.sec.gov/" + path.format(**item)})
                    counts[feed] = counts.get(feed, 0) + 1
        db.commit()
    sec_forms = {}
    if "sec_form4" in sources:
        sec_forms.update({"4": "sec_form4", "4/A": "sec_form4"})
    if "sec_13f" in sources:
        sec_forms.update({"13F-HR": "sec_13f", "13F-HR/A": "sec_13f"})
    if sec_forms:
        current = start
        while current <= end:
            if current.weekday() < 5:
                quarter = (current.month - 1) // 3 + 1
                url = f"https://www.sec.gov/Archives/edgar/daily-index/{current.year}/QTR{quarter}/master.{current:%Y%m%d}.idx"
                try:
                    rows = parse_sec_index(client.get(url), forms=tuple(sec_forms))
                    day_counts = {}
                    for row in rows:
                        if date.fromisoformat(row["filing_date"]) != current:
                            raise DirectSourceError("SEC daily index contains an unexpected filing date")
                        feed = sec_forms[row["form"]]
                        discover(db, feed, row)
                        day_counts[feed] = day_counts.get(feed, 0) + 1
                    db.commit()
                    for feed, count in day_counts.items():
                        counts[feed] = counts.get(feed, 0) + count
                except Exception as exc:
                    db.rollback()
                    for feed in sorted(set(sec_forms.values())):
                        errors.append({"feed": feed, "date": str(current), "error": str(exc)[:500]})
            current += timedelta(days=1)
    if "house_ptr" in sources:
        for year in range(start.year, end.year + 1):
            url = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
            try:
                rows = parse_house_index(client.get(url), year)
                for row in rows:
                    if start <= date.fromisoformat(row["filing_date"]) <= end:
                        discover(db, "house_ptr", row)
                        counts["house_ptr"] = counts.get("house_ptr", 0) + 1
                db.commit()
            except Exception as exc:
                db.rollback()
                errors.append({"feed": "house_ptr", "error": str(exc)[:500]})
    if "senate_ptr" in sources:
        # Reviewed URLs remain retryable, but are not discovery completeness.
        for item in senate_reports:
            discover(db, "senate_ptr", item)
            counts["senate_ptr"] = counts.get("senate_ptr", 0) + 1
        db.commit()
        try:
            if senate_client is None:
                raise DirectSourceError('Senate session collector is not configured; reviewed URLs are not coverage')
            rows = senate_client.discover(start=start, end=end)
            for row in rows:
                discover(db, 'senate_ptr', row)
            db.commit()
            counts['senate_ptr'] = len(rows)
        except Exception as exc:
            db.rollback()
            errors.append({'feed': 'senate_ptr', 'error': str(exc)[:500]})
    if "issuer_earnings" in sources:
        for company in issuer_registry:
            for item in company.get("documents", []):
                metadata = {**item, "symbol": company["symbol"], "company_website": company["company_website"], "investor_website": company["investor_website"]}
                # Identity is company + type + fiscal period, not provider/URL.
                metadata["key"] = f"{company['symbol']}:{item['document_type']}:{item['fiscal_year']}:Q{item['fiscal_quarter']}"
                discover(db, "issuer_earnings", metadata)
                counts["issuer_earnings"] = counts.get("issuer_earnings", 0) + 1
        db.commit()
    return counts, errors


def parse_document(feed, raw, metadata):
    reasons = []
    if feed == "sec_13f":
        source_text, parsed = parse_13f_submission(raw, metadata)
        if parsed["metadata"]["is_amendment"]:
            reasons.append("13F amendment requires the existing original/supplement reconciliation")
        if parsed["metadata"]["confidential_omitted"]:
            reasons.append("13F confidential holdings omitted; absence cannot establish an exit")
        return source_text, parsed, reasons
    if feed == "sec_form4":
        source_text = validated_ownership_xml(raw, metadata)
        parsed = parse_form4_xml(source_text, accession_number=metadata["key"], source_url=metadata["url"], filing_date=date.fromisoformat(metadata["filing_date"]))
        filing = parsed["filing"]
        if metadata["form"] == "4/A" or filing.get("document_type") == "4/A":
            reasons.append("Amendment requires reconciliation with original filing")
        if filing["reporting_owner_count"] != 1:
            reasons.append("Joint ownership attribution requires review")
        if not parsed["transactions"]:
            reasons.append("No transaction rows; holding-only or unsupported filing")
        for item in parsed["transactions"]:
            if not item["transaction_date"] or not item["ticker_normalized"] or item["shares"] is None:
                reasons.append("Transaction identity incomplete")
                break
            if item["transaction_date"] > item["filing_date"]:
                reasons.append("Transaction date is after disclosure date")
                break
        return source_text, parsed, reasons
    if feed in {"house_ptr", "senate_ptr"}:
        source_text, report = (parse_house_pdf if feed == "house_ptr" else parse_senate_html)(raw, metadata)
        transactions = (parse_house_disclosure if feed == "house_ptr" else parse_senate_disclosure)(report)
        if report.get("amendment_flag"):
            reasons.append("Congress amendment requires original filing reconciliation")
        for item in transactions:
            if not item["disclosure_date"] or not item["transaction_date"] or item["amount_low"] is None or not item["transaction_type_normalized"]:
                reasons.append("Congress transaction fields incomplete")
                break
            if item["transaction_date"] > item["disclosure_date"]:
                reasons.append("Transaction date is after disclosure date")
                break
            if item["asset_type_normalized"] == "stock" and not item["ticker_normalized"]:
                reasons.append("Unresolved stock symbol")
            if item["asset_type_normalized"] == "unresolved":
                reasons.append("Unresolved asset classification")
        hashes = [item['normalized_hash'] for item in transactions]
        if len(set(hashes)) != len(hashes):
            reasons.append('Congress source row identity is repeated')
        return source_text, {"metadata": metadata, "transactions": transactions, "source_report": report}, reasons
    if feed in {"sec_company", "sec_facts"}:
        parsed = json.loads(raw)
        if str(parsed.get("cik", "")).zfill(10) != metadata["cik"]:
            raise DirectSourceError("SEC response CIK does not match request")
        if feed == "sec_company":
            parsed = {key: parsed.get(key) for key in ("cik", "name", "tickers", "exchanges", "sic", "sicDescription", "website", "investorWebsite", "fiscalYearEnd")}
            # SIC remains SIC; it is not represented as a vendor's sector taxonomy.
            if not parsed.get("name") or not parsed.get("tickers"):
                reasons.append("SEC company identity incomplete")
        elif not parsed.get("facts", {}).get("us-gaap"):
            reasons.append("No US GAAP company facts; taxonomy coverage requires review")
        return raw.decode(), parsed, reasons
    if feed == "issuer_earnings":
        source_text, parsed = parse_issuer_material(raw, metadata)
        return source_text, parsed, reasons
    raise DirectSourceError(f"Unsupported direct source: {feed}")


def collect_direct_feeds(db, client, *, sources=SOURCES, start, end, symbols=(), issuer_registry=(), senate_reports=(), senate_client=None, limit=25, recheck_hours=24, retry_failed=False):
    if not 1 <= limit <= 200 or not 1 <= recheck_hours <= 720 or end < start or (end - start).days > 93:
        raise ValueError("Invalid direct feed collection bounds")
    if not set(sources).issubset(SOURCES):
        raise ValueError("Unknown direct source")
    run = DirectFeedRun(report_json=dumps({"sources": sources, "start": start, "end": end, "symbols": symbols}))
    db.add(run)
    db.commit()
    run_id = run.id
    discovered, errors = discover_sources(db, client, sources=sources, start=start, end=end, symbols=symbols,
                                          issuer_registry=issuer_registry, senate_reports=senate_reports, senate_client=senate_client)
    processed, results = 0, {}
    cutoff = utcnow() - timedelta(hours=recheck_hours)
    failed_cutoff = utcnow() - timedelta(hours=1)
    # A per-feed budget prevents one large SEC day starving Congress or earnings.
    for feed in sources:
        if feed == "sec_directory":
            continue
        ids = list(db.scalars(select(DirectFeedDocument.id).where(
            DirectFeedDocument.feed == feed,
            or_(DirectFeedDocument.status == "pending", DirectFeedDocument.checked_at.is_(None), DirectFeedDocument.checked_at < cutoff,
                (DirectFeedDocument.status == 'failed') & (DirectFeedDocument.checked_at <= failed_cutoff),
                DirectFeedDocument.status == "failed" if retry_failed else False),
        ).order_by(case((DirectFeedDocument.status == 'pending', 0), (DirectFeedDocument.status == 'failed', 1), else_=2),
                   DirectFeedDocument.checked_at.asc().nullsfirst(), DirectFeedDocument.id.asc()).limit(limit)))
        for document_id in ids:
            document = db.get(DirectFeedDocument, document_id)
            document.attempts += 1
            metadata = json.loads(document.metadata_json)
            try:
                raw = (senate_client if feed == 'senate_ptr' and senate_client else client).get(document.source_url)
                text, parsed, reasons = parse_document(feed, raw, metadata)
                record_document(db, document, raw, text, parsed, reasons=reasons)
                if feed == "sec_form4":
                    document.reconciliation_json = dumps(reconcile_insider(db, parsed))
                elif feed == "sec_13f":
                    document.reconciliation_json = dumps(reconcile_institutional(db, parsed))
                elif feed in {"house_ptr", "senate_ptr"}:
                    document.reconciliation_json = dumps(reconcile_congress(db, parsed))
                reconciliation = json.loads(document.reconciliation_json or "{}")
                if reconciliation.get("ambiguous") or reconciliation.get("existing_only_ids"):
                    document.status = "quarantined"
                    document.error = "; ".join(filter(None, [document.error,
                        "Existing records differ from source; canonical reconciliation required"]))
                processed += 1
                results[document.status] = results.get(document.status, 0) + 1
                db.commit()
            except Exception as exc:
                db.rollback()
                document = db.get(DirectFeedDocument, document_id)
                document.attempts += 1
                document.status = "failed"
                document.checked_at = utcnow()
                document.error = f"{type(exc).__name__}: {exc}"[:1000]
                # Retain a prior successful revision for diagnosis, but never
                # report failed refreshes as fresh or erase missing coverage.
                errors.append({"feed": feed, "key": document.source_key, "error": document.error})
                db.commit()
                # A denied/cooling/unreachable source must not trigger hundreds
                # of identical failing requests in one scheduled run.
                if isinstance(exc, DirectSourceError) and str(exc).startswith((
                        'Source transport failed', 'Source cooldown', 'Source HTTP 403:', 'Source HTTP 429:')):
                    break
    unresolved = list(db.scalars(select(DirectFeedDocument).where(
        DirectFeedDocument.feed.in_(sources),
        DirectFeedDocument.status.in_(("pending", "failed", "quarantined")),
    )))
    report = {"sources": sources, "window": {"start": start, "end": end}, "symbols": symbols,
              "discovered": discovered, "processed": processed, "results": results,
              "pending_documents": sum(row.status == "pending" for row in unresolved),
              "unresolved_documents": len(unresolved), "errors": errors, "public_writes": 0,
              "cutover_ready": False, "fmp_disabled": False}
    if senate_client is not None:
        report['senate_discovery'] = senate_client.search_receipt
    run = db.get(DirectFeedRun, run_id)
    run.status = "partial" if errors or unresolved else "collected"
    run.finished_at = utcnow()
    run.report_json = dumps(report)
    db.commit()
    return {"run_id": run_id, "status": run.status, **report}
