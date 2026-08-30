from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from app.clients.fmp import (
    FMPClientError,
    fetch_holder_industry_breakdown,
    fetch_holder_performance_summary,
    fetch_industry_summary,
    fetch_institutional_filing_dates,
    fetch_institutional_filing_extract,
    fetch_latest_institutional_filings,
)
from app.db import SessionLocal, engine, ensure_institutional_activity_schema
from app.models import InstitutionalFiling, InstitutionalPosition
from app.services.institutional_activity import (
    CANONICAL_INSTITUTIONAL_HOLDER_UNIVERSE,
    parse_latest_filing,
    process_filing_changes_and_events,
    process_filing_changes_and_events_symbol_batch,
    cleanup_overbroad_institutional_feed_events,
    get_canonical_filing_for_holder_period,
    normalize_cik,
    seed_canonical_institutional_holders,
    upsert_holder_industry_breakdown_rows,
    upsert_holder_performance_rows,
    upsert_industry_summary_rows,
    upsert_institutional_filing,
    upsert_institutional_holder,
    upsert_positions_for_filing,
)

logger = logging.getLogger(__name__)


def _count_filing_positions(db, filing: InstitutionalFiling) -> int:
    if filing.id is None:
        return 0
    return int(db.query(InstitutionalPosition).filter(InstitutionalPosition.filing_id == filing.id).count())


def _normalized_form_type(filing: InstitutionalFiling) -> str:
    return (filing.form_type or "").strip().upper()


def _is_no_holdings_notice_form(filing: InstitutionalFiling) -> bool:
    return _normalized_form_type(filing).startswith("13F-NT")


def _is_zero_position_retryable_form(filing: InstitutionalFiling) -> bool:
    return not _is_no_holdings_notice_form(filing)


def _should_retry_processed_zero_position_filing(db, filing: InstitutionalFiling) -> bool:
    return filing.processed_at is not None and _is_zero_position_retryable_form(filing) and _count_filing_positions(db, filing) == 0


def _mark_empty_extract_outcome(
    db,
    filing: InstitutionalFiling,
    *,
    raw_extract_rows: int,
    skipped_positions: int = 0,
) -> str:
    if _is_no_holdings_notice_form(filing):
        filing.processed_at = datetime.now(timezone.utc)
        logger.info(
            "institutional_empty_extract_processed_no_holdings cik=%s year=%s quarter=%s form_type=%s raw_extract_rows=%s skipped_positions=%s",
            filing.cik,
            filing.report_year,
            filing.report_quarter,
            filing.form_type,
            raw_extract_rows,
            skipped_positions,
        )
        return "empty_extract_processed_no_holdings"

    filing.processed_at = None
    logger.warning(
        "institutional_empty_extract_retryable cik=%s year=%s quarter=%s form_type=%s raw_extract_rows=%s skipped_positions=%s",
        filing.cik,
        filing.report_year,
        filing.report_quarter,
        filing.form_type,
        raw_extract_rows,
        skipped_positions,
    )
    return "empty_extract_retryable"


def _empty_extract_result(metric: str) -> dict[str, int | str]:
    return {
        "status": "ok",
        "processed_filings": 1 if metric == "empty_extract_processed_no_holdings" else 0,
        "empty_extract_retryable": 1 if metric == "empty_extract_retryable" else 0,
        "empty_extract_processed_no_holdings": 1 if metric == "empty_extract_processed_no_holdings" else 0,
        "position_rows": 0,
        "position_changes": 0,
        "summaries": 0,
        "activity_events": 0,
        "feed_events": 0,
    }


def _candidate_canonical_sort_key(candidate) -> tuple[int, object, str]:
    return (
        1 if bool(candidate.is_amendment) else 0,
        candidate.filing_date,
        candidate.accession_number or "",
    )


def ingest_latest_institutional_filings(
    *,
    start_page: int = 0,
    pages: int = 1,
    limit: int = 100,
    force: bool = False,
    max_filings: int | None = 25,
) -> dict[str, int | str | None]:
    ensure_institutional_activity_schema(engine)
    normalized_start_page = max(0, int(start_page or 0))
    page_count = max(1, int(pages or 1))
    counts: dict[str, int | str] = {
        "status": "ok",
        "start_page": normalized_start_page,
        "pages": page_count,
        "pages_scanned": 0,
        "first_empty_page_seen": None,
        "max_filings_reached": 0,
        "scanned": 0,
        "parsed": 0,
        "parse_failed": 0,
        "already_processed_skipped": 0,
        "processed_filings": 0,
        "empty_extract_retryable": 0,
        "empty_extract_processed_no_holdings": 0,
        "skipped": 0,
        "position_rows": 0,
        "position_changes": 0,
        "summaries": 0,
        "activity_events": 0,
        "feed_events": 0,
        "errors": 0,
    }
    processed = 0
    max_attempts = max(0, int(max_filings)) if max_filings is not None else None
    db = SessionLocal()
    try:
        for page in range(normalized_start_page, normalized_start_page + page_count):
            if max_attempts is not None and processed >= max_attempts:
                counts["max_filings_reached"] = 1
                return counts
            logger.info("Scanning latest institutional filings page=%s", page)
            rows = fetch_latest_institutional_filings(page=page, limit=max(1, min(int(limit or 100), 500)))
            if not rows:
                counts["first_empty_page_seen"] = page
                break
            counts["pages_scanned"] = int(counts["pages_scanned"] or 0) + 1
            for row in rows:
                if max_attempts is not None and processed >= max_attempts:
                    counts["max_filings_reached"] = 1
                    return counts
                counts["scanned"] = int(counts["scanned"]) + 1
                candidate = parse_latest_filing(row)
                if candidate is None:
                    counts["parse_failed"] = int(counts["parse_failed"]) + 1
                    counts["skipped"] = int(counts["skipped"]) + 1
                    continue
                counts["parsed"] = int(counts["parsed"]) + 1
                try:
                    upsert_institutional_holder(db, candidate)
                    filing, created = upsert_institutional_filing(db, candidate)
                    db.flush()
                    canonical_filing = get_canonical_filing_for_holder_period(
                        db,
                        filing.cik,
                        filing.report_year,
                        filing.report_quarter,
                    )
                    if canonical_filing is not None:
                        filing = canonical_filing
                    if filing.processed_at is not None and not force:
                        if _should_retry_processed_zero_position_filing(db, filing):
                            logger.info(
                                "institutional_retrying_processed_zero_position_filing cik=%s year=%s quarter=%s form_type=%s",
                                filing.cik,
                                filing.report_year,
                                filing.report_quarter,
                                filing.form_type,
                            )
                            filing.processed_at = None
                            db.flush()
                        else:
                            db.commit()
                            counts["already_processed_skipped"] = int(counts["already_processed_skipped"]) + 1
                            counts["skipped"] = int(counts["skipped"]) + 1
                            continue

                    extract_rows = fetch_institutional_filing_extract(
                        cik=candidate.cik,
                        year=candidate.report_year,
                        quarter=candidate.report_quarter,
                    )
                    if not extract_rows:
                        metric = _mark_empty_extract_outcome(db, filing, raw_extract_rows=0)
                        db.commit()
                        processed += 1
                        counts[metric] = int(counts[metric]) + 1
                        if metric == "empty_extract_processed_no_holdings":
                            counts["processed_filings"] = int(counts["processed_filings"]) + 1
                        continue

                    position_counts = upsert_positions_for_filing(db, filing=filing, rows=extract_rows)
                    position_row_count = int(position_counts.get("inserted_positions", 0)) + int(position_counts.get("updated_positions", 0))
                    if position_row_count == 0 and _count_filing_positions(db, filing) == 0:
                        metric = _mark_empty_extract_outcome(
                            db,
                            filing,
                            raw_extract_rows=len(extract_rows),
                            skipped_positions=int(position_counts.get("skipped_positions", 0)),
                        )
                        db.commit()
                        processed += 1
                        counts[metric] = int(counts[metric]) + 1
                        if metric == "empty_extract_processed_no_holdings":
                            counts["processed_filings"] = int(counts["processed_filings"]) + 1
                        continue

                    process_counts = process_filing_changes_and_events(db, filing)
                    db.commit()

                    processed += 1
                    counts["processed_filings"] = int(counts["processed_filings"]) + 1
                    counts["position_rows"] = int(counts["position_rows"]) + position_row_count
                    counts["position_changes"] = int(counts["position_changes"]) + int(process_counts.get("changes", 0))
                    counts["summaries"] = int(counts["summaries"]) + int(process_counts.get("summaries", 0))
                    counts["activity_events"] = int(counts["activity_events"]) + int(process_counts.get("activity_events", 0))
                    counts["feed_events"] = int(counts["feed_events"]) + int(process_counts.get("feed_events", 0))
                    if created:
                        logger.info("Processed new 13F filing cik=%s Q%s %s", candidate.cik, candidate.report_quarter, candidate.report_year)
                except Exception as exc:
                    db.rollback()
                    counts["errors"] = int(counts["errors"]) + 1
                    logger.exception("Failed to process institutional 13F filing row")
                    if isinstance(exc, SQLAlchemyError):
                        return counts
    finally:
        db.close()
    return counts


def ingest_institutional_filing(
    *,
    cik: str,
    year: int,
    quarter: int,
    force: bool = False,
    positions_only: bool = False,
    ensure_schema: bool = True,
) -> dict[str, int | str]:
    if ensure_schema:
        ensure_institutional_activity_schema(engine)
    db = SessionLocal()
    try:
        rows = fetch_institutional_filing_dates(cik=cik)
        candidates = []
        for row in rows:
            parsed = parse_latest_filing({**row, "cik": cik, "year": year, "quarter": quarter})
            if parsed and parsed.report_year == int(year) and parsed.report_quarter == int(quarter):
                candidates.append(parsed)
        if not candidates:
            raise ValueError(f"No 13F filing metadata found for cik={cik} Q{quarter} {year}")
        candidate = max(candidates, key=_candidate_canonical_sort_key)

        upsert_institutional_holder(db, candidate)
        filing, _ = upsert_institutional_filing(db, candidate)
        db.flush()
        canonical_filing = get_canonical_filing_for_holder_period(
            db,
            filing.cik,
            filing.report_year,
            filing.report_quarter,
        )
        if canonical_filing is not None:
            filing = canonical_filing
        if filing.processed_at is not None and not force:
            if _should_retry_processed_zero_position_filing(db, filing):
                logger.info(
                    "institutional_retrying_processed_zero_position_filing cik=%s year=%s quarter=%s form_type=%s",
                    filing.cik,
                    filing.report_year,
                    filing.report_quarter,
                    filing.form_type,
                )
                filing.processed_at = None
                db.flush()
            else:
                db.commit()
                return {"status": "ok", "processed_filings": 0, "skipped": 1}

        if positions_only and not force and _count_filing_positions(db, filing) > 0:
            db.commit()
            return {
                "status": "ok",
                "processed_filings": 0,
                "skipped": 1,
                "position_rows": 0,
                "positions_only": 1,
            }

        extract_rows = fetch_institutional_filing_extract(cik=candidate.cik, year=candidate.report_year, quarter=candidate.report_quarter)
        if not extract_rows:
            metric = _mark_empty_extract_outcome(db, filing, raw_extract_rows=0)
            db.commit()
            return _empty_extract_result(metric)
        position_counts = upsert_positions_for_filing(db, filing=filing, rows=extract_rows)
        position_row_count = int(position_counts.get("inserted_positions", 0)) + int(position_counts.get("updated_positions", 0))
        if position_row_count == 0 and _count_filing_positions(db, filing) == 0:
            metric = _mark_empty_extract_outcome(
                db,
                filing,
                raw_extract_rows=len(extract_rows),
                skipped_positions=int(position_counts.get("skipped_positions", 0)),
            )
            db.commit()
            return _empty_extract_result(metric)

        if positions_only:
            db.commit()
            return {
                "status": "ok",
                "processed_filings": 1,
                "empty_extract_retryable": 0,
                "empty_extract_processed_no_holdings": 0,
                "position_rows": position_row_count,
                "position_changes": 0,
                "summaries": 0,
                "activity_events": 0,
                "feed_events": 0,
                "positions_only": 1,
            }

        process_counts = process_filing_changes_and_events(db, filing)
        db.commit()
        return {
            "status": "ok",
            "processed_filings": 1,
            "empty_extract_retryable": 0,
            "empty_extract_processed_no_holdings": 0,
            "position_rows": position_row_count,
            "position_changes": int(process_counts.get("changes", 0)),
            "summaries": int(process_counts.get("summaries", 0)),
            "activity_events": int(process_counts.get("activity_events", 0)),
            "feed_events": int(process_counts.get("feed_events", 0)),
        }
    finally:
        db.close()


def backfill_institutional_holder(
    *,
    cik: str,
    force: bool = False,
    max_filings: int | None = None,
    positions_only: bool = False,
) -> dict[str, int | str]:
    rows = fetch_institutional_filing_dates(cik=cik)
    candidates = [candidate for row in rows if (candidate := parse_latest_filing({**row, "cik": cik}))]
    candidates.sort(key=lambda item: (item.report_year, item.report_quarter, item.filing_date), reverse=True)
    counts: dict[str, int | str] = {
        "status": "ok",
        "processed_filings": 0,
        "skipped": 0,
        "errors": 0,
        "position_rows": 0,
        "positions_only": 1 if positions_only else 0,
    }
    for candidate in candidates[: max_filings or len(candidates)]:
        try:
            result = ingest_institutional_filing(
                cik=candidate.cik,
                year=candidate.report_year,
                quarter=candidate.report_quarter,
                force=force,
                positions_only=positions_only,
            )
            counts["processed_filings"] = int(counts["processed_filings"]) + int(result.get("processed_filings", 0))
            counts["skipped"] = int(counts["skipped"]) + int(result.get("skipped", 0))
            counts["position_rows"] = int(counts["position_rows"]) + int(result.get("position_rows", 0))
        except Exception:
            counts["errors"] = int(counts["errors"]) + 1
            logger.exception("Failed to backfill 13F filing cik=%s Q%s %s", candidate.cik, candidate.report_quarter, candidate.report_year)
    return counts


def _default_historical_backfill_ciks() -> list[str]:
    return [
        cik
        for row in CANONICAL_INSTITUTIONAL_HOLDER_UNIVERSE
        if (cik := normalize_cik(row.get("cik")))
    ]


def backfill_missing_institutional_period_batch(
    *,
    report_year: int,
    report_quarter: int,
    max_holders: int = 10,
    apply: bool = False,
) -> dict[str, Any]:
    """Recover missing managers from the matching prior 13F period.

    Holdings are loaded before derived position-change events so a large filer
    cannot block the dashboard's quarter-level coverage recovery.
    """
    if report_quarter < 1 or report_quarter > 4:
        raise ValueError("report_quarter must be between 1 and 4")
    reference_year, reference_quarter = (
        (int(report_year) - 1, 4) if int(report_quarter) == 1 else (int(report_year), int(report_quarter) - 1)
    )
    db = SessionLocal()
    try:
        reference_rows = db.execute(
            select(InstitutionalPosition.cik, func.count(InstitutionalPosition.id))
            .where(
                InstitutionalPosition.report_year == reference_year,
                InstitutionalPosition.report_quarter == reference_quarter,
            )
            .group_by(InstitutionalPosition.cik)
            .order_by(func.count(InstitutionalPosition.id), InstitutionalPosition.cik)
        ).all()
        current_ciks = set(
            db.scalars(
                select(InstitutionalPosition.cik)
                .where(
                    InstitutionalPosition.report_year == int(report_year),
                    InstitutionalPosition.report_quarter == int(report_quarter),
                )
                .distinct()
            ).all()
        )
        missing_ciks = [str(cik) for cik, _position_count in reference_rows if cik not in current_ciks]
    finally:
        db.close()

    result = backfill_institutional_historical_batch(
        holder_ciks=missing_ciks,
        start_year=int(report_year),
        end_year=int(report_year),
        max_holders=max(1, int(max_holders)),
        max_filings_total=max(1, int(max_holders)),
        max_filings_per_holder=1,
        positions_only=True,
        apply=apply,
    )

    db = SessionLocal()
    try:
        current_after = int(
            db.execute(
                select(func.count(func.distinct(InstitutionalPosition.cik))).where(
                    InstitutionalPosition.report_year == int(report_year),
                    InstitutionalPosition.report_quarter == int(report_quarter),
                )
            ).scalar_one()
            or 0
        )
    finally:
        db.close()
    reference_count = len(reference_rows)
    return {
        **result,
        "report_year": int(report_year),
        "report_quarter": int(report_quarter),
        "reference_year": reference_year,
        "reference_quarter": reference_quarter,
        "reference_institution_count": reference_count,
        "current_institution_count_before": len(current_ciks),
        "missing_institution_count_before": len(missing_ciks),
        "current_institution_count_after": current_after,
        "coverage_pct_after": round((current_after / reference_count) * 100, 1) if reference_count else None,
    }


def _parse_cik_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    ciks: list[str] = []
    seen: set[str] = set()
    for raw in value.split(","):
        cik = normalize_cik(raw)
        if not cik or cik in seen:
            continue
        seen.add(cik)
        ciks.append(cik)
    return ciks


def _historical_existing_state(db, *, cik: str, year: int, quarter: int) -> dict[str, int | bool | None]:
    filing = get_canonical_filing_for_holder_period(db, cik, int(year), int(quarter))
    if filing is None:
        return {"filing_id": None, "position_count": 0, "processed": False, "retryable_zero_position": False}
    return {
        "filing_id": filing.id,
        "position_count": _count_filing_positions(db, filing),
        "processed": filing.processed_at is not None,
        "retryable_zero_position": _should_retry_processed_zero_position_filing(db, filing),
    }


def backfill_institutional_historical_batch(
    *,
    holder_ciks: list[str] | tuple[str, ...] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    max_holders: int | None = 3,
    max_filings_total: int | None = 10,
    max_filings_per_holder: int | None = 4,
    force: bool = False,
    positions_only: bool = False,
    apply: bool = False,
    target_existing_filing_id: int | None = None,
    symbol_batch_size: int | None = None,
    symbol_cursor: str | None = None,
) -> dict[str, Any]:
    if apply:
        ensure_institutional_activity_schema(engine)

    now_year = datetime.now(timezone.utc).year
    min_year = int(start_year) if start_year is not None else now_year - 5
    max_year = int(end_year) if end_year is not None else now_year
    if min_year > max_year:
        raise ValueError("--historical-start-year cannot be after --historical-end-year")

    source_ciks = list(holder_ciks or _default_historical_backfill_ciks())
    normalized_ciks: list[str] = []
    seen_ciks: set[str] = set()
    for raw_cik in source_ciks:
        cik = normalize_cik(raw_cik)
        if not cik or cik in seen_ciks:
            continue
        seen_ciks.add(cik)
        normalized_ciks.append(cik)
    holder_limit = max(0, int(max_holders)) if max_holders is not None else None
    if holder_limit is not None:
        normalized_ciks = normalized_ciks[:holder_limit]

    total_limit = max(0, int(max_filings_total)) if max_filings_total is not None else None
    per_holder_limit = max(0, int(max_filings_per_holder)) if max_filings_per_holder is not None else None
    counts: dict[str, Any] = {
        "status": "ok",
        "mode": "apply" if apply else "dry_run",
        "positions_only": 1 if positions_only else 0,
        "holders_requested": len(source_ciks),
        "holders_considered": len(normalized_ciks),
        "holders_with_candidates": 0,
        "candidate_filings": 0,
        "selected_filings": 0,
        "skipped_existing": 0,
        "skipped_bounds": 0,
        "processed_filings": 0,
        "processed_existing_position_filings": 0,
        "partial_filings": 0,
        "symbols_processed": 0,
        "symbols_total": 0,
        "next_symbol_cursor": None,
        "active_filing_id": target_existing_filing_id,
        "position_rows": 0,
        "position_changes": 0,
        "summaries": 0,
        "activity_events": 0,
        "feed_events": 0,
        "errors": 0,
        "selected": [],
        "error_details": [],
    }

    selected_total = 0
    db = SessionLocal()
    try:
        if target_existing_filing_id is not None:
            filing = db.get(InstitutionalFiling, int(target_existing_filing_id))
            if filing is None:
                counts["errors"] += 1
                counts["error_details"].append({"filing_id": int(target_existing_filing_id), "error": "existing 13F filing not found"})
                return counts

            position_count = db.query(InstitutionalPosition).filter(InstitutionalPosition.filing_id == filing.id).count()
            counts["holders_with_candidates"] = 1
            counts["candidate_filings"] = 1
            counts["selected_filings"] = 1
            counts["selected"].append(
                {
                    "cik": filing.cik,
                    "holder_name": None,
                    "year": filing.report_year,
                    "quarter": filing.report_quarter,
                    "filing_date": filing.filing_date.isoformat(),
                    "accession_number": filing.accession_number,
                    "existing_filing_id": filing.id,
                    "existing_position_count": position_count,
                    "apply_action": "process_existing_positions",
                }
            )
            if not apply:
                return counts

            try:
                if symbol_batch_size is not None:
                    process_counts = process_filing_changes_and_events_symbol_batch(
                        db,
                        filing,
                        after_symbol=symbol_cursor,
                        symbol_limit=symbol_batch_size,
                    )
                else:
                    process_counts = process_filing_changes_and_events(db, filing)
                db.commit()
                complete = bool(process_counts.get("complete", True))
                counts["active_filing_id"] = filing.id
                counts["symbols_processed"] += int(process_counts.get("symbols_processed", 0))
                counts["symbols_total"] = int(process_counts.get("symbols_total", counts.get("symbols_total") or 0))
                counts["next_symbol_cursor"] = process_counts.get("next_symbol_cursor")
                if complete:
                    counts["processed_filings"] += 1
                    counts["processed_existing_position_filings"] += 1
                else:
                    counts["partial_filings"] += 1
                counts["position_changes"] += int(process_counts.get("changes", 0))
                counts["summaries"] += int(process_counts.get("summaries", 0))
                counts["activity_events"] += int(process_counts.get("activity_events", 0))
                counts["feed_events"] += int(process_counts.get("feed_events", 0))
            except Exception as exc:
                db.rollback()
                counts["errors"] += 1
                counts["error_details"].append(
                    {
                        "filing_id": filing.id,
                        "cik": filing.cik,
                        "year": filing.report_year,
                        "quarter": filing.report_quarter,
                        "error": str(exc)[:240],
                    }
                )
                logger.exception(
                    "Failed to resume historical 13F filing id=%s cik=%s Q%s %s",
                    filing.id,
                    filing.cik,
                    filing.report_quarter,
                    filing.report_year,
                )
            return counts

        for cik in normalized_ciks:
            if total_limit is not None and selected_total >= total_limit:
                break
            try:
                rows = fetch_institutional_filing_dates(cik=cik)
            except Exception as exc:
                counts["errors"] += 1
                counts["error_details"].append({"cik": cik, "error": str(exc)[:240]})
                logger.exception("Failed to fetch historical 13F filing dates cik=%s", cik)
                continue

            by_period = {}
            for row in rows:
                candidate = parse_latest_filing({**row, "cik": cik})
                if candidate is None:
                    continue
                if candidate.report_year < min_year or candidate.report_year > max_year:
                    counts["skipped_bounds"] += 1
                    continue
                key = (candidate.report_year, candidate.report_quarter)
                existing = by_period.get(key)
                if existing is None or _candidate_canonical_sort_key(candidate) > _candidate_canonical_sort_key(existing):
                    by_period[key] = candidate

            candidates = sorted(by_period.values(), key=lambda item: (item.report_year, item.report_quarter, item.filing_date))
            holder_selected = 0
            holder_had_candidate = False
            for candidate in candidates:
                counts["candidate_filings"] += 1
                if total_limit is not None and selected_total >= total_limit:
                    break
                if per_holder_limit is not None and holder_selected >= per_holder_limit:
                    break

                existing_state = _historical_existing_state(
                    db,
                    cik=candidate.cik,
                    year=candidate.report_year,
                    quarter=candidate.report_quarter,
                )
                if target_existing_filing_id is not None and existing_state["filing_id"] != int(target_existing_filing_id):
                    continue
                if not force:
                    if bool(existing_state["processed"]) and not bool(existing_state["retryable_zero_position"]):
                        counts["skipped_existing"] += 1
                        continue
                    if positions_only and int(existing_state["position_count"] or 0) > 0:
                        counts["skipped_existing"] += 1
                        continue

                selected_total += 1
                holder_selected += 1
                holder_had_candidate = True
                selected_row = {
                    "cik": candidate.cik,
                    "holder_name": candidate.holder_name,
                    "year": candidate.report_year,
                    "quarter": candidate.report_quarter,
                    "filing_date": candidate.filing_date.isoformat(),
                    "accession_number": candidate.accession_number,
                    "existing_filing_id": existing_state["filing_id"],
                    "existing_position_count": existing_state["position_count"],
                    "apply_action": (
                        "process_existing_positions"
                        if int(existing_state["position_count"] or 0) > 0 and not positions_only
                        else "fetch_extract"
                    ),
                }
                if len(counts["selected"]) < 100:
                    counts["selected"].append(selected_row)

                if not apply:
                    continue

                try:
                    if selected_row["apply_action"] == "process_existing_positions":
                        filing = get_canonical_filing_for_holder_period(
                            db,
                            candidate.cik,
                            candidate.report_year,
                            candidate.report_quarter,
                        )
                        if filing is None:
                            raise ValueError(
                                f"No existing 13F filing found for cik={candidate.cik} Q{candidate.report_quarter} {candidate.report_year}"
                            )
                        if symbol_batch_size is not None:
                            process_counts = process_filing_changes_and_events_symbol_batch(
                                db,
                                filing,
                                after_symbol=symbol_cursor,
                                symbol_limit=symbol_batch_size,
                            )
                        else:
                            process_counts = process_filing_changes_and_events(db, filing)
                        db.commit()
                        complete = bool(process_counts.get("complete", True))
                        counts["active_filing_id"] = filing.id
                        counts["symbols_processed"] += int(process_counts.get("symbols_processed", 0))
                        counts["symbols_total"] = int(process_counts.get("symbols_total", counts.get("symbols_total") or 0))
                        counts["next_symbol_cursor"] = process_counts.get("next_symbol_cursor")
                        if complete:
                            counts["processed_filings"] += 1
                            counts["processed_existing_position_filings"] += 1
                        else:
                            counts["partial_filings"] += 1
                        counts["position_changes"] += int(process_counts.get("changes", 0))
                        counts["summaries"] += int(process_counts.get("summaries", 0))
                        counts["activity_events"] += int(process_counts.get("activity_events", 0))
                        counts["feed_events"] += int(process_counts.get("feed_events", 0))
                    else:
                        result = ingest_institutional_filing(
                            cik=candidate.cik,
                            year=candidate.report_year,
                            quarter=candidate.report_quarter,
                            force=force,
                            positions_only=positions_only,
                            ensure_schema=False,
                        )
                        counts["processed_filings"] += int(result.get("processed_filings", 0))
                        counts["position_rows"] += int(result.get("position_rows", 0))
                        counts["position_changes"] += int(result.get("position_changes", 0))
                        counts["summaries"] += int(result.get("summaries", 0))
                        counts["activity_events"] += int(result.get("activity_events", 0))
                        counts["feed_events"] += int(result.get("feed_events", 0))
                        counts["skipped_existing"] += int(result.get("skipped", 0))
                except Exception as exc:
                    db.rollback()
                    counts["errors"] += 1
                    counts["error_details"].append(
                        {
                            "cik": candidate.cik,
                            "year": candidate.report_year,
                            "quarter": candidate.report_quarter,
                            "error": str(exc)[:240],
                        }
                    )
                    logger.exception(
                        "Failed to apply historical 13F filing cik=%s Q%s %s",
                        candidate.cik,
                        candidate.report_quarter,
                        candidate.report_year,
                    )
            if holder_had_candidate:
                counts["holders_with_candidates"] += 1
        counts["selected_filings"] = selected_total
    finally:
        db.close()
    return counts


def ingest_holder_enrichment(*, cik: str, year: int | None = None, quarter: int | None = None) -> dict[str, Any]:
    ensure_institutional_activity_schema(engine)
    db = SessionLocal()
    try:
        result: dict[str, Any] = {"status": "ok"}
        performance_rows = fetch_holder_performance_summary(cik=cik)
        result["performance"] = upsert_holder_performance_rows(db, cik, performance_rows)
        if year is not None and quarter is not None:
            breakdown_rows = fetch_holder_industry_breakdown(cik=cik, year=int(year), quarter=int(quarter))
            result["industry_breakdown"] = upsert_holder_industry_breakdown_rows(db, cik, int(year), int(quarter), breakdown_rows)
        db.commit()
        return result
    finally:
        db.close()


def ingest_industry_summary(*, year: int, quarter: int) -> dict[str, int | str]:
    ensure_institutional_activity_schema(engine)
    rows = fetch_industry_summary(year=int(year), quarter=int(quarter))
    db = SessionLocal()
    try:
        counts = upsert_industry_summary_rows(db, int(year), int(quarter), rows)
        db.commit()
        return {"status": "ok", **counts}
    finally:
        db.close()


def seed_canonical_holder_universe() -> dict[str, int]:
    ensure_institutional_activity_schema(engine)
    db = SessionLocal()
    try:
        result = seed_canonical_institutional_holders(db)
        db.commit()
        return result
    finally:
        db.close()


def institutional_activity_ingest_run(*, pages: int, limit: int, max_filings: int = 25, start_page: int = 0) -> dict[str, int | str | None]:
    return ingest_latest_institutional_filings(start_page=start_page, pages=pages, limit=limit, max_filings=max_filings)


def cleanup_institutional_feed_events(*, dry_run: bool = True) -> dict[str, int | str | bool | dict[str, int]]:
    ensure_institutional_activity_schema(engine)
    db = SessionLocal()
    try:
        result = cleanup_overbroad_institutional_feed_events(db, dry_run=dry_run)
        if dry_run:
            db.rollback()
        else:
            db.commit()
        return result
    finally:
        db.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest institutional 13F activity into Walnut Market Terminal.")
    parser.add_argument("--start-page", type=int, default=int(os.getenv("INGEST_INSTITUTIONAL_START_PAGE", "0")))
    parser.add_argument("--pages", type=int, default=int(os.getenv("INGEST_INSTITUTIONAL_PAGES", "1")))
    parser.add_argument("--limit", type=int, default=int(os.getenv("INGEST_INSTITUTIONAL_LIMIT", "100")))
    parser.add_argument("--max-filings", type=int, default=int(os.getenv("INGEST_INSTITUTIONAL_MAX_FILINGS", "25")))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--positions-only", action="store_true", help="Load filing holdings snapshots without generating change/activity/feed events.")
    parser.add_argument("--cik")
    parser.add_argument("--holder-ciks", help="Comma-separated CIKs for bounded historical backfill.")
    parser.add_argument("--year", type=int)
    parser.add_argument("--quarter", type=int)
    parser.add_argument("--historical-backfill", action="store_true", help="Plan a bounded 13F historical backfill. Dry-run unless --apply-historical-backfill is set.")
    parser.add_argument("--apply-historical-backfill", action="store_true", help="Write bounded 13F historical backfill rows.")
    parser.add_argument("--recover-missing-period", action="store_true", help="Recover managers missing from a filing period using the matching prior quarter as the source universe.")
    parser.add_argument("--recovery-year", type=int, default=None)
    parser.add_argument("--recovery-quarter", type=int, default=None)
    parser.add_argument("--historical-job-init", action="store_true", help="Initialize durable historical 13F backfill state.")
    parser.add_argument("--historical-job-config", action="store_true", help="Update durable historical 13F backfill configuration without running it.")
    parser.add_argument("--historical-job-status", action="store_true", help="Print durable historical 13F backfill status.")
    parser.add_argument("--historical-job-run-once", action="store_true", help="Run one durable historical 13F backfill slice.")
    parser.add_argument("--scheduled-historical-once", action="store_true", help="Run one scheduled durable historical 13F backfill slice.")
    parser.add_argument("--historical-job-enable", action="store_true", help="Enable durable historical 13F backfill during --historical-job-init.")
    parser.add_argument("--historical-start-year", type=int, default=None)
    parser.add_argument("--historical-end-year", type=int, default=None)
    parser.add_argument("--historical-symbol-batch-size", type=int, default=None)
    parser.add_argument("--max-holders", type=int, default=3)
    parser.add_argument("--max-filings-total", type=int, default=10)
    parser.add_argument("--max-filings-per-holder", type=int, default=4)
    parser.add_argument("--holder-enrichment", action="store_true")
    parser.add_argument("--industry-summary", action="store_true")
    parser.add_argument("--seed-canonical-holders", action="store_true")
    parser.add_argument("--cleanup-feed-events", action="store_true")
    parser.add_argument("--apply-cleanup", action="store_true")
    parser.add_argument("--job-init", action="store_true", help="Initialize durable latest-filings job state without running ingestion.")
    parser.add_argument("--job-run-once", action="store_true", help="Run one durable latest-filings job window and persist status.")
    parser.add_argument("--require-job-enabled", action="store_true", help="Skip --job-run-once unless the persisted job state is enabled.")
    parser.add_argument("--scheduled-latest-enabled-check", action="store_true", help="Exit zero only when both scheduled latest-filings gates are enabled.")
    parser.add_argument("--scheduled-latest-once", action="store_true", help="Run one scheduled latest-filings page using the persisted cursor.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    try:
        if args.job_init:
            from app.services.institutional_ingest_job import initialize_latest_job_state

            result = initialize_latest_job_state(
                cursor_page=args.start_page,
                pages_per_run=args.pages,
                limit=args.limit,
                max_filings_per_run=args.max_filings,
                enabled=False,
            )
        elif args.scheduled_latest_enabled_check:
            from app.services.institutional_ingest_job import scheduled_latest_enabled_check

            result = scheduled_latest_enabled_check()
            logger.info("Institutional scheduled latest enabled check: %s", result)
            print(result)
            if not result.get("enabled"):
                raise SystemExit(75)
            return
        elif args.scheduled_latest_once:
            from app.services.institutional_ingest_job import run_scheduled_latest_once

            result = run_scheduled_latest_once()
        elif args.job_run_once:
            from app.services.institutional_ingest_job import run_latest_ingest_job_once

            result = run_latest_ingest_job_once(require_enabled=args.require_job_enabled)
        elif args.cleanup_feed_events:
            result = cleanup_institutional_feed_events(dry_run=not args.apply_cleanup)
        elif args.historical_job_init:
            from app.services.institutional_ingest_job import initialize_historical_job_state

            result = initialize_historical_job_state(
                cursor_page=args.start_page,
                start_year=args.historical_start_year,
                end_year=args.historical_end_year,
                holder_ciks=_parse_cik_csv(args.holder_ciks),
                max_filings_per_run=args.max_filings_total,
                symbol_batch_size=args.historical_symbol_batch_size,
                enabled=args.historical_job_enable,
            )
        elif args.historical_job_config:
            from app.services.institutional_ingest_job import update_historical_job_config, historical_job_status_payload

            configured_max_filings = args.max_filings_total if "--max-filings-total" in sys.argv else None
            db = SessionLocal()
            try:
                update_historical_job_config(
                    db,
                    start_year=args.historical_start_year,
                    end_year=args.historical_end_year,
                    holder_ciks=_parse_cik_csv(args.holder_ciks),
                    max_filings_per_run=configured_max_filings,
                    symbol_batch_size=args.historical_symbol_batch_size,
                )
                db.commit()
                result = historical_job_status_payload(db)
            finally:
                db.close()
        elif args.historical_job_status:
            from app.services.institutional_ingest_job import historical_job_status_payload

            db = SessionLocal()
            try:
                result = historical_job_status_payload(db)
            finally:
                db.close()
        elif args.historical_job_run_once:
            from app.services.institutional_ingest_job import run_historical_backfill_once

            result = run_historical_backfill_once(require_enabled=True)
        elif args.scheduled_historical_once:
            from app.services.institutional_ingest_job import run_historical_backfill_once

            result = run_historical_backfill_once(require_enabled=True)
            status = str(result.get("status") or "").lower()
            logger.info("Institutional scheduled historical backfill result: %s", result)
            print(result)
            if status == "failed":
                raise SystemExit(1)
            if status in {"paused", "complete", "skipped_locked"}:
                raise SystemExit(0)
            return
        elif args.recover_missing_period:
            if args.recovery_year is None or args.recovery_quarter is None:
                raise SystemExit("--recover-missing-period requires --recovery-year and --recovery-quarter")
            result = backfill_missing_institutional_period_batch(
                report_year=args.recovery_year,
                report_quarter=args.recovery_quarter,
                max_holders=args.max_holders,
                apply=args.apply_historical_backfill,
            )
        elif args.historical_backfill:
            result = backfill_institutional_historical_batch(
                holder_ciks=_parse_cik_csv(args.holder_ciks),
                start_year=args.historical_start_year,
                end_year=args.historical_end_year,
                max_holders=args.max_holders,
                max_filings_total=args.max_filings_total,
                max_filings_per_holder=args.max_filings_per_holder,
                force=args.force,
                positions_only=args.positions_only,
                apply=args.apply_historical_backfill,
            )
        elif args.industry_summary:
            if args.year is None or args.quarter is None:
                raise SystemExit("--industry-summary requires --year and --quarter")
            result = ingest_industry_summary(year=args.year, quarter=args.quarter)
        elif args.seed_canonical_holders:
            result = seed_canonical_holder_universe()
        elif args.holder_enrichment:
            if not args.cik:
                raise SystemExit("--holder-enrichment requires --cik")
            result = ingest_holder_enrichment(cik=args.cik, year=args.year, quarter=args.quarter)
        elif args.cik and args.year and args.quarter:
            result = ingest_institutional_filing(
                cik=args.cik,
                year=args.year,
                quarter=args.quarter,
                force=args.force,
                positions_only=args.positions_only,
            )
        elif args.cik:
            result = backfill_institutional_holder(
                cik=args.cik,
                force=args.force,
                max_filings=args.max_filings,
                positions_only=args.positions_only,
            )
        else:
            result = ingest_latest_institutional_filings(
                start_page=args.start_page,
                pages=args.pages,
                limit=args.limit,
                force=args.force,
                max_filings=args.max_filings,
            )
    except FMPClientError as exc:
        raise SystemExit(str(exc)) from exc
    logger.info("Institutional activity ingest completed: %s", result)
    print(result)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main()
    else:
        print(
            institutional_activity_ingest_run(
                start_page=int(os.getenv("INGEST_INSTITUTIONAL_START_PAGE", "0")),
                pages=int(os.getenv("INGEST_INSTITUTIONAL_PAGES", "1")),
                limit=int(os.getenv("INGEST_INSTITUTIONAL_LIMIT", "100")),
                max_filings=int(os.getenv("INGEST_INSTITUTIONAL_MAX_FILINGS", "25")),
            )
        )
