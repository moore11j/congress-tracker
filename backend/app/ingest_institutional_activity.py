from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

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
    parse_latest_filing,
    process_filing_changes_and_events,
    cleanup_overbroad_institutional_feed_events,
    get_canonical_filing_for_holder_period,
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
) -> dict[str, int | str]:
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
) -> dict[str, int | str]:
    rows = fetch_institutional_filing_dates(cik=cik)
    candidates = [candidate for row in rows if (candidate := parse_latest_filing({**row, "cik": cik}))]
    candidates.sort(key=lambda item: (item.report_year, item.report_quarter, item.filing_date), reverse=True)
    counts: dict[str, int | str] = {"status": "ok", "processed_filings": 0, "skipped": 0, "errors": 0}
    for candidate in candidates[: max_filings or len(candidates)]:
        try:
            result = ingest_institutional_filing(
                cik=candidate.cik,
                year=candidate.report_year,
                quarter=candidate.report_quarter,
                force=force,
            )
            counts["processed_filings"] = int(counts["processed_filings"]) + int(result.get("processed_filings", 0))
            counts["skipped"] = int(counts["skipped"]) + int(result.get("skipped", 0))
        except Exception:
            counts["errors"] = int(counts["errors"]) + 1
            logger.exception("Failed to backfill 13F filing cik=%s Q%s %s", candidate.cik, candidate.report_quarter, candidate.report_year)
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
    parser.add_argument("--cik")
    parser.add_argument("--year", type=int)
    parser.add_argument("--quarter", type=int)
    parser.add_argument("--holder-enrichment", action="store_true")
    parser.add_argument("--industry-summary", action="store_true")
    parser.add_argument("--cleanup-feed-events", action="store_true")
    parser.add_argument("--apply-cleanup", action="store_true")
    parser.add_argument("--job-init", action="store_true", help="Initialize durable latest-filings job state without running ingestion.")
    parser.add_argument("--job-run-once", action="store_true", help="Run one durable latest-filings job window and persist status.")
    parser.add_argument("--require-job-enabled", action="store_true", help="Skip --job-run-once unless the persisted job state is enabled.")
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
        elif args.job_run_once:
            from app.services.institutional_ingest_job import run_latest_ingest_job_once

            result = run_latest_ingest_job_once(require_enabled=args.require_job_enabled)
        elif args.cleanup_feed_events:
            result = cleanup_institutional_feed_events(dry_run=not args.apply_cleanup)
        elif args.industry_summary:
            if args.year is None or args.quarter is None:
                raise SystemExit("--industry-summary requires --year and --quarter")
            result = ingest_industry_summary(year=args.year, quarter=args.quarter)
        elif args.holder_enrichment:
            if not args.cik:
                raise SystemExit("--holder-enrichment requires --cik")
            result = ingest_holder_enrichment(cik=args.cik, year=args.year, quarter=args.quarter)
        elif args.cik and args.year and args.quarter:
            result = ingest_institutional_filing(cik=args.cik, year=args.year, quarter=args.quarter, force=args.force)
        elif args.cik:
            result = backfill_institutional_holder(cik=args.cik, force=args.force, max_filings=args.max_filings)
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
