from __future__ import annotations

import argparse
import io
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from xml.etree import ElementTree as etree

import requests
from sqlalchemy import delete, select

from app.db import Base, SessionLocal, engine, ensure_house_annual_disclosure_schema
from app.models import HouseAnnualDisclosureDocument, HouseAnnualDisclosureHolding, Member
from app.services.congress_metadata import get_congress_metadata_resolver
from app.utils.symbols import canonical_symbol, classify_symbol

SOURCE = "house_clerk_financial_disclosure"
CLERK_BASE = "https://disclosures-clerk.house.gov"
ZIP_URL_TEMPLATE = f"{CLERK_BASE}/public_disc/financial-pdfs/{{year}}FD.zip"
PDF_URL_TEMPLATE = f"{CLERK_BASE}/public_disc/financial-pdfs/{{year}}/{{document_id}}.pdf"
ANNUAL_FILING_TYPES = {"O", "A", "T"}
USER_AGENT = "congress-tracker annual-disclosure-ingest/1.0"
TARGET_MEMBER_HINTS = {
    "P000197": ("Pelosi", "CA11"),
    "K000389": ("Khanna", "CA17"),
    "F000110": ("Fields", "LA06"),
    "E000296": ("Evans", "PA03"),
}

VALUE_RANGE_RE = re.compile(
    r"(?P<range>"
    r"(?:Over\s+)?\$?\d[\d,]*(?:\s*-\s*\$?\d[\d,]*)?"
    r"|None"
    r")",
    re.I,
)
ASSET_VALUE_RANGE_RE = re.compile(
    r"(?P<range>(?:Over\s+)?\$\d[\d,]*(?:\s*-\s*\$?\d[\d,]*)?|None)",
    re.I,
)
TICKER_RE = re.compile(r"(?:ticker[:\s]+|\()(?P<symbol>[A-Z][A-Z0-9.\-]{0,9})(?:\))?", re.I)
OWNER_RE = re.compile(r"^(SP|DC|JT|self|spouse|dependent|joint)\b[:\s-]*", re.I)
OWNER_SUFFIX_RE = re.compile(r"\b(?P<owner>SP|DC|JT|self|spouse|dependent|joint)\s*$", re.I)
SECTION_START_RE = re.compile(r"assets?\s+and\s+unearned\s+income|assets?\s+unearned\s+income", re.I)
SECTION_STOP_RE = re.compile(r"\b(transactions|liabilities|agreements|earned income|positions held)\b", re.I)
ASSET_TYPE_TERMS = ("stock", "common stock", "equity", "option", "mutual fund", "etf", "bond", "fund")
ASSET_ROW_START_RE = re.compile(r"\([A-Z][A-Z0-9.\-]{0,9}\)\s+\[(?P<asset_code>[A-Z]{1,3})\]")


@dataclass(frozen=True)
class ClerkDisclosureIndexRow:
    member_name: str
    first_name: str | None
    last_name: str | None
    state_district: str | None
    filing_year: int
    filing_type: str | None
    filing_date: date | None
    document_id: str
    member_bioguide_id: str | None = None

    @property
    def report_url(self) -> str:
        return PDF_URL_TEMPLATE.format(year=self.filing_year, document_id=self.document_id)


@dataclass(frozen=True)
class ParsedHolding:
    asset_name: str
    symbol: str | None
    owner: str | None
    asset_type: str | None
    value_range: str | None
    value_min: float | None
    value_max: float | None
    income_type: str | None
    income_range: str | None
    raw_text: str


@dataclass(frozen=True)
class PdfExtractionResult:
    text: str
    extraction_method: str
    pages_processed: int
    parser_errors: list[str]


def _parse_date(value: object | None) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def _clean_text(value: object | None) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text or None


def _amount_range(value: str | None) -> tuple[float | None, float | None]:
    if not value:
        return None, None
    text = value.replace(",", "").replace("$", "").strip()
    if text.lower() == "none":
        return 0.0, 0.0
    over = text.lower().startswith("over ")
    text = re.sub(r"^over\s+", "", text, flags=re.I).strip()
    if "-" in text:
        left, right = [part.strip() for part in text.split("-", 1)]
        try:
            return float(left), float(right)
        except ValueError:
            return None, None
    try:
        parsed = float(text)
    except ValueError:
        return None, None
    return (parsed, None) if over else (parsed, parsed)


def _holding_midpoint(value_min: float | None, value_max: float | None) -> float | None:
    if value_min is not None and value_max is not None:
        return (float(value_min) + float(value_max)) / 2.0
    return float(value_min) if value_min is not None else value_max


def _safe_symbol(raw: str | None) -> str | None:
    symbol = canonical_symbol(raw)
    status, normalized, _detail = classify_symbol(symbol)
    return normalized if status == "eligible" else None


def _symbol_from_asset(text: str) -> str | None:
    for match in TICKER_RE.finditer(text):
        symbol = _safe_symbol(match.group("symbol"))
        if symbol:
            return symbol
    return None


def _asset_type_from_text(text: str) -> str | None:
    lowered = text.lower()
    for term in ASSET_TYPE_TERMS:
        if term in lowered:
            return term
    return None


def _normalize_pdf_line(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\x00", "")).strip()


def _is_asset_table_start(line: str) -> bool:
    lowered = line.lower()
    return bool(
        SECTION_START_RE.search(line)
        or ("asset owner" in lowered and "value of asset" in lowered and "income" in lowered)
    )


def _is_asset_table_noise(line: str) -> bool:
    lowered = line.lower()
    return bool(
        not line
        or lowered.startswith("asset owner")
        or lowered.startswith("value of asset")
        or lowered.startswith("income type")
        or lowered.startswith("filing id")
        or lowered.startswith("name:")
        or lowered.startswith("status:")
        or lowered.startswith("state/district:")
        or lowered.startswith("location:")
        or lowered.startswith("description:")
        or lowered in {"none", "n/a"}
    )


def _asset_type_from_code(asset_code: str | None, row_text: str) -> str | None:
    code = (asset_code or "").upper()
    if code == "ST":
        return "stock"
    if code in {"EF", "ETF"}:
        return "etf"
    if code in {"MF", "MFU"}:
        return "mutual fund"
    if code in {"OP", "OPT"}:
        return "option"
    if code in {"BD", "B"}:
        return "bond"
    return _asset_type_from_text(row_text)


def _clean_asset_name(value: str) -> str:
    text = re.sub(r"\[[A-Z]{1,3}\]", "", value)
    text = re.sub(r"\((?:[A-Z][A-Z0-9.\-]{0,9})\)", "", text)
    return text.strip(" -:,")


def _parse_holding_row(row: str, *, asset_code: str | None = None) -> ParsedHolding | None:
    line = _normalize_pdf_line(row)
    range_match = ASSET_VALUE_RANGE_RE.search(line)
    if not range_match:
        return None
    value_range = range_match.group("range").strip()
    before = line[: range_match.start()].strip(" -:")
    after = line[range_match.end() :].strip(" -:")
    if len(before) < 3:
        return None

    owner = None
    owner_match = OWNER_RE.match(before)
    if owner_match:
        owner = owner_match.group(1).lower()
        before = before[owner_match.end() :].strip(" -:")
    else:
        owner_suffix = OWNER_SUFFIX_RE.search(before)
        if owner_suffix:
            owner = owner_suffix.group("owner").lower()
            before = before[: owner_suffix.start()].strip(" -:")

    symbol = _symbol_from_asset(before)
    value_min, value_max = _amount_range(value_range)
    asset_name = _clean_asset_name(before)
    if not asset_name:
        return None

    income_type = None
    income_range = None
    if after:
        income_match = ASSET_VALUE_RANGE_RE.search(after)
        if income_match:
            income_range = income_match.group("range").strip()
            income_type = after[: income_match.start()].strip(" -:") or None
        else:
            income_type = after

    return ParsedHolding(
        asset_name=asset_name,
        symbol=symbol,
        owner=owner,
        asset_type=_asset_type_from_code(asset_code, line),
        value_range=value_range,
        value_min=value_min,
        value_max=value_max,
        income_type=income_type,
        income_range=income_range,
        raw_text=line,
    )


def _parse_joined_asset_rows(lines: list[str]) -> list[ParsedHolding]:
    rows: list[tuple[str, str | None]] = []
    current: list[str] = []
    current_asset_code: str | None = None
    for line in lines:
        start_match = ASSET_ROW_START_RE.search(line)
        if start_match:
            if current:
                rows.append((" ".join(current), current_asset_code))
            current = [line]
            current_asset_code = start_match.group("asset_code")
        elif current:
            current.append(line)
    if current:
        rows.append((" ".join(current), current_asset_code))

    holdings: list[ParsedHolding] = []
    for row, asset_code in rows:
        holding = _parse_holding_row(row, asset_code=asset_code)
        if holding:
            holdings.append(holding)
    if holdings:
        return holdings

    for line in lines:
        holding = _parse_holding_row(line)
        if holding:
            holdings.append(holding)
    return holdings


def _iter_index_rows(zip_bytes: bytes, *, year: int) -> list[etree.Element]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        xml_name = f"{year}FD.xml"
        with archive.open(xml_name) as handle:
            root = etree.parse(handle).getroot()
    return list(root.findall(".//Member"))


def _index_row_from_xml(
    node: etree.Element,
    *,
    year: int,
    metadata,
    fallback_bioguide_id: str | None = None,
) -> ClerkDisclosureIndexRow:
    first = _clean_text(node.findtext("First"))
    last = _clean_text(node.findtext("Last"))
    state_district = _clean_text(node.findtext("StateDst"))
    state = state_district[:2] if state_district and len(state_district) >= 2 else None
    district = state_district[2:] if state_district and len(state_district) > 2 else None
    resolved = None
    if not fallback_bioguide_id and metadata is not None:
        resolved = metadata.resolve(
            first_name=first,
            last_name=last,
            full_name=f"{first or ''} {last or ''}".strip() or None,
            chamber="house",
            state=state,
            house_district=district,
        )
    return ClerkDisclosureIndexRow(
        member_name=f"{first or ''} {last or ''}".strip() or last or first or "Unknown",
        first_name=first,
        last_name=last,
        state_district=state_district,
        filing_year=year,
        filing_type=_clean_text(node.findtext("FilingType")),
        filing_date=_parse_date(node.findtext("FilingDate")),
        document_id=str(node.findtext("DocID") or "").strip(),
        member_bioguide_id=fallback_bioguide_id or (resolved.bioguide_id if resolved else None),
    )


def _fetch_bytes(url: str, *, session: requests.Session, timeout_s: int = 45) -> bytes:
    response = session.get(url, timeout=timeout_s)
    response.raise_for_status()
    return response.content


def _is_near_blank_pdf_text(text: str) -> bool:
    return len(re.sub(r"[\s\x00]+", "", text or "")) < 50


def _extract_pdf_text(pdf_bytes: bytes) -> PdfExtractionResult:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    return PdfExtractionResult(
        text=text,
        extraction_method="pypdf",
        pages_processed=len(reader.pages),
        parser_errors=[],
    )


def _extract_pdf_text_with_ocr(pdf_bytes: bytes, *, max_pages: int = 4) -> PdfExtractionResult:
    pypdf_result = _extract_pdf_text(pdf_bytes)
    if not _is_near_blank_pdf_text(pypdf_result.text):
        return pypdf_result

    errors: list[str] = []
    try:
        from pdf2image import convert_from_bytes
        import pytesseract
    except Exception as exc:
        return PdfExtractionResult(
            text=pypdf_result.text,
            extraction_method="pypdf",
            pages_processed=pypdf_result.pages_processed,
            parser_errors=[f"ocr_unavailable: {exc}"],
        )

    try:
        images = convert_from_bytes(
            pdf_bytes,
            dpi=100,
            first_page=1,
            last_page=max(max_pages, 1),
            fmt="png",
            grayscale=True,
            thread_count=1,
        )
        ocr_pages: list[str] = []
        for index, image in enumerate(images, start=1):
            try:
                ocr_pages.append(pytesseract.image_to_string(image, config="--psm 11", timeout=20))
            except RuntimeError as exc:
                errors.append(f"ocr_page_{index}_failed: {exc}")
        ocr_text = "\n".join(ocr_pages)
        return PdfExtractionResult(
            text=ocr_text,
            extraction_method="ocr",
            pages_processed=len(images),
            parser_errors=errors,
        )
    except Exception as exc:
        return PdfExtractionResult(
            text=pypdf_result.text,
            extraction_method="pypdf",
            pages_processed=pypdf_result.pages_processed,
            parser_errors=[f"ocr_failed: {exc}"],
        )


def parse_holdings_from_pdf_text(text: str) -> list[ParsedHolding]:
    in_assets = False
    asset_lines: list[str] = []
    for raw_line in text.replace("\x00", "").splitlines():
        line = _normalize_pdf_line(raw_line)
        if not line:
            continue
        if _is_asset_table_start(line):
            in_assets = True
            continue
        if in_assets and SECTION_STOP_RE.search(line):
            break
        if not in_assets:
            continue
        if _is_asset_table_noise(line):
            continue
        asset_lines.append(line)
    return _parse_joined_asset_rows(asset_lines)


def _target_last_name(db, member_id: str | None) -> str | None:
    if not member_id:
        return None
    hint = TARGET_MEMBER_HINTS.get(member_id.upper())
    if hint:
        return hint[0]
    member = db.execute(select(Member).where(Member.bioguide_id == member_id)).scalar_one_or_none()
    return member.last_name if member and member.last_name else None


def _select_index_rows(
    db,
    rows: list[etree.Element],
    *,
    year: int,
    member_id: str | None,
    all_members: bool,
    metadata,
) -> list[ClerkDisclosureIndexRow]:
    last_name_hint = _target_last_name(db, member_id)
    target_hint = TARGET_MEMBER_HINTS.get(member_id.upper()) if member_id else None
    selected: list[ClerkDisclosureIndexRow] = []
    for node in rows:
        if last_name_hint and (node.findtext("Last") or "").strip().lower() != last_name_hint.lower():
            continue
        state_district = (node.findtext("StateDst") or "").strip()
        fallback_bioguide_id = (
            member_id
            if member_id and target_hint and state_district.upper() == target_hint[1]
            else None
        )
        row = _index_row_from_xml(
            node,
            year=year,
            metadata=metadata,
            fallback_bioguide_id=fallback_bioguide_id,
        )
        if not row.document_id or row.filing_type not in ANNUAL_FILING_TYPES:
            continue
        if member_id and row.member_bioguide_id != member_id:
            continue
        if not all_members and not member_id:
            continue
        selected.append(row)
    return selected


def _upsert_document_and_holdings(
    db,
    row: ClerkDisclosureIndexRow,
    holdings: list[ParsedHolding],
    extraction: PdfExtractionResult | None = None,
) -> tuple[bool, int]:
    document = db.execute(
        select(HouseAnnualDisclosureDocument).where(HouseAnnualDisclosureDocument.document_id == row.document_id)
    ).scalar_one_or_none()
    inserted_document = document is None
    payload = {
        "state_district": row.state_district,
        "first_name": row.first_name,
        "last_name": row.last_name,
    }
    if extraction is not None:
        payload.update(
            {
                "extraction_method": extraction.extraction_method,
                "pages_processed": extraction.pages_processed,
                "parser_errors": extraction.parser_errors,
            }
        )
    if document is None:
        document = HouseAnnualDisclosureDocument(
            source=SOURCE,
            member_name=row.member_name,
            member_bioguide_id=row.member_bioguide_id,
            filing_year=row.filing_year,
            filing_type=row.filing_type,
            report_url=row.report_url,
            document_id=row.document_id,
            filing_date=row.filing_date,
            state_district=row.state_district,
            payload_json=json.dumps(payload, sort_keys=True),
        )
        db.add(document)
        db.flush()
    else:
        document.member_name = row.member_name
        document.member_bioguide_id = row.member_bioguide_id
        document.filing_year = row.filing_year
        document.filing_type = row.filing_type
        document.report_url = row.report_url
        document.filing_date = row.filing_date
        document.state_district = row.state_district
        document.payload_json = json.dumps(payload, sort_keys=True)
        db.execute(delete(HouseAnnualDisclosureHolding).where(HouseAnnualDisclosureHolding.document_row_id == document.id))

    holding_rows = [
        HouseAnnualDisclosureHolding(
            document_row_id=document.id,
            source=SOURCE,
            member_name=row.member_name,
            member_bioguide_id=row.member_bioguide_id,
            filing_year=row.filing_year,
            filing_type=row.filing_type,
            filing_date=row.filing_date,
            report_url=row.report_url,
            document_id=row.document_id,
            asset_name=holding.asset_name,
            symbol=holding.symbol,
            owner=holding.owner,
            asset_type=holding.asset_type,
            value_range=holding.value_range,
            value_min=holding.value_min,
            value_max=holding.value_max,
            income_type=holding.income_type,
            income_range=holding.income_range,
            payload_json=json.dumps({"raw_text": holding.raw_text}, sort_keys=True),
        )
        for holding in holdings
    ]
    db.add_all(holding_rows)
    return inserted_document, len(holding_rows)


def ingest_house_annual_disclosures(
    *,
    year: int,
    member_id: str | None = None,
    all_members: bool = False,
    apply: bool = False,
    max_documents: int | None = None,
) -> dict[str, Any]:
    if not member_id and not all_members:
        raise ValueError("Pass --member-id or --all-members.")
    Base.metadata.create_all(bind=engine)
    ensure_house_annual_disclosure_schema()
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    db = SessionLocal()
    try:
        metadata = None if member_id and member_id.upper() in TARGET_MEMBER_HINTS else get_congress_metadata_resolver()
        zip_bytes = _fetch_bytes(ZIP_URL_TEMPLATE.format(year=year), session=session)
        index_nodes = _iter_index_rows(zip_bytes, year=year)
        index_rows = _select_index_rows(
            db,
            index_nodes,
            year=year,
            member_id=member_id,
            all_members=all_members,
            metadata=metadata,
        )
        if max_documents is not None:
            index_rows = index_rows[: max(max_documents, 0)]
        documents_seen = 0
        documents_inserted = 0
        holdings_parsed = 0
        holdings_inserted = 0
        documents: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for row in index_rows:
            documents_seen += 1
            try:
                pdf_bytes = _fetch_bytes(row.report_url, session=session)
                extraction = _extract_pdf_text_with_ocr(pdf_bytes)
                parsed_holdings = parse_holdings_from_pdf_text(extraction.text)
                holdings_parsed += len(parsed_holdings)
                resolved_symbols = sorted({holding.symbol for holding in parsed_holdings if holding.symbol})
                unresolved_holdings = [
                    {
                        "asset_name": holding.asset_name,
                        "value_range": holding.value_range,
                    }
                    for holding in parsed_holdings
                    if not holding.symbol
                ][:20]
                documents.append(
                    {
                        "document_id": row.document_id,
                        "member_name": row.member_name,
                        "member_bioguide_id": row.member_bioguide_id,
                        "filing_type": row.filing_type,
                        "filing_date": row.filing_date.isoformat() if row.filing_date else None,
                        "report_url": row.report_url,
                        "holdings_parsed": len(parsed_holdings),
                        "tickers_resolved": len(resolved_symbols),
                        "resolved_symbols": resolved_symbols[:50],
                        "unresolved_holdings_count": len([holding for holding in parsed_holdings if not holding.symbol]),
                        "unresolved_holdings_sample": unresolved_holdings,
                        "extraction_method": extraction.extraction_method,
                        "pages_processed": extraction.pages_processed,
                        "parser_errors": extraction.parser_errors,
                        "parser_error": "; ".join(extraction.parser_errors) if extraction.parser_errors else None,
                    }
                )
                if apply:
                    inserted_document, inserted_holdings = _upsert_document_and_holdings(db, row, parsed_holdings, extraction)
                    documents_inserted += 1 if inserted_document else 0
                    holdings_inserted += inserted_holdings
                    db.commit()
                else:
                    db.rollback()
            except Exception as exc:
                db.rollback()
                documents.append(
                    {
                        "document_id": row.document_id,
                        "member_name": row.member_name,
                        "member_bioguide_id": row.member_bioguide_id,
                        "filing_type": row.filing_type,
                        "filing_date": row.filing_date.isoformat() if row.filing_date else None,
                        "report_url": row.report_url,
                        "holdings_parsed": 0,
                        "tickers_resolved": 0,
                        "resolved_symbols": [],
                        "unresolved_holdings_count": 0,
                        "unresolved_holdings_sample": [],
                        "extraction_method": None,
                        "pages_processed": 0,
                        "parser_errors": [str(exc)],
                        "parser_error": str(exc),
                    }
                )
                errors.append({"document_id": row.document_id, "member_name": row.member_name, "error": str(exc)})
        return {
            "status": "ok" if not errors else "partial",
            "source": SOURCE,
            "year": year,
            "member_id": member_id,
            "all_members": all_members,
            "apply": apply,
            "index_rows": len(index_nodes),
            "annual_documents_selected": len(index_rows),
            "documents_seen": documents_seen,
            "documents_inserted": documents_inserted,
            "holdings_parsed": holdings_parsed,
            "holdings_inserted": holdings_inserted,
            "documents": documents,
            "errors": errors[:10],
        }
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest House Clerk annual financial disclosure holdings.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--member-id", default=None)
    parser.add_argument("--all-members", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-documents", type=int, default=None)
    args = parser.parse_args()
    result = ingest_house_annual_disclosures(
        year=args.year,
        member_id=args.member_id,
        all_members=args.all_members,
        apply=bool(args.apply and not args.dry_run),
        max_documents=args.max_documents,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
