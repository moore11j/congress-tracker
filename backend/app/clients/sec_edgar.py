"""Small, rate-limited client for primary-source SEC EDGAR 13F data."""

from __future__ import annotations

import os
import time
import xml.etree.ElementTree as ET
from datetime import date
from typing import Any

import requests

from app.services.institutional_activity import normalize_cik


SEC_DATA_BASE_URL = "https://data.sec.gov"
SEC_ARCHIVES_BASE_URL = "https://www.sec.gov/Archives/edgar/data"
SEC_REQUEST_INTERVAL_SECONDS = 0.12


class SecEdgarClientError(RuntimeError):
    pass


_last_request_at = 0.0


def _headers() -> dict[str, str]:
    return {
        "User-Agent": os.getenv(
            "SEC_EDGAR_USER_AGENT",
            "Walnut Market Terminal institutional research contact@walnutmarkets.com",
        ).strip(),
        "Accept-Encoding": "gzip, deflate",
    }


def _request(url: str, *, expect_json: bool) -> Any:
    global _last_request_at
    wait_seconds = SEC_REQUEST_INTERVAL_SECONDS - (time.monotonic() - _last_request_at)
    if wait_seconds > 0:
        time.sleep(wait_seconds)
    try:
        response = requests.get(url, headers=_headers(), timeout=30)
    except requests.RequestException as exc:
        raise SecEdgarClientError(f"SEC EDGAR request failed for {url}: {exc}") from exc
    finally:
        _last_request_at = time.monotonic()
    if response.status_code == 404:
        return None
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise SecEdgarClientError(f"SEC EDGAR error ({response.status_code}) for {url}: {response.text[:200]}") from exc
    if not expect_json:
        return response.content
    try:
        return response.json()
    except ValueError as exc:
        raise SecEdgarClientError(f"SEC EDGAR returned invalid JSON for {url}") from exc


def _as_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _quarter_end(year: int, quarter: int) -> date:
    return date(year, quarter * 3, 31 if quarter in {1, 4} else 30)


def _text(node: ET.Element, path: str) -> str | None:
    found = node.find(path)
    if found is None or found.text is None:
        return None
    value = found.text.strip()
    return value or None


def _number(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


def _archive_url(cik: str, accession_number: str, filename: str | None = None) -> str:
    accession_path = accession_number.replace("-", "")
    base = f"{SEC_ARCHIVES_BASE_URL}/{int(cik)}/{accession_path}"
    return f"{base}/{filename}" if filename else base


def fetch_13f_filing_metadata(*, cik: str, report_year: int, report_quarter: int) -> list[dict[str, Any]]:
    """Return SEC 13F-HR/13F-HR-A metadata for an exact reporting period."""
    normalized_cik = normalize_cik(cik)
    if not normalized_cik:
        return []
    payload = _request(f"{SEC_DATA_BASE_URL}/submissions/CIK{normalized_cik}.json", expect_json=True)
    if not isinstance(payload, dict):
        return []
    recent = (payload.get("filings") or {}).get("recent")
    if not isinstance(recent, dict):
        return []
    forms = recent.get("form")
    if not isinstance(forms, list):
        return []
    expected_report_date = _quarter_end(int(report_year), int(report_quarter))
    rows: list[dict[str, Any]] = []
    for index, form in enumerate(forms):
        normalized_form = str(form or "").upper().strip()
        if normalized_form not in {"13F-HR", "13F-HR/A"}:
            continue
        report_date = _as_date((recent.get("reportDate") or [None])[index] if index < len(recent.get("reportDate") or []) else None)
        if report_date != expected_report_date:
            continue
        accession = (recent.get("accessionNumber") or [None])[index] if index < len(recent.get("accessionNumber") or []) else None
        filing_date = (recent.get("filingDate") or [None])[index] if index < len(recent.get("filingDate") or []) else None
        primary_document = (recent.get("primaryDocument") or [None])[index] if index < len(recent.get("primaryDocument") or []) else None
        if not isinstance(accession, str) or not accession:
            continue
        rows.append(
            {
                "cik": normalized_cik,
                "holderName": payload.get("name"),
                "accessionNumber": accession,
                "filingDate": filing_date,
                "reportDate": report_date.isoformat(),
                "reportYear": int(report_year),
                "reportQuarter": int(report_quarter),
                "formType": normalized_form,
                "primaryDocument": primary_document,
                "filingUrl": _archive_url(normalized_cik, accession, primary_document if isinstance(primary_document, str) else None),
                "source": "sec_edgar",
            }
        )
    return rows


def _information_table_filenames(*, cik: str, accession_number: str) -> list[str]:
    payload = _request(f"{_archive_url(cik, accession_number)}/index.json", expect_json=True)
    items = ((payload or {}).get("directory") or {}).get("item") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return []
    xml_names = [str(item.get("name")) for item in items if isinstance(item, dict) and str(item.get("name") or "").lower().endswith(".xml")]
    preferred: list[str] = []
    for name in xml_names:
        normalized = name.lower().replace("_", "").replace("-", "")
        if "infotable" in normalized or "informationtable" in normalized or "inftab" in normalized:
            preferred.append(name)
    # Some filers call the information table simply ``13F.xml``.  Inspect the
    # remaining XML attachments too; only the document containing infoTable
    # elements is accepted by the parser below.
    return preferred + [name for name in xml_names if name not in preferred]


def fetch_13f_information_table(*, cik: str, accession_number: str) -> list[dict[str, Any]]:
    """Fetch an SEC XML information table and normalize values to USD."""
    normalized_cik = normalize_cik(cik)
    if not normalized_cik:
        return []
    filenames = _information_table_filenames(cik=normalized_cik, accession_number=accession_number)
    if not filenames:
        return []
    for filename in filenames:
        payload = _request(_archive_url(normalized_cik, accession_number, filename), expect_json=False)
        if not isinstance(payload, bytes):
            continue
        try:
            root = ET.fromstring(payload)
        except ET.ParseError:
            continue
        rows: list[dict[str, Any]] = []
        for item in root.findall(".//{*}infoTable"):
            cusip = _text(item, "{*}cusip")
            if not cusip:
                continue
            value_usd = _number(_text(item, "{*}value"))
            rows.append(
                {
                    "cusip": cusip,
                    "issuerName": _text(item, "{*}nameOfIssuer"),
                    "shares": _number(_text(item, "{*}shrsOrPrnAmt/{*}sshPrnamt")),
                    # The XML information-table values returned for the
                    # recovered filings are already dollar-denominated.
                    "valueUsd": value_usd,
                    "putCall": _text(item, "{*}putCall"),
                    "investmentDiscretion": _text(item, "{*}investmentDiscretion"),
                    "votingAuthority": {
                        "sole": _number(_text(item, "{*}votingAuthority/{*}Sole")),
                        "shared": _number(_text(item, "{*}votingAuthority/{*}Shared")),
                        "none": _number(_text(item, "{*}votingAuthority/{*}None")),
                    },
                    "source": "sec_edgar",
                }
            )
        if rows:
            return rows
    return []
