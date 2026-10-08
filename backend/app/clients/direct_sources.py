"""Primary source transport and discovery. No FMP fallback or public writes."""
from __future__ import annotations

import io
import json
import math
import os
import re
import time
import zipfile
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urljoin, urlsplit
from xml.etree import ElementTree as ET

import requests
from lxml import html
from pypdf import PdfReader


class DirectSourceError(RuntimeError):
    pass


class DirectSourceClient:
    """Serial, identified requests; redirects stay inside the exact allowlist."""

    def __init__(self, *, issuer_hosts=(), session=None, interval=0.5):
        self.hosts = {"www.sec.gov", "data.sec.gov", "disclosures-clerk.house.gov", "efdsearch.senate.gov", *issuer_hosts}
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": os.getenv("SEC_EDGAR_USER_AGENT", "Walnut Markets source research contact@walnutmarkets.com")})
        self.interval = max(0.5, interval)
        self.last_request = 0.0

    def get(self, url: str) -> bytes:
        for _redirect in range(4):
            parts = urlsplit(url)
            if parts.scheme != "https" or parts.hostname not in self.hosts or parts.username or parts.password or parts.port not in (None, 443):
                raise DirectSourceError("Source URL outside the approved HTTPS host list")
            for attempt in range(3):
                time.sleep(max(0, self.interval - (time.monotonic() - self.last_request)))
                try:
                    response = self.session.get(url, timeout=(10, 40), allow_redirects=False, stream=True)
                    self.last_request = time.monotonic()
                    if response.status_code in (429, 500, 502, 503, 504):
                        retry = response.headers.get("Retry-After", "2")
                        response.close()
                        if attempt == 2:
                            raise DirectSourceError(f"Source HTTP {response.status_code}: {url}")
                        # Do not sleep through a long server cooldown or retry sooner.
                        if not retry.isdigit() or int(retry) > 30:
                            raise DirectSourceError(f"Source cooldown: {url}")
                        time.sleep(max(int(retry), 2 ** attempt))
                        continue
                    break
                except requests.RequestException as exc:
                    if attempt == 2:
                        raise DirectSourceError(f"Source transport failed: {url}") from exc
                    time.sleep(2 ** attempt)
            with response:
                if response.is_redirect:
                    url = urljoin(url, response.headers["Location"])
                    continue
                if response.status_code != 200:
                    raise DirectSourceError(f"Source HTTP {response.status_code}: {url}")
                data = bytearray()
                for chunk in response.iter_content(65536):
                    data.extend(chunk)
                    if len(data) > 30_000_000:
                        raise DirectSourceError(f"Source exceeds size limit: {url}")
                return bytes(data)
        raise DirectSourceError("Too many source redirects")

    def json(self, url):
        try:
            return json.loads(self.get(url))
        except (ValueError, UnicodeError) as exc:
            raise DirectSourceError(f"Expected source JSON: {url}") from exc


def parse_sec_index(raw: bytes, *, forms=("4", "4/A")) -> list[dict]:
    rows = []
    text = raw.decode("utf-8", errors="replace")
    if not re.search(r"^CIK\|Company Name\|Form Type\|Date Filed\|File\s*Name\s*$", text, re.I | re.M):
        raise DirectSourceError("SEC master index header missing")
    for line in text.splitlines():
        fields = line.split("|")
        if len(fields) != 5 or fields[2] not in forms:
            continue
        cik, name, form, filed, filename = fields
        if not re.fullmatch(r"edgar/data/\d+/\d{10}-\d{2}-\d{6}\.txt", filename):
            raise DirectSourceError("Unrecognized SEC filing path")
        rows.append({"key": filename.rsplit("/", 1)[1][:-4], "cik": cik.zfill(10), "name": name,
                     "form": form, "filing_date": date.fromisoformat(filed).isoformat(),
                     "url": "https://www.sec.gov/Archives/" + filename})
    # Joint filers can occupy multiple index lines for one accession.
    return list({row["key"]: row for row in rows}.values())


def ownership_xml(raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace")
    matches = re.findall(r"<ownershipDocument\b.*?</ownershipDocument>", text, re.S | re.I)
    if len(matches) != 1:
        raise DirectSourceError("Expected exactly one ownership XML document")
    return matches[0]


def validated_ownership_xml(raw: bytes, metadata: dict) -> str:
    """Bind the ownership payload to the actual EDGAR submission identity."""
    text = raw.decode('utf-8', errors='replace')
    header = text.split('</SEC-HEADER>', 1)[0]
    accession = re.search(r'ACCESSION NUMBER:\s*(\d{10}-\d{2}-\d{6})', header)
    filed = re.search(r'FILED AS OF DATE:\s*(\d{8})', header)
    if not accession or accession[1] != metadata['key'] or not filed:
        raise DirectSourceError('SEC Form 4 submission identity missing or mismatched')
    if datetime.strptime(filed[1], '%Y%m%d').date().isoformat() != metadata['filing_date']:
        raise DirectSourceError('SEC Form 4 filing date does not match discovery')
    xml = ownership_xml(raw)
    root = ET.fromstring(xml)
    form = (root.findtext('{*}documentType') or '').strip()
    if form != metadata['form'] or form not in {'4', '4/A'}:
        raise DirectSourceError('SEC ownership form does not match discovery')
    ciks = {str(node.text or '').strip().zfill(10) for node in root.iter()
            if node.tag.split('}')[-1] in {'issuerCik', 'rptOwnerCik'}}
    if str(metadata['cik']).zfill(10) not in ciks:
        raise DirectSourceError('SEC ownership issuer/owner does not match index CIK')
    return xml


def parse_13f_submission(raw: bytes, metadata: dict) -> tuple[str, dict]:
    """Validate a complete modern EDGAR submission before staging its holdings."""
    from app.clients.sec_edgar import parse_13f_information_table

    text = raw.decode("utf-8")
    header = text.split("</SEC-HEADER>", 1)[0]
    accession = re.search(r"ACCESSION NUMBER:\s*(\d{10}-\d{2}-\d{6})", header)
    filed = re.search(r"FILED AS OF DATE:\s*(\d{8})", header)
    if not accession or accession[1] != metadata["key"] or not filed:
        raise DirectSourceError("SEC 13F submission identity missing or mismatched")
    filing_date = datetime.strptime(filed[1], "%Y%m%d").date()
    if filing_date.isoformat() != metadata["filing_date"]:
        raise DirectSourceError("SEC 13F filing date does not match discovery")
    if filing_date < date(2023, 1, 3):
        raise DirectSourceError("Historical 13F value units require separate validation")

    documents = []
    for block in re.findall(r"<DOCUMENT>(.*?)</DOCUMENT>", text, re.S):
        filename = re.search(r"<FILENAME>([^\r\n]+)", block)
        xml = re.search(r"<XML>\s*(.*?)\s*</XML>", block, re.S)
        if xml:
            if not filename or not re.fullmatch(r"[A-Za-z0-9_.-]+", filename[1]):
                raise DirectSourceError("SEC 13F attachment name invalid")
            payload = xml[1].encode()
            documents.append((filename[1], payload, ET.fromstring(payload)))
    covers = [root for _, _, root in documents if root.tag.split("}")[-1] == "edgarSubmission"]
    tables = [(name, payload, root) for name, payload, root in documents if root.tag.split("}")[-1] == "informationTable"]
    if len(covers) != 1 or len(tables) != 1:
        raise DirectSourceError("SEC 13F requires exactly one cover and one information table")
    cover = covers[0]

    def value(path):
        return (cover.findtext(path) or "").strip()

    def flag(path):
        raw_flag = value(path).lower()
        if raw_flag not in {"", "true", "false", "1", "0"}:
            raise DirectSourceError("SEC 13F boolean field invalid")
        return raw_flag in {"true", "1"}

    cik = value(".//{*}filer/{*}credentials/{*}cik").zfill(10)
    form = value(".//{*}submissionType")
    if cik != metadata["cik"] or form != metadata["form"] or form not in {"13F-HR", "13F-HR/A"}:
        raise DirectSourceError("SEC 13F manager or form does not match discovery")
    period = datetime.strptime(value(".//{*}reportCalendarOrQuarter"), "%m-%d-%Y").date()
    if (period.month, period.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)} or period > filing_date:
        raise DirectSourceError("SEC 13F report period invalid")
    expected_count = int(value(".//{*}tableEntryTotal"))
    try:
        expected_value = Decimal(value(".//{*}tableValueTotal").replace(",", ""))
        source_nodes = tables[0][2].findall(".//{*}infoTable")
        total = Decimal(0)
        for node in source_nodes:
            for path in ("{*}value", "{*}shrsOrPrnAmt/{*}sshPrnamt"):
                number = Decimal((node.findtext(path) or "").replace(",", ""))
                if not number.is_finite() or number < 0 or not math.isfinite(float(number)):
                    raise DirectSourceError("SEC 13F holding number invalid")
                if path == "{*}value":
                    total += number
            if not (node.findtext("{*}cusip") or "").strip():
                raise DirectSourceError("SEC 13F holding missing CUSIP")
            if (node.findtext("{*}shrsOrPrnAmt/{*}sshPrnamtType") or "").strip() not in {"SH", "PRN"}:
                raise DirectSourceError("SEC 13F share/principal type invalid")
        if not expected_value.is_finite() or len(source_nodes) != expected_count or total != expected_value:
            raise DirectSourceError("SEC 13F cover count/value does not match information table")
    except InvalidOperation as exc:
        raise DirectSourceError("SEC 13F holding/cover number missing or invalid") from exc
    source_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{metadata['key'].replace('-', '')}/{tables[0][0]}"
    rows = parse_13f_information_table(tables[0][1], accession_number=metadata["key"], source_url=source_url)
    if len(rows) != expected_count or not rows:
        raise DirectSourceError("SEC 13F row coverage incomplete or empty")
    for index, row in enumerate(rows):
        row["source_line_ref"] = str(index + 1)
    return text, {"metadata": {**metadata, "report_period": period.isoformat(),
                               "report_year": period.year, "report_quarter": period.month // 3,
                               "amendment_type": value(".//{*}amendmentType") or None,
                               "is_amendment": form.endswith("/A") or flag(".//{*}isAmendment"),
                               "confidential_omitted": flag(".//{*}isConfidentialOmitted"),
                               "table_entry_total": expected_count, "table_value_total_usd": str(total),
                               "value_unit": "USD"}, "positions": rows}


def parse_house_index(raw: bytes, year: int) -> list[dict]:
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            member = next(name for name in archive.namelist() if name.lower() == f"{year}fd.xml")
            if archive.getinfo(member).file_size > 30_000_000:
                raise DirectSourceError("House index exceeds size limit")
            root = ET.fromstring(archive.read(member))
    except (ValueError, StopIteration, ET.ParseError, zipfile.BadZipFile) as exc:
        raise DirectSourceError("Invalid House disclosure index") from exc
    rows = []
    for item in root.findall(".//Member"):
        values = {node.tag: (node.text or "").strip() for node in item}
        if values.get("FilingType") != "P":
            continue
        document_id = values.get("DocID", "")
        if not document_id.isdigit():
            raise DirectSourceError("House PTR missing document ID")
        filed = datetime.strptime(values["FilingDate"], "%m/%d/%Y").date()
        rows.append({"key": f"{year}:{document_id}", "filing_id": document_id, "filing_date": filed.isoformat(),
                     "member_name": " ".join(filter(None, [values.get("First"), values.get("Last") ])),
                     "district": values.get("StateDst"), "form": "PTR",
                     "url": f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{document_id}.pdf"})
    if not rows:
        raise DirectSourceError("House index contains no PTR records; coverage is unverified")
    return rows


_HOUSE_TRADE = re.compile(
    r"\[(?P<asset_type>[A-Z0-9]{1,3})\]\s*(?P<type>P|S(?:\s*\((?:F|P|full|partial)\))?|E)\s+"
    r"(?P<date>\d{2}/\d{2}/\d{4})\s+(?P<notification>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<amount>(?:Over\s+)?\$[\d,]+(?:\s*-\s*\$[\d,]+)?)", re.I)


def parse_house_pdf(raw: bytes, metadata: dict) -> tuple[str, dict]:
    if not raw.startswith(b"%PDF"):
        raise DirectSourceError("Expected a House PDF")
    reader = PdfReader(io.BytesIO(raw))
    if len(reader.pages) > 150:
        raise DirectSourceError("House report exceeds page limit")
    from app.clients.house_ptr_pdf import extract_house_columns
    pages, texts = [], []
    for page in reader.pages:
        tokens = []
        def visit(value, cm, tm, font, size):
            value = ' '.join(value.replace('\x00', '').split())
            if value:
                x = tm[4] * cm[0] + tm[5] * cm[2] + cm[4]
                y = tm[4] * cm[1] + tm[5] * cm[3] + cm[5]
                tokens.append((x, y, value))
        texts.append(page.extract_text(visitor_text=visit) or '')
        pages.append(tokens)
    text = "\n".join(texts).replace("\x00", "")
    if not text.strip():
        from app.clients.house_ptr_review import reviewed_house_scan
        try:
            reviewed = reviewed_house_scan(raw, metadata, len(reader.pages))
        except ValueError as exc:
            raise DirectSourceError(str(exc)) from exc
        if reviewed is not None:
            return reviewed
        raise DirectSourceError('House report has no text layer; scanned form/OCR review required')
    if metadata.get('filing_id'):
        body_ids = set(re.findall(r'Filing ID\s*#(\d+)', text))
        if body_ids != {str(metadata['filing_id'])}:
            raise DirectSourceError('House report filing ID differs from discovery')
    if metadata.get('member_name'):
        def name_key(value):
            words = re.findall(r'\w+', value.casefold())
            if words and words[0] == 'hon':
                words.pop(0)
            if words and words[-1] in {'jr', 'sr', 'ii', 'iii', 'iv'}:
                words.pop()
            return words
        names = re.findall(r'^Name:\s*([^\n]+)', text, re.M)
        if len(names) != 1 or name_key(names[0]) != name_key(metadata['member_name']):
            raise DirectSourceError('House report member differs from discovery')
    try:
        column_rows = extract_house_columns(pages)
    except ValueError as exc:
        raise DirectSourceError(str(exc)) from exc
    markers = list(re.finditer(r"\[[A-Z0-9]{1,3}\]", text))
    matches = column_rows if column_rows is not None else list(_HOUSE_TRADE.finditer(text))
    if not matches or len(matches) != len(markers):
        raise DirectSourceError(f"House row coverage incomplete: {len(matches)}/{len(markers)}; manual/OCR review required")
    rows = []
    previous = 0
    for index, match in enumerate(matches):
        prefix = text[previous:match.start()] if column_rows is None else ''
        # The asset is immediately before its bracketed classification. Filing
        # status, notes and table headings are not part of its identity.
        lines = [line.strip() for line in prefix.splitlines() if line.strip()]
        asset_lines = []
        for line in reversed(lines):
            if ":" in line or line.startswith(("ID Owner", "Amount Cap", "$200", "Gains >", "Filing ID")):
                break
            asset_lines.insert(0, line)
            if len(asset_lines) == 3:
                break
        asset = match['asset'] if column_rows is not None else " ".join(asset_lines)
        symbols = re.findall(r"\(([A-Z][A-Z0-9.\-]{0,9})\)", asset)
        owner = re.search(r"(?:^|\s)(SP|DC|JT)(?:\s|$)", asset)
        code = match["asset_type"].upper()
        rows.append({"source_line_ref": (match['printed_id'] or str(index + 1)) if column_rows is not None else str(index + 1),
                     "source_line_ref_kind": 'printed' if column_rows is not None and match['printed_id'] else 'document_row_ordinal',
                     "owner": match['owner'] if column_rows is not None else (owner[1] if owner else "self"),
                     "symbol": symbols[-1] if symbols else None, "assetDescription": asset,
                     # Official House codebook: EF is ETF; ET is an exchange
                     # traded note. Keep the original code alongside its label.
                     "asset_type_code": code,
                     "asset_type": {"ST": "stock", "OP": "option", "MF": "mutual_fund", "EF": "etf", "ET": "etn",
                         "CS": "corporate_debt", "GS": "government_security", "HE": "private_fund",
                         "HN": "private_fund", "PS": "private_stock", "OL": "business_interest",
                         "OI": "investment_interest", "AB": "asset_backed_security", "OT": "other"}.get(code, 'unresolved'),
                     "transaction_type_raw": match['type'],
                     "transaction_type": {"P": "purchase", "E": "exchange"}.get(match["type"].upper(), "sale"),
                     "transaction_date": datetime.strptime(match["date"], "%m/%d/%Y").date().isoformat(),
                     "notification_date": datetime.strptime(match["notification"], "%m/%d/%Y").date().isoformat(),
                     "source_notes": match['source_notes'] if column_rows is not None else None,
                     "amount": match["amount"]})
        if column_rows is None:
            previous = match.end()
    raw_report = {**metadata, "document_url": metadata["url"], "transactions": rows,
                  "parser_version": "official_congress_rows_v2",
                  "amendment_flag": bool(re.search(r"\bAmend(?:ment|ed)\b", text, re.I))}
    return text, raw_report


def parse_senate_html(raw: bytes, metadata: dict) -> tuple[str, dict]:
    """Parse an accessible official PTR; consent/challenges are never bypassed."""
    root = html.fromstring(raw)
    if metadata.get('filing_date'):
        filed = re.findall(r'\bFiled\s+(\d{2}/\d{2}/\d{4})\s*@', root.text_content())
        if len(filed) != 1 or datetime.strptime(filed[0], '%m/%d/%Y').date().isoformat() != metadata['filing_date']:
            raise DirectSourceError('Senate report filing date differs from discovery')
    headings_text = [' '.join(node.text_content().split()) for node in root.xpath('//h1|//h2|//h3')]
    if metadata.get('report_title') and metadata['report_title'] not in headings_text:
        raise DirectSourceError('Senate report title differs from discovery')
    name_words = lambda text: ' '.join(re.findall(r'[a-z0-9]+', text.casefold()))
    if metadata.get('member_name') and not any(name_words(metadata['member_name']) in name_words(heading) for heading in headings_text):
        raise DirectSourceError('Senate report member differs from discovery')
    tables = root.xpath("//table[.//th[contains(., 'Transaction Date') or normalize-space(.)='Date']]")
    if len(tables) != 1:
        raise DirectSourceError("Senate PTR table unavailable; access or layout requires review")
    table = tables[0]
    headings = [" ".join(node.text_content().split()).lower() for node in table.xpath(".//thead//th")]
    aliases = {"#": "source_line_ref", "transaction date": "transaction_date", "date": "transaction_date", "owner": "owner",
               "ticker": "symbol", "asset name": "assetDescription", "asset type": "asset_type",
               "type": "transaction_type", "transaction type": "transaction_type", "amount": "amount"}
    if not {"source_line_ref", "transaction_date", "owner", "transaction_type", "amount"}.issubset({aliases.get(h) for h in headings}):
        raise DirectSourceError("Senate PTR columns incomplete")
    rows = []
    seen_lines = set()
    for tr in table.xpath(".//tbody/tr"):
        cells = [" ".join(td.text_content().split()) for td in tr.xpath("./td")]
        if len(cells) != len(headings):
            raise DirectSourceError("Senate PTR row/column mismatch")
        row = {aliases[h]: value for h, value in zip(headings, cells) if h in aliases}
        row["transaction_date"] = datetime.strptime(row["transaction_date"], "%m/%d/%Y").date().isoformat()
        line_ref = row["source_line_ref"]
        if not re.fullmatch(r"[1-9][0-9]*", line_ref) or line_ref in seen_lines:
            raise DirectSourceError("Senate PTR row identity missing or repeated")
        seen_lines.add(line_ref)
        rows.append(row)
    if not rows:
        raise DirectSourceError("Senate PTR has no parseable rows")
    text = root.text_content()
    # Ordinary reports contain a certification promising a future amendment.
    # Only the report heading or discovery metadata identifies this as amended.
    report_headings = " ".join(node.text_content() for node in root.xpath("//h1|//h2|//h3")
                               if "periodic transaction report" in node.text_content().lower())
    return text, {**metadata, "document_url": metadata["url"], "transactions": rows,
                  "parser_version": "official_congress_rows_v2",
                  "amendment_flag": bool(metadata.get("amendment_flag") or
                                         re.search(r"\bamend(?:ed|ment)\b", report_headings, re.I))}


def parse_issuer_material(raw: bytes, metadata: dict) -> tuple[str, dict]:
    root = html.fromstring(raw)
    for node in root.xpath("//script|//style|//nav|//footer|//header"):
        node.drop_tree()
    main = root.xpath("//main|//*[@role='main']")
    text = " ".join((main[0] if main else root).text_content().split())
    kind = metadata["document_type"]
    if kind not in {"earnings_transcript", "earnings_release", "earnings_presentation"}:
        raise DirectSourceError("Unknown earnings material type")
    if len(text) < 1000 or not re.search(r"earnings|financial results", text, re.I):
        raise DirectSourceError("Issuer page does not contain earnings material")
    if kind == "earnings_transcript" and not re.search(r"transcript|prepared remarks", text, re.I):
        raise DirectSourceError("Issuer page does not identify transcript text")
    if not re.search(metadata["period_pattern"], text, re.I):
        raise DirectSourceError("Issuer fiscal period did not match reviewed configuration")
    return text, {**metadata, "text": text, "has_qa": bool(re.search(r"question.{0,5}answer|Q&A", text, re.I)),
                  "canonical_key": f"{metadata['symbol']}:{kind}:{metadata['fiscal_year']}:Q{metadata['fiscal_quarter']}"}
