from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Iterable

from sqlalchemy import and_, case, func, literal, or_, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.models import (
    CikMeta,
    CongressMemberAlias,
    InsiderTransaction,
    InsiderTransactionNormalized,
    InstitutionalHolder,
    InstitutionalTransaction,
    Member,
    SearchEntity,
    SearchEntityTerm,
    SearchQueryLog,
    Security,
    TickerMeta,
)
from app.services.government_departments import DEPARTMENT_ALIASES, list_departments
from app.services.ticker_identity import safe_company_identity_candidate
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

_WORD_RE = re.compile(r"[a-z0-9]+")
_ENTITY_SUFFIX_WORDS = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "llc",
    "llp",
    "lp",
    "ltd",
    "limited",
    "plc",
}
_KIND_FROM_ENTITY_TYPE = {
    "stock": "ticker",
    "member": "member",
    "insider": "insider",
    "institution": "institution",
    "department": "agency",
}
_ENTITY_TYPE_ORDER = {"stock": 0, "institution": 1, "member": 2, "insider": 3, "department": 4}
_TERM_TYPE_WEIGHT = {
    "ticker": 12000.0,
    "display": 10000.0,
    "alias": 9200.0,
    "canonical": 8800.0,
    "company": 6500.0,
    "keyword": 3000.0,
    "subtitle": 900.0,
}
_NICKNAMES = {
    "timothy": ("tim",),
    "jennifer": ("jen",),
    "michael": ("mike",),
    "william": ("bill", "will"),
    "robert": ("bob", "rob"),
    "richard": ("rick", "dick"),
    "james": ("jim",),
    "johnathan": ("john",),
    "jonathan": ("jon",),
}
_EXPLICIT_PERSON_ALIASES = {
    "cook timothy d": ("Tim Cook", "Timothy Cook", "Timothy D Cook", "Timothy D. Cook", "Cook Timothy D", "Cook, Timothy D."),
    "timothy d cook": ("Tim Cook", "Timothy Cook", "Timothy D Cook", "Timothy D. Cook", "Cook Timothy D", "Cook, Timothy D."),
    "huang jen hsun": ("Jensen Huang", "Jen-Hsun Huang", "Jen Hsun Huang", "Huang Jen Hsun"),
    "jen hsun huang": ("Jensen Huang", "Jen-Hsun Huang", "Jen Hsun Huang", "Huang Jen Hsun"),
}
_EXPLICIT_COMPANY_ALIASES = {
    "AAPL": ("Apple",),
    "BRK-B": ("BRK", "Berkshire", "Berkshire Hathaway"),
    "GOOG": ("Google", "Alphabet", "Alphabet Inc.", "GOOGL"),
    "GOOGL": ("Google", "Alphabet", "Alphabet Inc.", "GOOG"),
    "META": ("Meta", "Facebook", "Meta Platforms"),
    "NVDA": ("NVIDIA", "Nvidia"),
}
_STATIC_COMPANY_META = {
    "AAPL": ("Apple Inc.", "NASDAQ"),
    "LMT": ("Lockheed Martin", None),
    "NOW": ("ServiceNow Inc.", None),
    "NVDA": ("NVIDIA Corporation", "NASDAQ"),
    "PLTR": ("Palantir Technologies Inc.", None),
    "TSLA": ("Tesla Inc.", "NASDAQ"),
}


@dataclass(frozen=True)
class SearchBuildStats:
    indexed_by_type: dict[str, int]
    total_indexed: int


def normalize_search_text(value: str | None) -> str:
    return " ".join(_WORD_RE.findall((value or "").casefold().replace("&", " and ")))


def compact_search_text(value: str | None) -> str:
    return "".join(_WORD_RE.findall((value or "").casefold().replace("&", " and ")))


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _json_list(value: str | None) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, ValueError):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()] if isinstance(parsed, list) else []


def _unique_strings(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = _clean(value)
        if not cleaned:
            continue
        key = normalize_search_text(cleaned)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
    return result


def _title_name_part(part: str) -> str:
    if len(part) == 1:
        return part.upper()
    return part[:1].upper() + part[1:].lower()


def _title_person_name(value: str | None) -> str | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    chunks = []
    for word in re.split(r"(\s+|-)", cleaned.strip()):
        if word.isspace() or word == "-":
            chunks.append(word)
        else:
            chunks.append(_title_name_part(word) if word.isupper() else word)
    return "".join(chunks).strip() or None


def _person_name_variants(source_name: str | None) -> list[str]:
    cleaned = _clean(source_name)
    if not cleaned:
        return []

    variants: list[str] = [cleaned]
    normalized_key = normalize_search_text(cleaned)
    variants.extend(_EXPLICIT_PERSON_ALIASES.get(normalized_key, ()))

    comma_reordered: str | None = None
    if "," in cleaned:
        last, rest = cleaned.split(",", 1)
        comma_reordered = f"{rest.strip()} {last.strip()}".strip()
        variants.append(comma_reordered)

    parts = [part for part in re.split(r"\s+", comma_reordered or cleaned.replace(",", " ")) if part]
    if cleaned.isupper() and 2 <= len(parts) <= 4:
        variants.append(" ".join([*parts[1:], parts[0]]))
        variants.append(f"{parts[0]}, {' '.join(parts[1:])}")
    elif len(parts) >= 2:
        variants.append(" ".join(parts))
        variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
        variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")

    titled = [_title_person_name(item) for item in variants]
    variants.extend(item for item in titled if item)

    for item in list(variants):
        words = item.replace(",", " ").split()
        if len(words) >= 3:
            first, middle, last = words[0], words[1], words[-1]
            variants.append(f"{first} {last}")
            variants.append(f"{first} {middle[:1]}. {last}")
            variants.append(f"{first} {middle[:1]} {last}")
        first_key = normalize_search_text(words[0] if words else "")
        for nickname in _NICKNAMES.get(first_key, ()):
            if len(words) >= 2:
                variants.append(f"{nickname.title()} {words[-1]}")

    return _unique_strings(variants)


def _display_person_name(source_name: str | None) -> str | None:
    aliases = _person_name_variants(source_name)
    normalized_key = normalize_search_text(source_name)
    explicit = _EXPLICIT_PERSON_ALIASES.get(normalized_key)
    if explicit:
        return explicit[0]
    if aliases:
        for alias in aliases:
            if "," not in alias and not alias.isupper():
                return alias
        return aliases[0]
    return None


def _member_href(member_name: str, bioguide_id: str) -> str:
    slug = member_name.strip().upper().replace(".", "").replace(",", "").replace("'", "").replace("-", " ")
    slug = "_".join(part for part in slug.split() if part)
    return f"/member/{slug or bioguide_id}"


def _insider_slug(name: str, reporting_cik: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in name.strip())
    slug = "-".join(part for part in slug.split("-") if part)
    return f"{slug}-{reporting_cik}" if slug else reporting_cik


def _legacy_payload_dict(payload_json: object) -> dict[str, Any]:
    try:
        parsed = json.loads(str(payload_json or "{}"))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _legacy_payload_role(row_role: object, payload: dict[str, Any]) -> str | None:
    role_text = _clean(row_role) or _clean(payload.get("typeOfOwner")) or _clean(payload.get("role"))
    if not role_text:
        return None
    officer_match = re.search(r"officer\s*:\s*([^,;]+)", role_text, flags=re.IGNORECASE)
    if officer_match:
        officer_title = officer_match.group(1).strip()
        if officer_title:
            return officer_title
    if re.search(r"\bdirector\b", role_text, flags=re.IGNORECASE):
        return "Director"
    return role_text


def _role_keywords(role: str | None) -> list[str]:
    normalized = normalize_search_text(role)
    keywords = [role] if role else []
    if "chief executive officer" in normalized or "ceo" in normalized.split():
        keywords.append("CEO")
    if "chief financial officer" in normalized:
        keywords.append("CFO")
    if "chief operating officer" in normalized:
        keywords.append("COO")
    if "chief technology officer" in normalized:
        keywords.append("CTO")
    if "president" in normalized:
        keywords.append("President")
    if "director" in normalized:
        keywords.append("Director")
    return _unique_strings(keywords)


def _insider_popularity_score(role: str | None) -> float:
    normalized = normalize_search_text(role)
    if "chief executive officer" in normalized or "ceo" in normalized.split() or "president" in normalized:
        return 120.0
    if "chief" in normalized or "officer" in normalized:
        return 105.0
    if "director" in normalized:
        return 65.0
    return 55.0 if role else 35.0


def _search_text(*values: Any, aliases: Iterable[str] = (), keywords: Iterable[str] = ()) -> tuple[str, str, str]:
    text_value = " ".join(_unique_strings([*values, *aliases, *keywords]))
    return text_value, normalize_search_text(text_value), compact_search_text(text_value)


def _entity(
    *,
    entity_id: str,
    entity_type: str,
    display_name: str,
    canonical_url: str,
    canonical_name: str | None = None,
    source_table: str | None = None,
    source_id: str | None = None,
    ticker: str | None = None,
    company_name: str | None = None,
    aliases: Iterable[str] = (),
    keywords: Iterable[str] = (),
    subtitle: str | None = None,
    popularity_score: float = 0.0,
) -> SearchEntity:
    aliases_list = _unique_strings(aliases)
    keywords_list = _unique_strings(keywords)
    search_text, normalized, compact = _search_text(
        display_name,
        canonical_name,
        ticker,
        company_name,
        subtitle,
        aliases=aliases_list,
        keywords=keywords_list,
    )
    return SearchEntity(
        entity_id=entity_id,
        entity_type=entity_type,
        source_table=source_table,
        source_id=source_id,
        display_name=display_name,
        canonical_name=canonical_name,
        ticker=ticker,
        company_name=company_name,
        aliases_json=json.dumps(aliases_list),
        keywords_json=json.dumps(keywords_list),
        subtitle=subtitle,
        canonical_url=canonical_url,
        popularity_score=float(popularity_score or 0.0),
        search_text=search_text,
        normalized_search_text=normalized,
        compact_search_text=compact,
        updated_at=datetime.now(timezone.utc),
    )


def _term(entity: SearchEntity, term_type: str, term_text: Any, rank_weight: float) -> SearchEntityTerm | None:
    cleaned = _clean(term_text)
    normalized = normalize_search_text(cleaned)
    compact = compact_search_text(cleaned)
    if not cleaned or not normalized or not compact:
        return None
    return SearchEntityTerm(
        entity_id=entity.entity_id,
        entity_type=entity.entity_type,
        term_type=term_type,
        term_text=cleaned,
        normalized_term=normalized,
        compact_term=compact,
        rank_weight=float(rank_weight or 0.0),
        updated_at=datetime.now(timezone.utc),
    )


def _entity_terms(entity: SearchEntity) -> list[SearchEntityTerm]:
    raw_terms: list[tuple[str, Any, float]] = [
        ("display", entity.display_name, _TERM_TYPE_WEIGHT["display"]),
        ("canonical", entity.canonical_name, _TERM_TYPE_WEIGHT["canonical"]),
        ("ticker", entity.ticker, _TERM_TYPE_WEIGHT["ticker"]),
        ("company", entity.company_name, _TERM_TYPE_WEIGHT["company"]),
        ("subtitle", entity.subtitle, _TERM_TYPE_WEIGHT["subtitle"]),
    ]
    raw_terms.extend(("alias", alias, _TERM_TYPE_WEIGHT["alias"]) for alias in _json_list(entity.aliases_json))
    raw_terms.extend(("keyword", keyword, _TERM_TYPE_WEIGHT["keyword"]) for keyword in _json_list(entity.keywords_json))
    terms: list[SearchEntityTerm] = []
    seen: set[tuple[str, str, str]] = set()
    for term_type, term_text, weight in raw_terms:
        term = _term(entity, term_type, term_text, weight)
        if term is None:
            continue
        key = (term.term_type, term.normalized_term, term.compact_term)
        if key in seen:
            continue
        seen.add(key)
        terms.append(term)
    return terms


def _company_name_maps(db: Session) -> tuple[dict[str, str], dict[str, str | None]]:
    names: dict[str, str] = {}
    exchanges: dict[str, str | None] = {}
    for row in db.execute(select(TickerMeta.symbol, TickerMeta.company_name, TickerMeta.exchange)).all():
        symbol = normalize_symbol(row.symbol)
        if not symbol:
            continue
        label = safe_company_identity_candidate(_clean(row.company_name), symbol)
        if label:
            names[symbol] = label
        exchanges[symbol] = _clean(row.exchange)
    for row in db.execute(select(Security.symbol, Security.name)).all():
        symbol = normalize_symbol(row.symbol)
        if not symbol or symbol in names:
            continue
        label = safe_company_identity_candidate(_clean(row.name), symbol)
        if label:
            names[symbol] = label
    for symbol, (label, exchange) in _STATIC_COMPANY_META.items():
        names.setdefault(symbol, label)
        exchanges.setdefault(symbol, exchange)
    return names, exchanges


def _stock_entities(db: Session) -> list[SearchEntity]:
    names, exchanges = _company_name_maps(db)
    entities: list[SearchEntity] = []
    for symbol in sorted(names):
        label = names[symbol]
        aliases = [symbol, label, *_EXPLICIT_COMPANY_ALIASES.get(symbol, ())]
        exchange = exchanges.get(symbol)
        subtitle = " - ".join(part for part in ["Ticker", label, exchange] if part)
        entities.append(
            _entity(
                entity_id=f"stock:{symbol}",
                entity_type="stock",
                source_table="ticker_meta",
                source_id=symbol,
                display_name=label,
                canonical_name=label,
                ticker=symbol,
                company_name=label,
                aliases=aliases,
                subtitle=subtitle,
                canonical_url=f"/ticker/{symbol}",
                popularity_score=200.0 if symbol in _STATIC_COMPANY_META else 25.0,
            )
        )
    return entities


def _member_entities(db: Session) -> list[SearchEntity]:
    alias_rows = {
        row.alias_member_id: row
        for row in db.execute(select(CongressMemberAlias)).scalars().all()
    }
    entities: list[SearchEntity] = []
    for row in db.execute(select(Member)).scalars().all():
        bioguide_id = _clean(row.bioguide_id)
        name = _clean(f"{row.first_name or ''} {row.last_name or ''}")
        if not bioguide_id or not name:
            continue
        alias_row = alias_rows.get(bioguide_id)
        aliases = [name, row.last_name, f"{row.last_name or ''} {row.first_name or ''}", f"{row.last_name or ''}, {row.first_name or ''}"]
        if alias_row is not None:
            aliases.extend([alias_row.member_name, alias_row.member_slug, alias_row.authoritative_member_id])
        subtitle = " - ".join(part for part in ["Member", _clean(row.chamber), _clean(row.party), _clean(row.state)] if part)
        entities.append(
            _entity(
                entity_id=f"member:{bioguide_id}",
                entity_type="member",
                source_table="members",
                source_id=bioguide_id,
                display_name=name,
                canonical_name=name,
                aliases=aliases,
                keywords=[row.chamber, row.party, row.state, "congress"],
                subtitle=subtitle,
                canonical_url=_member_href(name, bioguide_id),
                popularity_score=80.0,
            )
        )
    return entities


def _insider_entities(db: Session, company_names: dict[str, str]) -> list[SearchEntity]:
    by_key: dict[str, SearchEntity] = {}
    normalized_rows = db.execute(
        select(
            InsiderTransactionNormalized.reporting_owner_cik,
            InsiderTransactionNormalized.reporting_owner_name,
            InsiderTransactionNormalized.ticker_normalized,
            InsiderTransactionNormalized.issuer_name,
            InsiderTransactionNormalized.officer_title,
            InsiderTransactionNormalized.is_director,
            func.max(func.coalesce(InsiderTransactionNormalized.filing_date, InsiderTransactionNormalized.transaction_date)).label("latest_date"),
        )
        .where(InsiderTransactionNormalized.reporting_owner_name.is_not(None))
        .where(InsiderTransactionNormalized.is_duplicate.is_(False))
        .group_by(
            InsiderTransactionNormalized.reporting_owner_cik,
            InsiderTransactionNormalized.reporting_owner_name,
            InsiderTransactionNormalized.ticker_normalized,
            InsiderTransactionNormalized.issuer_name,
            InsiderTransactionNormalized.officer_title,
            InsiderTransactionNormalized.is_director,
        )
    ).all()
    legacy_rows = db.execute(
        select(
            InsiderTransaction.reporting_cik,
            InsiderTransaction.insider_name,
            InsiderTransaction.symbol,
            InsiderTransaction.role,
            InsiderTransaction.payload_json,
            func.max(func.coalesce(InsiderTransaction.filing_date, InsiderTransaction.transaction_date)).label("latest_date"),
        )
        .group_by(
            InsiderTransaction.reporting_cik,
            InsiderTransaction.insider_name,
            InsiderTransaction.symbol,
            InsiderTransaction.role,
            InsiderTransaction.payload_json,
        )
    ).all()

    for row in normalized_rows:
        reporting_cik = _clean(row.reporting_owner_cik)
        source_name = _clean(row.reporting_owner_name)
        display_name = _display_person_name(source_name)
        if not reporting_cik or not display_name:
            continue
        symbol = normalize_symbol(row.ticker_normalized)
        issuer_name = safe_company_identity_candidate(_clean(row.issuer_name), symbol) or (company_names.get(symbol or "") if symbol else None)
        role = _clean(row.officer_title) or ("Director" if row.is_director else None)
        aliases = _person_name_variants(source_name)
        keywords = [issuer_name, symbol, *_role_keywords(role), "insider", "executive"]
        subtitle = " - ".join(part for part in ["Insider", issuer_name, symbol, role] if part)
        issuer_key = symbol or "unknown"
        href = f"/insider/{_insider_slug(display_name, reporting_cik)}"
        if symbol:
            href = f"{href}?issuer={symbol}"
        entity = _entity(
            entity_id=f"insider:{reporting_cik}:{issuer_key}",
            entity_type="insider",
            source_table="insider_transactions_normalized",
            source_id=f"{reporting_cik}:{issuer_key}",
            display_name=display_name,
            canonical_name=source_name,
            ticker=symbol,
            company_name=issuer_name,
            aliases=aliases,
            keywords=keywords,
            subtitle=subtitle,
            canonical_url=href,
            popularity_score=_insider_popularity_score(role),
        )
        by_key[entity.entity_id] = entity

    for row in legacy_rows:
        payload = _legacy_payload_dict(getattr(row, "payload_json", None))
        reporting_cik = _clean(row.reporting_cik) or _clean(payload.get("reportingCik"))
        source_name = _clean(row.insider_name) or _clean(payload.get("reportingName")) or _clean(payload.get("reporting_owner_name")) or _clean(payload.get("ownerName"))
        display_name = _display_person_name(source_name)
        if not reporting_cik or not display_name:
            continue
        symbol = normalize_symbol(row.symbol) or normalize_symbol(payload.get("symbol"))
        issuer_name = company_names.get(symbol or "") if symbol else None
        role = _legacy_payload_role(row.role, payload)
        issuer_key = symbol or "unknown"
        entity_id = f"insider:{reporting_cik}:{issuer_key}"
        if entity_id in by_key:
            continue
        href = f"/insider/{_insider_slug(display_name, reporting_cik)}"
        if symbol:
            href = f"{href}?issuer={symbol}"
        subtitle = " - ".join(part for part in ["Insider", issuer_name, symbol, role] if part)
        by_key[entity_id] = _entity(
            entity_id=entity_id,
            entity_type="insider",
            source_table="insider_transactions",
            source_id=f"{reporting_cik}:{issuer_key}",
            display_name=display_name,
            canonical_name=source_name,
            ticker=symbol,
            company_name=issuer_name,
            aliases=_person_name_variants(source_name),
            keywords=[issuer_name, symbol, *_role_keywords(role), "insider", "executive"],
            subtitle=subtitle,
            canonical_url=href,
            popularity_score=_insider_popularity_score(role),
        )
    return list(by_key.values())


def _institution_entities(db: Session) -> list[SearchEntity]:
    cik_names = {
        row.cik: _clean(row.company_name)
        for row in db.execute(select(CikMeta.cik, CikMeta.company_name)).all()
        if _clean(row.cik)
    }
    latest_transaction_names: dict[str, str] = {}
    transaction_rows = db.execute(
        select(
            InstitutionalTransaction.institution_cik,
            InstitutionalTransaction.institution_name,
            func.max(InstitutionalTransaction.filing_date).label("latest_filing_date"),
        )
        .where(InstitutionalTransaction.institution_cik.is_not(None))
        .group_by(InstitutionalTransaction.institution_cik, InstitutionalTransaction.institution_name)
        .order_by(func.max(InstitutionalTransaction.filing_date).desc())
    ).all()
    for row in transaction_rows:
        cik = _clean(row.institution_cik)
        name = _clean(row.institution_name)
        if cik and name and cik not in latest_transaction_names:
            latest_transaction_names[cik] = name

    entities: list[SearchEntity] = []
    for row in db.execute(select(InstitutionalHolder)).scalars().all():
        cik = _clean(row.cik)
        if not cik:
            continue
        label = _clean(row.holder_name) or cik_names.get(cik) or latest_transaction_names.get(cik) or f"Institution {cik}"
        aliases = [label, row.normalized_holder_name, cik_names.get(cik), latest_transaction_names.get(cik), cik]
        subtitle = " - ".join(part for part in ["Institution", _clean(row.holder_type), "13F profile"] if part)
        entities.append(
            _entity(
                entity_id=f"institution:{cik}",
                entity_type="institution",
                source_table="institutional_holders",
                source_id=cik,
                display_name=label,
                canonical_name=label,
                aliases=aliases,
                keywords=[row.holder_type, "13F", "institution", "fund"],
                subtitle=subtitle,
                canonical_url=f"/institution/{cik}",
                popularity_score=float(row.quality_score or 0.0),
            )
        )
    return entities


def _department_entities(db: Session) -> list[SearchEntity]:
    entities: list[SearchEntity] = []
    for item in list_departments(db).get("items", []):
        slug = _clean(item.get("slug"))
        name = _clean(item.get("name"))
        if not slug or not name:
            continue
        aliases = [name, *(item.get("aliases") or []), *DEPARTMENT_ALIASES.get(name, ())]
        entities.append(
            _entity(
                entity_id=f"department:{slug}",
                entity_type="department",
                source_table="government_departments",
                source_id=slug,
                display_name=name,
                canonical_name=name,
                aliases=aliases,
                keywords=["government", "department", "agency", "contracts"],
                subtitle="Government department - contract awards",
                canonical_url=f"/departments/{slug}",
                popularity_score=float(item.get("totalAwarded") or 0.0) / 1_000_000_000.0,
            )
        )
    return entities


def rebuild_search_entities(db: Session) -> SearchBuildStats:
    company_names, _ = _company_name_maps(db)
    entities = [
        *_stock_entities(db),
        *_member_entities(db),
        *_insider_entities(db, company_names),
        *_institution_entities(db),
        *_department_entities(db),
    ]
    terms: list[SearchEntityTerm] = []
    for entity in entities:
        terms.extend(_entity_terms(entity))
    db.execute(SearchEntityTerm.__table__.delete())
    db.execute(SearchEntity.__table__.delete())
    for start in range(0, len(entities), 500):
        db.add_all(entities[start : start + 500])
        db.flush()
    for start in range(0, len(terms), 1000):
        db.add_all(terms[start : start + 1000])
        db.flush()
    indexed_by_type: dict[str, int] = {}
    for entity in entities:
        indexed_by_type[entity.entity_type] = indexed_by_type.get(entity.entity_type, 0) + 1
    return SearchBuildStats(indexed_by_type=indexed_by_type, total_indexed=len(entities))


def search_entity_count(db: Session) -> int:
    try:
        return int(db.execute(select(func.count()).select_from(SearchEntity)).scalar_one() or 0)
    except SQLAlchemyError:
        return 0


def _bounded_edit_distance(left: str, right: str, max_distance: int) -> int:
    if abs(len(left) - len(right)) > max_distance:
        return max_distance + 1
    previous = list(range(len(right) + 1))
    for i, left_ch in enumerate(left, start=1):
        current = [i]
        row_min = current[0]
        for j, right_ch in enumerate(right, start=1):
            cost = 0 if left_ch == right_ch else 1
            value = min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost)
            current.append(value)
            row_min = min(row_min, value)
        if row_min > max_distance:
            return max_distance + 1
        previous = current
    return previous[-1]


def _base_company_key(value: str | None) -> str:
    words = _WORD_RE.findall((value or "").casefold().replace("&", " and "))
    while len(words) > 1 and words[-1] in _ENTITY_SUFFIX_WORDS:
        words.pop()
    return " ".join(words)


def _field_score(query: str, value: str | None, *, exact: float, prefix: float, contains: float, fuzzy: float = 0.0) -> float:
    q_norm = normalize_search_text(query)
    v_norm = normalize_search_text(value)
    if not q_norm or not v_norm:
        return 0.0
    q_compact = compact_search_text(query)
    v_compact = compact_search_text(value)
    if q_norm == v_norm or q_compact == v_compact:
        return exact
    if v_norm.startswith(q_norm) or v_compact.startswith(q_compact):
        return prefix
    if len(q_norm) >= 2 and (q_norm in v_norm or q_compact in v_compact):
        return contains
    if fuzzy and len(q_compact) >= 3:
        budget = 1 if len(q_compact) < 6 else 2
        candidates = [word for word in _WORD_RE.findall(v_norm) if word]
        candidates.append(v_compact[: max(len(q_compact), min(len(v_compact), len(q_compact) + 2))])
        if min((_bounded_edit_distance(q_compact, candidate, budget) for candidate in candidates), default=budget + 1) <= budget:
            return fuzzy
    return 0.0


def _entity_score(entity: SearchEntity, query: str) -> float:
    q_norm = normalize_search_text(query)
    q_compact = compact_search_text(query)
    aliases = _json_list(entity.aliases_json)
    keywords = _json_list(entity.keywords_json)
    score = 0.0
    score = max(score, _field_score(query, entity.ticker, exact=12000.0, prefix=8500.0, contains=5200.0, fuzzy=7000.0))
    score = max(score, _field_score(query, entity.display_name, exact=10000.0, prefix=7800.0, contains=4200.0, fuzzy=6000.0))
    score = max(score, _field_score(query, entity.canonical_name, exact=8800.0, prefix=7200.0, contains=3800.0, fuzzy=5200.0))
    for alias in aliases:
        score = max(score, _field_score(query, alias, exact=9200.0, prefix=7400.0, contains=3900.0, fuzzy=5600.0))
    company_score = max(
        _field_score(query, entity.company_name, exact=6500.0, prefix=5200.0, contains=2800.0, fuzzy=3600.0),
        _field_score(query, f"{entity.company_name or ''} {entity.ticker or ''}", exact=6600.0, prefix=5300.0, contains=3000.0, fuzzy=3600.0),
    )
    score = max(score, company_score)
    for keyword in keywords:
        score = max(score, _field_score(query, keyword, exact=3000.0, prefix=2200.0, contains=900.0, fuzzy=0.0))
    if len(q_norm.split()) >= 2 and q_norm in normalize_search_text(entity.search_text):
        score = max(score, 5000.0)
    tokens = [token for token in q_norm.split() if len(token) >= 2]
    if len(tokens) >= 2 and all(token in entity.normalized_search_text for token in tokens):
        score = max(score, 6200.0)
    if q_compact and q_compact in entity.compact_search_text and len(q_compact) >= 3:
        score = max(score, 3600.0)
    if score > 0:
        score += min(float(entity.popularity_score or 0.0), 200.0) / 20.0
    return score


def _candidate_clause(query: str) -> Any:
    q_norm = normalize_search_text(query)
    q_compact = compact_search_text(query)
    clauses = []
    if q_norm:
        contains = f"%{q_norm}%"
        prefix = f"{q_norm}%"
        clauses.extend(
            [
                func.lower(SearchEntity.ticker) == q_norm.upper().lower(),
                func.lower(SearchEntity.display_name).like(prefix),
                func.lower(SearchEntity.canonical_name).like(prefix),
                SearchEntity.normalized_search_text.like(contains),
            ]
        )
    if q_compact:
        clauses.append(SearchEntity.compact_search_text.like(f"%{q_compact}%"))
        if len(q_compact) >= 2:
            clauses.append(func.lower(SearchEntity.ticker).like(f"{q_compact[:2]}%"))
    for token in q_norm.split()[:4]:
        if len(token) >= 3:
            clauses.append(SearchEntity.normalized_search_text.like(f"%{token}%"))
    return or_(*clauses) if clauses else literal(False)


def _high_confidence_candidate_clause(query: str) -> Any:
    q_norm = normalize_search_text(query)
    q_compact = compact_search_text(query)
    clauses = []
    if q_norm:
        clauses.extend(
            [
                func.lower(SearchEntity.ticker) == q_norm,
                func.lower(SearchEntity.display_name) == q_norm,
                func.lower(SearchEntity.canonical_name) == q_norm,
                SearchEntity.normalized_search_text.like(f"%{q_norm}%"),
            ]
        )
    if q_compact:
        clauses.append(SearchEntity.compact_search_text.like(f"%{q_compact}%"))
    return or_(*clauses) if clauses else literal(False)


def _candidate_order(query: str) -> Any:
    q_norm = normalize_search_text(query)
    q_compact = compact_search_text(query)
    first_token = q_norm.split()[0] if q_norm.split() else ""
    token_clauses = [
        SearchEntity.normalized_search_text.like(f"%{token}%")
        for token in q_norm.split()[:4]
        if len(token) >= 3
    ]
    return case(
        (func.lower(SearchEntity.ticker) == q_norm, 0),
        (SearchEntity.compact_search_text.like(f"%{q_compact}%"), 1),
        (SearchEntity.normalized_search_text.like(f"%{q_norm}%"), 2),
        (func.lower(SearchEntity.display_name).like(f"{first_token}%"), 3) if first_token else (literal(False), 3),
        (and_(*token_clauses), 3) if token_clauses else (literal(False), 3),
        else_=4,
    )


def _term_candidate_clause(query: str, *, high_confidence: bool) -> Any:
    q_norm = normalize_search_text(query)
    q_compact = compact_search_text(query)
    clauses = []
    if q_norm:
        clauses.extend(
            [
                SearchEntityTerm.normalized_term == q_norm,
                SearchEntityTerm.normalized_term.like(f"{q_norm}%"),
            ]
        )
        if not high_confidence:
            clauses.append(SearchEntityTerm.normalized_term.like(f"%{q_norm}%"))
    if q_compact:
        clauses.extend(
            [
                SearchEntityTerm.compact_term == q_compact,
                SearchEntityTerm.compact_term.like(f"{q_compact}%"),
            ]
        )
        if not high_confidence:
            clauses.append(SearchEntityTerm.compact_term.like(f"%{q_compact}%"))
    if not high_confidence:
        for token in q_norm.split()[:4]:
            if len(token) >= 3:
                clauses.extend(
                    [
                        SearchEntityTerm.normalized_term == token,
                        SearchEntityTerm.normalized_term.like(f"{token}%"),
                        SearchEntityTerm.normalized_term.like(f"%{token}%"),
                    ]
                )
    return or_(*clauses) if clauses else literal(False)


def _term_score_expr(query: str) -> Any:
    q_norm = normalize_search_text(query)
    q_compact = compact_search_text(query)
    first_token = q_norm.split()[0] if q_norm.split() else ""
    token_exact_clauses = [
        SearchEntityTerm.normalized_term == token
        for token in q_norm.split()[:4]
        if len(token) >= 3
    ]
    return case(
        (SearchEntityTerm.normalized_term == q_norm, SearchEntityTerm.rank_weight + 5000.0),
        (SearchEntityTerm.compact_term == q_compact, SearchEntityTerm.rank_weight + 5000.0),
        (SearchEntityTerm.normalized_term.like(f"{q_norm}%"), SearchEntityTerm.rank_weight + 3000.0) if q_norm else (literal(False), 0.0),
        (SearchEntityTerm.compact_term.like(f"{q_compact}%"), SearchEntityTerm.rank_weight + 3000.0) if q_compact else (literal(False), 0.0),
        (SearchEntityTerm.normalized_term.like(f"{first_token}%"), SearchEntityTerm.rank_weight + 1200.0) if first_token else (literal(False), 0.0),
        (and_(*token_exact_clauses), SearchEntityTerm.rank_weight + 1000.0) if token_exact_clauses else (literal(False), 0.0),
        (SearchEntityTerm.normalized_term.like(f"%{q_norm}%"), SearchEntityTerm.rank_weight + 700.0) if q_norm else (literal(False), 0.0),
        (SearchEntityTerm.compact_term.like(f"%{q_compact}%"), SearchEntityTerm.rank_weight + 700.0) if q_compact else (literal(False), 0.0),
        else_=SearchEntityTerm.rank_weight,
    )


def _term_candidate_entity_ids(db: Session, query: str, *, high_confidence: bool, limit: int) -> list[str]:
    score_expr = func.max(_term_score_expr(query)).label("term_score")
    rows = db.execute(
        select(SearchEntityTerm.entity_id, score_expr)
        .where(_term_candidate_clause(query, high_confidence=high_confidence))
        .group_by(SearchEntityTerm.entity_id)
        .order_by(score_expr.desc(), SearchEntityTerm.entity_id.asc())
        .limit(limit)
    ).all()
    return [str(row.entity_id) for row in rows if row.entity_id]


def _load_entities_by_entity_id(db: Session, entity_ids: list[str]) -> list[SearchEntity]:
    if not entity_ids:
        return []
    order = {entity_id: index for index, entity_id in enumerate(entity_ids)}
    entities = db.execute(select(SearchEntity).where(SearchEntity.entity_id.in_(entity_ids))).scalars().all()
    entities.sort(key=lambda entity: order.get(entity.entity_id, len(order)))
    return entities


def entity_to_suggest_item(entity: SearchEntity) -> dict[str, str | int | float | None]:
    kind = _KIND_FROM_ENTITY_TYPE.get(entity.entity_type, entity.entity_type)
    return {
        "kind": kind,
        "id": entity.source_id or entity.entity_id,
        "symbol": entity.ticker,
        "label": entity.display_name,
        "subtitle": entity.subtitle,
        "href": entity.canonical_url,
    }


def _rank_search_results(rows: list[SearchEntity], query: str, bounded_limit: int) -> list[dict[str, str | int | float | None]]:
    scored: list[tuple[float, SearchEntity]] = []
    for entity in rows:
        score = _entity_score(entity, query)
        if score > 0:
            scored.append((score, entity))
    scored.sort(
        key=lambda item: (
            -item[0],
            _ENTITY_TYPE_ORDER.get(item[1].entity_type, 99),
            -(float(item[1].popularity_score or 0.0)),
            item[1].display_name,
        )
    )
    results: list[dict[str, str | int | float | None]] = []
    seen_result_keys: set[str] = set()
    for _, entity in scored:
        result_key = entity.canonical_url or f"{entity.entity_type}:{entity.source_id or entity.entity_id}"
        if result_key in seen_result_keys:
            continue
        seen_result_keys.add(result_key)
        results.append(entity_to_suggest_item(entity))
        if len(results) >= bounded_limit:
            break
    return results


class PostgresSearchProvider:
    def _legacy_search(self, db: Session, query: str, *, limit: int = 8) -> list[dict[str, str | int | float | None]]:
        bounded_limit = max(1, min(int(limit or 8), 20))
        rows = []
        seen_ids: set[str] = set()
        for pass_index, (clause, candidate_limit) in enumerate((
            (_high_confidence_candidate_clause(query), max(bounded_limit * 25, 120)),
            (_candidate_clause(query), max(bounded_limit * 120, 600)),
        )):
            for row in db.execute(
                select(SearchEntity)
                .where(clause)
                .order_by(_candidate_order(query), SearchEntity.entity_type.asc(), SearchEntity.display_name.asc())
                .limit(candidate_limit)
            ).scalars():
                if row.entity_id in seen_ids:
                    continue
                seen_ids.add(row.entity_id)
                rows.append(row)
            high_scores = [_entity_score(entity, query) for entity in rows]
            if high_scores and max(high_scores) >= 7000 and (pass_index == 0 or len(rows) >= bounded_limit):
                break
        return _rank_search_results(rows, query, bounded_limit)

    def _term_search(self, db: Session, query: str, *, limit: int = 8) -> list[dict[str, str | int | float | None]]:
        bounded_limit = max(1, min(int(limit or 8), 20))
        seen_ids: set[str] = set()
        rows: list[SearchEntity] = []
        for pass_index, (high_confidence, candidate_limit) in enumerate((
            (True, max(bounded_limit * 20, 80)),
            (False, max(bounded_limit * 80, 400)),
        )):
            entity_ids = _term_candidate_entity_ids(db, query, high_confidence=high_confidence, limit=candidate_limit)
            next_ids = [entity_id for entity_id in entity_ids if entity_id not in seen_ids]
            for entity_id in next_ids:
                seen_ids.add(entity_id)
            rows.extend(_load_entities_by_entity_id(db, next_ids))
            high_scores = [_entity_score(entity, query) for entity in rows]
            if high_scores and max(high_scores) >= 7000 and (pass_index == 0 or len(rows) >= bounded_limit):
                break
        return _rank_search_results(rows, query, bounded_limit)

    def search(self, db: Session, query: str, *, limit: int = 8) -> list[dict[str, str | int | float | None]]:
        try:
            results = self._term_search(db, query, limit=limit)
        except SQLAlchemyError:
            logger.exception("search_entity_term_query_failed")
            return self._legacy_search(db, query, limit=limit)
        if results:
            return results
        return self._legacy_search(db, query, limit=limit)


def search_entities(db: Session, query: str, *, limit: int = 8) -> list[dict[str, str | int | float | None]]:
    return PostgresSearchProvider().search(db, query, limit=limit)


def record_search_query(db: Session, query: str, result_count: int, top_result_type: str | None, latency_ms: float | None) -> None:
    try:
        db.add(
            SearchQueryLog(
                query=(query or "").strip()[:240],
                normalized_query=normalize_search_text(query)[:240],
                result_count=int(result_count or 0),
                top_result_type=top_result_type,
                latency_ms=float(latency_ms) if latency_ms is not None else None,
                created_at=datetime.now(timezone.utc),
            )
        )
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        logger.exception("search_query_log_failed query_length=%s", len(query or ""))


def search_coverage_audit(db: Session) -> dict[str, Any]:
    indexed_by_type = {
        row.entity_type: int(row.count or 0)
        for row in db.execute(
            select(SearchEntity.entity_type, func.count(SearchEntity.id).label("count")).group_by(SearchEntity.entity_type)
        ).all()
    }
    insider_keys: set[str] = set()
    for row in db.execute(
        select(InsiderTransactionNormalized.reporting_owner_cik, InsiderTransactionNormalized.ticker_normalized)
        .where(InsiderTransactionNormalized.reporting_owner_cik.is_not(None))
        .where(InsiderTransactionNormalized.reporting_owner_name.is_not(None))
        .where(InsiderTransactionNormalized.is_duplicate.is_(False))
    ).all():
        cik = _clean(row.reporting_owner_cik)
        if cik:
            insider_keys.add(f"{cik}:{normalize_symbol(row.ticker_normalized) or 'unknown'}")
    for row in db.execute(select(InsiderTransaction.reporting_cik, InsiderTransaction.symbol, InsiderTransaction.payload_json)).all():
        payload = _legacy_payload_dict(row.payload_json)
        cik = _clean(row.reporting_cik) or _clean(payload.get("reportingCik"))
        source_name = _clean(payload.get("reportingName")) or _clean(payload.get("reporting_owner_name")) or _clean(payload.get("ownerName"))
        if cik and source_name:
            insider_keys.add(f"{cik}:{normalize_symbol(row.symbol) or normalize_symbol(payload.get('symbol')) or 'unknown'}")

    routable_by_type = {
        "stock": len(_company_name_maps(db)[0]),
        "member": int(db.execute(select(func.count()).select_from(Member)).scalar_one() or 0),
        "insider": len(insider_keys),
        "institution": int(db.execute(select(func.count()).select_from(InstitutionalHolder)).scalar_one() or 0),
        "department": len(list_departments(db).get("items", [])),
    }
    total_routable = sum(routable_by_type.values())
    total_indexed = sum(indexed_by_type.values())
    coverage_pct = (total_indexed / total_routable * 100.0) if total_routable else 100.0
    missing_by_type = {
        entity_type: max(routable_by_type.get(entity_type, 0) - indexed_by_type.get(entity_type, 0), 0)
        for entity_type in sorted(routable_by_type)
    }
    invalid_urls = int(
        db.execute(
            select(func.count())
            .select_from(SearchEntity)
            .where((SearchEntity.canonical_url.is_(None)) | (~SearchEntity.canonical_url.like("/%")))
        ).scalar_one()
        or 0
    )
    return {
        "routable_entities": total_routable,
        "indexed_entities": total_indexed,
        "coverage_pct": round(coverage_pct, 2),
        "routable_by_type": routable_by_type,
        "indexed_by_type": indexed_by_type,
        "missing_entities": sum(missing_by_type.values()),
        "missing_by_type": missing_by_type,
        "invalid_urls": invalid_urls,
    }


def smoke_search_queries(db: Session, queries: list[str], *, limit: int = 5) -> list[dict[str, Any]]:
    rows = []
    for query in queries:
        started = perf_counter()
        items = search_entities(db, query, limit=limit)
        rows.append(
            {
                "query": query,
                "latency_ms": round((perf_counter() - started) * 1000, 2),
                "result_count": len(items),
                "top_result": items[0] if items else None,
            }
        )
    return rows
