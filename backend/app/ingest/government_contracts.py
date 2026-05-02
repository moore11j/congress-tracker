from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import logging
import re
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import inspect, select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.db import SessionLocal, engine
from app.models import AppSetting, Event, GovernmentContract, GovernmentContractAction
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

USA_SPENDING_ENDPOINT = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
USA_SPENDING_TRANSACTIONS_ENDPOINT = "https://api.usaspending.gov/api/v2/transactions/"
USA_SPENDING_SOURCE = "usaspending"
DEFAULT_CONTRACT_AWARD_TYPE_CODES = ["A", "B", "C", "D"]
DEFAULT_FIELDS = [
    "Award ID",
    "Recipient Name",
    "Award Amount",
    "Start Date",
    "End Date",
    "Awarding Agency",
    "Awarding Sub Agency",
    "Funding Agency",
    "Funding Sub Agency",
    "Description",
    "Contract Award Type",
    "Award Type",
]
DEFAULT_TARGET_SYMBOLS = [
    "LMT",
    "RTX",
    "BA",
    "GD",
    "NOC",
    "HII",
    "PLTR",
    "MSFT",
    "AMZN",
    "ORCL",
    "IBM",
    "LDOS",
    "CACI",
    "BAH",
    "SAIC",
]
CONTRACT_INGEST_LAST_RUN_AT_KEY = "government_contracts_ingest_last_run_at"
CONTRACT_INGEST_LAST_SUMMARY_KEY = "government_contracts_ingest_last_summary_json"
CONTRACT_INGEST_MIN_INTERVAL = timedelta(hours=12)
_ALIAS_FILE = Path(__file__).resolve().parent.parent / "data" / "government_contract_ticker_aliases.json"
_CORPORATE_SUFFIXES = {
    "INC",
    "INCORPORATED",
    "CORP",
    "CORPORATION",
    "LLC",
    "LTD",
    "CO",
    "COMPANY",
    "THE",
}


def ensure_government_contracts_schema(target_engine=engine) -> None:
    GovernmentContract.__table__.create(bind=target_engine, checkfirst=True)
    GovernmentContractAction.__table__.create(bind=target_engine, checkfirst=True)

    inspector = inspect(target_engine)
    columns = {column["name"] for column in inspector.get_columns("government_contracts")}
    additions = {
        "award_id": "TEXT",
        "dedupe_key": "TEXT",
        "recipient_name": "TEXT",
        "raw_recipient_name": "TEXT",
        "awarding_sub_agency": "TEXT",
        "funding_agency": "TEXT",
        "funding_sub_agency": "TEXT",
        "period_start": "DATE",
        "period_end": "DATE",
        "contract_type": "TEXT",
        "source_url": "TEXT",
        "mapping_method": "TEXT",
        "mapping_confidence": "FLOAT",
        "updated_at": "TIMESTAMP",
    }
    with target_engine.begin() as conn:
        for name, column_type in additions.items():
            if name not in columns:
                conn.execute(text(f"ALTER TABLE government_contracts ADD COLUMN {name} {column_type}"))
        conn.execute(
            text(
                "UPDATE government_contracts "
                "SET source = 'usaspending' "
                "WHERE source IS NULL OR TRIM(source) = ''"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_government_contracts_symbol "
                "ON government_contracts (symbol)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_government_contracts_awarding_agency "
                "ON government_contracts (awarding_agency)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_government_contracts_source_award_id "
                "ON government_contracts (source, award_id)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_government_contracts_source_dedupe_key "
                "ON government_contracts (source, dedupe_key)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_government_contract_actions_symbol "
                "ON government_contract_actions (symbol)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_government_contract_actions_action_date "
                "ON government_contract_actions (action_date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_government_contract_actions_obligated_amount "
                "ON government_contract_actions (obligated_amount)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_government_contract_actions_parent_award_id "
                "ON government_contract_actions (parent_award_id)"
            )
        )


def government_contracts_table_exists(db: Session) -> bool:
    return inspect(db.get_bind()).has_table("government_contracts")


def normalize_recipient_name(value: str | None) -> str:
    raw = (value or "").strip().upper()
    if not raw:
        return ""
    cleaned = re.sub(r"[^A-Z0-9]+", " ", raw)
    tokens = [token for token in cleaned.split() if token and token not in _CORPORATE_SUFFIXES]
    return " ".join(tokens)


def load_ticker_aliases(path: Path | None = None) -> dict[str, str]:
    raw = json.loads((path or _ALIAS_FILE).read_text(encoding="utf-8"))
    aliases: dict[str, str] = {}
    for alias, symbol in raw.items():
        normalized_alias = normalize_recipient_name(alias)
        normalized_symbol = normalize_symbol(symbol)
        if normalized_alias and normalized_symbol:
            aliases[normalized_alias] = normalized_symbol
    return aliases


def aliases_for_symbols(symbols: list[str], alias_map: dict[str, str]) -> dict[str, list[str]]:
    requested = {normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)}
    grouped: dict[str, list[str]] = defaultdict(list)
    for alias, symbol in alias_map.items():
        if symbol in requested:
            grouped[symbol].append(alias)
    return {
        symbol: sorted(set(aliases), key=lambda item: (-len(item), item))
        for symbol, aliases in grouped.items()
    }


def match_recipient_to_symbol(recipient_name: str | None, alias_map: dict[str, str]) -> dict[str, Any] | None:
    normalized_name = normalize_recipient_name(recipient_name)
    if not normalized_name:
        return None

    exact_symbol = alias_map.get(normalized_name)
    if exact_symbol:
        return {
            "symbol": exact_symbol,
            "mapping_method": "alias_exact",
            "mapping_confidence": 1.0,
            "matched_alias": normalized_name,
        }

    contains_hits: list[tuple[str, str]] = []
    for alias, symbol in alias_map.items():
        if len(alias) < 8:
            continue
        if alias in normalized_name or normalized_name in alias:
            contains_hits.append((alias, symbol))
    if contains_hits:
        symbols = {symbol for _alias, symbol in contains_hits}
        if len(symbols) == 1:
            matched_alias = max((alias for alias, _symbol in contains_hits), key=len)
            return {
                "symbol": next(iter(symbols)),
                "mapping_method": "alias_contains",
                "mapping_confidence": 0.98,
                "matched_alias": matched_alias,
            }

    best_alias = ""
    best_symbol = None
    best_ratio = 0.0
    for alias, symbol in alias_map.items():
        ratio = difflib.SequenceMatcher(a=normalized_name, b=alias).ratio()
        if ratio > best_ratio:
            best_alias = alias
            best_symbol = symbol
            best_ratio = ratio
    if best_symbol and best_ratio >= 0.95:
        return {
            "symbol": best_symbol,
            "mapping_method": "alias_fuzzy",
            "mapping_confidence": round(best_ratio, 4),
            "matched_alias": best_alias,
        }
    return None


def fetch_spending_by_award(
    *,
    lookback_days: int,
    min_award_amount: float,
    limit: int,
    max_pages: int,
    recipient_search_text: str | None = None,
    award_type_codes: list[str] | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    today = date.today()
    start_date = (today - timedelta(days=max(1, int(lookback_days or 365)))).isoformat()
    end_date = today.isoformat()
    page = 1
    rows: list[dict[str, Any]] = []
    codes = award_type_codes or DEFAULT_CONTRACT_AWARD_TYPE_CODES

    while page <= max(1, int(max_pages or 1)):
        filters: dict[str, Any] = {
            "award_type_codes": codes,
            "time_period": [{"start_date": start_date, "end_date": end_date}],
            "award_amounts": [{"lower_bound": float(min_award_amount or 0)}],
        }
        if recipient_search_text:
            filters["recipient_search_text"] = [recipient_search_text]
        payload = {
            "filters": filters,
            "fields": DEFAULT_FIELDS,
            "page": page,
            "limit": max(1, min(int(limit or 100), 100)),
            "sort": "Award Amount",
            "order": "desc",
        }
        response = requests.post(USA_SPENDING_ENDPOINT, json=payload, timeout=45)
        response.raise_for_status()
        body = response.json()
        page_rows = body.get("results") if isinstance(body, dict) else None
        if not isinstance(page_rows, list) or not page_rows:
            break
        rows.extend(row for row in page_rows if isinstance(row, dict))

        metadata = body.get("page_metadata") if isinstance(body.get("page_metadata"), dict) else {}
        has_next = bool(metadata.get("hasNext"))
        if verbose:
            logger.info(
                "usaspending page=%s fetched=%s has_next=%s recipient=%s",
                page,
                len(page_rows),
                has_next,
                recipient_search_text,
            )
        if not has_next:
            break
        page += 1

    return rows


def fetch_award_transaction_history(
    award_id: str,
    *,
    limit: int = 5000,
    max_pages: int = 10,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    cleaned_award_id = _clean_text(award_id)
    if not cleaned_award_id:
        return []

    page = 1
    rows: list[dict[str, Any]] = []
    while page <= max(1, int(max_pages or 1)):
        payload = {
            "award_id": cleaned_award_id,
            "page": page,
            "limit": max(1, min(int(limit or 5000), 5000)),
            "sort": "action_date",
            "order": "desc",
        }
        response = requests.post(USA_SPENDING_TRANSACTIONS_ENDPOINT, json=payload, timeout=45)
        response.raise_for_status()
        body = response.json()
        page_rows = body.get("results") if isinstance(body, dict) else None
        if not isinstance(page_rows, list) or not page_rows:
            break
        rows.extend(row for row in page_rows if isinstance(row, dict))
        metadata = body.get("page_metadata") if isinstance(body.get("page_metadata"), dict) else {}
        has_next = bool(metadata.get("hasNext") or metadata.get("has_next"))
        if verbose:
            logger.info(
                "usaspending transactions award_id=%s page=%s fetched=%s has_next=%s",
                cleaned_award_id,
                page,
                len(page_rows),
                has_next,
            )
        if not has_next:
            break
        page += 1

    return rows


def normalize_usaspending_award(raw: dict[str, Any], alias_map: dict[str, str]) -> dict[str, Any] | None:
    raw_recipient_name = _clean_text(raw.get("Recipient Name"))
    mapping = match_recipient_to_symbol(raw_recipient_name, alias_map)
    if mapping is None:
        return None

    award_amount = _positive_float(raw.get("Award Amount"))
    if award_amount is None:
        return None

    period_start = _parse_date(raw.get("Start Date"))
    period_end = _parse_date(raw.get("End Date"))
    award_date = period_start or period_end
    if award_date is None:
        return None

    award_id = _clean_text(raw.get("Award ID")) or _clean_text(raw.get("generated_internal_id")) or _clean_text(raw.get("internal_id"))
    description = _clean_text(raw.get("Description"))
    recipient_name = _clean_text(raw_recipient_name)
    source_url = _award_source_url(raw)
    contract_type = _clean_text(raw.get("Contract Award Type")) or _clean_text(raw.get("Award Type"))

    normalized = {
        "award_id": award_id,
        "dedupe_key": _government_contract_dedupe_key(
            symbol=mapping["symbol"],
            recipient_name=recipient_name,
            award_date=award_date,
            award_amount=award_amount,
            description=description,
        ),
        "symbol": mapping["symbol"],
        "recipient_name": recipient_name,
        "raw_recipient_name": recipient_name,
        "awarding_agency": _clean_text(raw.get("Awarding Agency")),
        "awarding_sub_agency": _clean_text(raw.get("Awarding Sub Agency")),
        "funding_agency": _clean_text(raw.get("Funding Agency")),
        "funding_sub_agency": _clean_text(raw.get("Funding Sub Agency")),
        "award_amount": round(award_amount, 2),
        "award_date": award_date,
        "period_start": period_start,
        "period_end": period_end,
        "description": description,
        "contract_type": contract_type,
        "source_url": source_url,
        "source": USA_SPENDING_SOURCE,
        "mapping_method": mapping["mapping_method"],
        "mapping_confidence": mapping["mapping_confidence"],
        "payload_json": json.dumps(raw, sort_keys=True),
    }
    return normalized


def normalize_usaspending_action(
    raw: dict[str, Any],
    *,
    parent_award: dict[str, Any],
) -> dict[str, Any] | None:
    action_date = _parse_date(
        raw.get("action_date")
        or raw.get("actionDate")
        or raw.get("Action Date")
        or raw.get("period_of_performance_current_end_date")
    )
    obligated_amount = _float_value(
        raw.get("transaction_obligated_amount")
        or raw.get("transactionObligatedAmount")
        or raw.get("federal_action_obligation")
        or raw.get("federalActionObligation")
        or raw.get("obligated_amount")
        or raw.get("amount")
        or raw.get("Amount")
    )
    if action_date is None or obligated_amount is None:
        return None

    parent_award_id = _clean_text(parent_award.get("award_id"))
    if not parent_award_id:
        return None

    modification_number = _clean_text(
        raw.get("modification_number")
        or raw.get("modificationNumber")
        or raw.get("Modification Number")
        or raw.get("transaction_unique_id")
        or raw.get("transactionUniqueId")
    )
    description = _clean_text(
        raw.get("transaction_description")
        or raw.get("transactionDescription")
        or raw.get("description")
        or raw.get("Description")
    )
    action_type = _clean_text(
        raw.get("action_type")
        or raw.get("actionType")
        or raw.get("action_type_description")
        or raw.get("actionTypeDescription")
    )
    dedupe_key = _government_contract_action_dedupe_key(
        parent_award_id=parent_award_id,
        modification_number=modification_number,
        action_date=action_date,
        obligated_amount=obligated_amount,
        description=description,
    )

    action_payload = {**raw, "parent_award": parent_award}
    return {
        "parent_award_id": parent_award_id,
        "modification_number": modification_number,
        "dedupe_key": dedupe_key,
        "symbol": parent_award["symbol"],
        "recipient_name": parent_award.get("recipient_name"),
        "awarding_agency": _clean_text(raw.get("awarding_agency"))
        or _clean_text(raw.get("awardingAgency"))
        or parent_award.get("awarding_agency"),
        "awarding_sub_agency": _clean_text(raw.get("awarding_sub_agency"))
        or _clean_text(raw.get("awardingSubAgency"))
        or parent_award.get("awarding_sub_agency"),
        "action_date": action_date,
        "obligated_amount": round(float(obligated_amount), 2),
        "description": description or parent_award.get("description"),
        "action_type": action_type,
        "source_url": parent_award.get("source_url") or f"https://www.usaspending.gov/award/{parent_award_id}",
        "source": USA_SPENDING_SOURCE,
        "payload_json": json.dumps(action_payload, default=str, sort_keys=True),
    }


def normalize_usaspending_actions(
    raw_award: dict[str, Any],
    *,
    parent_award: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_actions: list[dict[str, Any]] = []
    for key in ("transactions", "transaction_history", "transactionHistory", "Transaction History", "modifications"):
        value = raw_award.get(key)
        if isinstance(value, list):
            raw_actions.extend(item for item in value if isinstance(item, dict))

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_action in raw_actions:
        action = normalize_usaspending_action(raw_action, parent_award=parent_award)
        if action is None or action["dedupe_key"] in seen:
            continue
        seen.add(action["dedupe_key"])
        normalized.append(action)
    return normalized


def ingest_government_contracts(
    *,
    lookback_days: int = 365,
    min_award_amount: float = 1_000_000,
    limit: int = 100,
    max_pages: int = 50,
    recipient: str | None = None,
    symbols: list[str] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
    enforce_guardrail: bool = True,
) -> dict[str, Any]:
    alias_map = load_ticker_aliases()
    requested_symbols = [symbol for symbol in (symbols or []) if normalize_symbol(symbol)]
    targeted_aliases = aliases_for_symbols(requested_symbols, alias_map)
    search_terms: list[str] = []
    if recipient:
        search_terms.append(recipient.strip())
    for symbol in requested_symbols:
        for alias in targeted_aliases.get(normalize_symbol(symbol) or "", []):
            search_terms.append(alias)
    deduped_terms: list[str] = []
    seen_terms: set[str] = set()
    for term in search_terms:
        normalized_term = normalize_recipient_name(term)
        if normalized_term and normalized_term not in seen_terms:
            deduped_terms.append(term)
            seen_terms.add(normalized_term)

    mode = "targeted" if deduped_terms else "broad"
    summary = {
        "status": "ok",
        "mode": mode,
        "lookback_days": max(1, int(lookback_days or 365)),
        "min_award_amount": float(min_award_amount or 0),
        "fetched_count": 0,
        "mapped_count": 0,
        "inserted_count": 0,
        "updated_count": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "actions_inserted": 0,
        "actions_updated": 0,
        "skipped_count": 0,
        "unmapped_count": 0,
        "unmapped_top_recipients": [],
        "last_run_at": None,
    }

    db = SessionLocal()
    try:
        ensure_government_contracts_schema(db.get_bind())
        now = datetime.now(timezone.utc)
        if enforce_guardrail and not dry_run:
            guardrail = _guardrail_state(db, now=now)
            if not guardrail["allowed"]:
                summary.update(
                    {
                        "status": "guarded_skip",
                        "last_run_at": guardrail["last_run_at"],
                        "guardrail_reason": "within_12_hours",
                    }
                )
                logger.info("government_contracts_ingest skipped summary=%s", summary)
                return summary

        unmapped_counter: Counter[str] = Counter()
        search_space = deduped_terms or [None]
        for term in search_space:
            rows = fetch_spending_by_award(
                lookback_days=lookback_days,
                min_award_amount=min_award_amount,
                limit=limit,
                max_pages=max_pages,
                recipient_search_text=term,
                verbose=verbose,
            )
            summary["fetched_count"] += len(rows)
            for raw_row in rows:
                recipient_name = _clean_text(raw_row.get("Recipient Name"))
                mapping = match_recipient_to_symbol(recipient_name, alias_map)
                if mapping is None:
                    if recipient_name:
                        unmapped_counter[recipient_name] += 1
                    summary["unmapped_count"] += 1
                    continue
                normalized_row = normalize_usaspending_award(raw_row, alias_map)
                if normalized_row is None:
                    summary["skipped_count"] += 1
                    continue

                summary["mapped_count"] += 1
                if dry_run:
                    continue

                upsert_result = _upsert_government_contract(db, normalized_row)
                if upsert_result == "inserted":
                    summary["inserted_count"] += 1
                    summary["rows_inserted"] += 1
                elif upsert_result == "updated":
                    summary["updated_count"] += 1
                    summary["rows_updated"] += 1
                else:
                    summary["skipped_count"] += 1

                action_rows = normalize_usaspending_actions(raw_row, parent_award=normalized_row)
                transaction_award_id = _award_transaction_lookup_id(raw_row) or normalized_row.get("award_id")
                if not action_rows and transaction_award_id:
                    try:
                        action_rows = [
                            action
                            for raw_action in fetch_award_transaction_history(
                                transaction_award_id,
                                verbose=verbose,
                            )
                            for action in [normalize_usaspending_action(raw_action, parent_award=normalized_row)]
                            if action is not None
                        ]
                    except Exception:
                        logger.info(
                            "government_contracts transaction history unavailable award_id=%s",
                            normalized_row.get("award_id"),
                            exc_info=verbose,
                        )

                for action_row in action_rows:
                    action_result = _upsert_government_contract_action(db, action_row)
                    if action_result == "inserted":
                        summary["actions_inserted"] += 1
                    elif action_result == "updated":
                        summary["actions_updated"] += 1

            if not dry_run:
                db.commit()

        summary["last_run_at"] = now.isoformat()
        summary["unmapped_top_recipients"] = [
            {"recipient_name": name, "count": count}
            for name, count in unmapped_counter.most_common(10)
        ]

        if not dry_run:
            _set_setting(db, CONTRACT_INGEST_LAST_RUN_AT_KEY, summary["last_run_at"])
            _set_setting(db, CONTRACT_INGEST_LAST_SUMMARY_KEY, json.dumps(summary, sort_keys=True))
            db.commit()

        logger.info("government_contracts_ingest summary=%s", summary)
        return summary
    finally:
        db.close()


def run_government_contracts_ingest_job(
    *,
    lookback_days: int,
    min_award_amount: float = 1_000_000,
    max_pages: int = 10,
    limit: int = 100,
    symbols: list[str] | None = None,
    recipient: str | None = None,
) -> dict[str, Any]:
    return ingest_government_contracts(
        lookback_days=lookback_days,
        min_award_amount=min_award_amount,
        max_pages=max_pages,
        limit=limit,
        symbols=symbols or DEFAULT_TARGET_SYMBOLS,
        recipient=recipient,
        dry_run=False,
        verbose=False,
        enforce_guardrail=True,
    )


def _upsert_government_contract(db: Session, values: dict[str, Any]) -> str:
    existing = _select_existing_government_contract(db, values)
    mutable_fields = {
        key: values[key]
        for key in (
            "symbol",
            "recipient_name",
            "raw_recipient_name",
            "awarding_agency",
            "awarding_sub_agency",
            "funding_agency",
            "funding_sub_agency",
            "award_amount",
            "award_date",
            "period_start",
            "period_end",
            "description",
            "contract_type",
            "source_url",
            "mapping_method",
            "mapping_confidence",
            "payload_json",
        )
    }
    if existing is not None:
        changed = any(getattr(existing, key) != value for key, value in mutable_fields.items())
    else:
        changed = False

    conflict_columns = ["source", "award_id"] if values.get("award_id") else ["source", "dedupe_key"]
    stmt = _government_contract_insert(db).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=conflict_columns,
        set_={**mutable_fields, "updated_at": datetime.now(timezone.utc)},
    )
    db.execute(stmt)
    db.flush()

    contract = _select_existing_government_contract(db, values)
    if contract is None:
        return "skipped"
    _sync_government_contract_event(db, contract)
    db.flush()

    if existing is None:
        return "inserted"
    if changed:
        return "updated"
    return "skipped"


def _upsert_government_contract_action(db: Session, values: dict[str, Any]) -> str:
    existing = _select_existing_government_contract_action(db, values)
    mutable_fields = {
        key: values[key]
        for key in (
            "symbol",
            "recipient_name",
            "awarding_agency",
            "awarding_sub_agency",
            "action_date",
            "obligated_amount",
            "description",
            "action_type",
            "source_url",
            "payload_json",
        )
    }
    if existing is not None:
        changed = any(getattr(existing, key) != value for key, value in mutable_fields.items())
    else:
        changed = False

    conflict_columns = (
        ["source", "parent_award_id", "modification_number"]
        if values.get("modification_number")
        else ["source", "parent_award_id", "dedupe_key"]
    )
    stmt = _government_contract_action_insert(db).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=conflict_columns,
        set_={**mutable_fields, "updated_at": datetime.now(timezone.utc)},
    )
    db.execute(stmt)
    db.flush()

    action = _select_existing_government_contract_action(db, values)
    if action is None:
        return "skipped"
    _sync_government_contract_action_event(db, action)
    db.flush()

    if existing is None:
        return "inserted"
    if changed:
        return "updated"
    return "skipped"


def _select_existing_government_contract(db: Session, values: dict[str, Any]) -> GovernmentContract | None:
    if values.get("award_id"):
        return db.execute(
            select(GovernmentContract)
            .where(GovernmentContract.source == values["source"])
            .where(GovernmentContract.award_id == values["award_id"])
            .limit(1)
        ).scalar_one_or_none()
    return db.execute(
        select(GovernmentContract)
        .where(GovernmentContract.source == values["source"])
        .where(GovernmentContract.dedupe_key == values["dedupe_key"])
        .limit(1)
    ).scalar_one_or_none()


def _select_existing_government_contract_action(db: Session, values: dict[str, Any]) -> GovernmentContractAction | None:
    if values.get("modification_number"):
        return db.execute(
            select(GovernmentContractAction)
            .where(GovernmentContractAction.source == values["source"])
            .where(GovernmentContractAction.parent_award_id == values["parent_award_id"])
            .where(GovernmentContractAction.modification_number == values["modification_number"])
            .limit(1)
        ).scalar_one_or_none()
    return db.execute(
        select(GovernmentContractAction)
        .where(GovernmentContractAction.source == values["source"])
        .where(GovernmentContractAction.parent_award_id == values["parent_award_id"])
        .where(GovernmentContractAction.dedupe_key == values["dedupe_key"])
        .limit(1)
    ).scalar_one_or_none()


def _sync_government_contract_event(db: Session, contract: GovernmentContract) -> None:
    event = db.get(Event, contract.event_id) if contract.event_id else None
    if event is None and contract.award_id:
        event = db.execute(
            select(Event)
            .where(Event.event_type == "government_contract")
            .where(Event.source == USA_SPENDING_SOURCE)
            .where(Event.symbol == contract.symbol)
            .where(Event.payload_json.contains(contract.award_id))
            .limit(1)
        ).scalar_one_or_none()
        if event is not None:
            linked_contract_id = db.execute(
                select(GovernmentContract.id)
                .where(GovernmentContract.event_id == event.id)
                .where(GovernmentContract.id != contract.id)
                .limit(1)
            ).scalar_one_or_none()
            if linked_contract_id is not None:
                event = None

    payload_json = json.dumps(_government_contract_event_payload(contract), sort_keys=True)
    event_ts = datetime.combine(contract.award_date, time.min, tzinfo=timezone.utc)
    amount = int(round(contract.award_amount)) if contract.award_amount and contract.award_amount > 0 else None

    if event is None:
        event = Event(
            event_type="government_contract",
            ts=event_ts,
            event_date=event_ts,
            symbol=contract.symbol,
            source=USA_SPENDING_SOURCE,
            member_name=contract.recipient_name,
            member_bioguide_id=None,
            chamber=None,
            party=None,
            trade_type="award",
            transaction_type=contract.contract_type,
            amount_min=amount,
            amount_max=amount,
            impact_score=0.0,
            payload_json=payload_json,
        )
        db.add(event)
        db.flush()
        contract.event_id = event.id
        return

    event.ts = event_ts
    event.event_date = event_ts
    event.symbol = contract.symbol
    event.source = USA_SPENDING_SOURCE
    event.member_name = contract.recipient_name
    event.trade_type = "award"
    event.transaction_type = contract.contract_type
    event.amount_min = amount
    event.amount_max = amount
    event.payload_json = payload_json
    contract.event_id = event.id


def _sync_government_contract_action_event(db: Session, action: GovernmentContractAction) -> None:
    event = db.get(Event, action.event_id) if action.event_id else None
    payload_json = json.dumps(_government_contract_action_event_payload(action), sort_keys=True)
    event_ts = datetime.combine(action.action_date, time.min, tzinfo=timezone.utc)
    amount = int(round(action.obligated_amount)) if action.obligated_amount is not None else None

    if event is None:
        event = Event(
            event_type="government_contract",
            ts=event_ts,
            event_date=event_ts,
            symbol=action.symbol,
            source=USA_SPENDING_SOURCE,
            member_name=action.awarding_agency,
            member_bioguide_id=None,
            chamber=None,
            party=None,
            trade_type="funding_action",
            transaction_type=action.action_type,
            amount_min=amount,
            amount_max=amount,
            impact_score=0.0,
            payload_json=payload_json,
        )
        db.add(event)
        db.flush()
        action.event_id = event.id
        return

    event.ts = event_ts
    event.event_date = event_ts
    event.symbol = action.symbol
    event.source = USA_SPENDING_SOURCE
    event.member_name = action.awarding_agency
    event.trade_type = "funding_action"
    event.transaction_type = action.action_type
    event.amount_min = amount
    event.amount_max = amount
    event.payload_json = payload_json
    action.event_id = event.id


def _government_contract_event_payload(contract: GovernmentContract) -> dict[str, Any]:
    raw_payload = _loads_dict(contract.payload_json)
    return {
        "award_id": contract.award_id,
        "symbol": contract.symbol,
        "recipient_name": contract.recipient_name,
        "raw_recipient_name": contract.raw_recipient_name,
        "award_date": contract.award_date.isoformat() if contract.award_date else None,
        "period_start": contract.period_start.isoformat() if contract.period_start else None,
        "period_end": contract.period_end.isoformat() if contract.period_end else None,
        "award_amount": round(float(contract.award_amount), 2),
        "awarding_agency": contract.awarding_agency,
        "awarding_sub_agency": contract.awarding_sub_agency,
        "funding_agency": contract.funding_agency,
        "funding_sub_agency": contract.funding_sub_agency,
        "description": contract.description,
        "contract_type": contract.contract_type,
        "source_url": contract.source_url,
        "mapping_method": contract.mapping_method,
        "mapping_confidence": contract.mapping_confidence,
        "raw": raw_payload,
    }


def _government_contract_action_event_payload(action: GovernmentContractAction) -> dict[str, Any]:
    raw_payload = _loads_dict(action.payload_json)
    period_start = None
    parent = raw_payload.get("parent_award") if isinstance(raw_payload.get("parent_award"), dict) else {}
    if isinstance(parent, dict):
        period_start = parent.get("period_start")
    return {
        "event_subtype": "funding_action",
        "parent_award_id": action.parent_award_id,
        "award_id": action.parent_award_id,
        "modification_number": action.modification_number,
        "symbol": action.symbol,
        "recipient_name": action.recipient_name,
        "awarding_agency": action.awarding_agency,
        "awarding_sub_agency": action.awarding_sub_agency,
        "action_date": action.action_date.isoformat() if action.action_date else None,
        "report_date": action.action_date.isoformat() if action.action_date else None,
        "obligated_amount": round(float(action.obligated_amount), 2),
        "amount": round(float(action.obligated_amount), 2),
        "description": action.description,
        "title": action.description,
        "action_type": action.action_type,
        "period_start": period_start,
        "source_url": action.source_url,
        "raw": raw_payload,
    }


def _guardrail_state(db: Session, *, now: datetime) -> dict[str, Any]:
    row = db.get(AppSetting, CONTRACT_INGEST_LAST_RUN_AT_KEY)
    last_run_at = _parse_datetime(row.value) if row and row.value else None
    if last_run_at is None:
        return {"allowed": True, "last_run_at": None}
    allowed = now - last_run_at >= CONTRACT_INGEST_MIN_INTERVAL
    return {"allowed": allowed, "last_run_at": last_run_at.isoformat()}


def _set_setting(db: Session, key: str, value: str | None) -> None:
    row = db.get(AppSetting, key)
    if row is None:
        row = AppSetting(key=key)
        db.add(row)
    row.value = value


def _government_contract_insert(db: Session):
    dialect = db.get_bind().dialect.name
    if dialect == "postgresql":
        return postgres_insert(GovernmentContract.__table__)
    return sqlite_insert(GovernmentContract.__table__)


def _government_contract_action_insert(db: Session):
    dialect = db.get_bind().dialect.name
    if dialect == "postgresql":
        return postgres_insert(GovernmentContractAction.__table__)
    return sqlite_insert(GovernmentContractAction.__table__)


def _government_contract_dedupe_key(
    *,
    symbol: str,
    recipient_name: str | None,
    award_date: date,
    award_amount: float,
    description: str | None,
) -> str:
    description_hash = hashlib.sha1((description or "").encode("utf-8")).hexdigest()[:16]
    key = "|".join(
        [
            symbol,
            normalize_recipient_name(recipient_name),
            award_date.isoformat(),
            f"{award_amount:.2f}",
            description_hash,
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _government_contract_action_dedupe_key(
    *,
    parent_award_id: str,
    modification_number: str | None,
    action_date: date,
    obligated_amount: float,
    description: str | None,
) -> str:
    if modification_number:
        raw = "|".join([parent_award_id, modification_number])
    else:
        description_hash = hashlib.sha1((description or "").encode("utf-8")).hexdigest()[:16]
        raw = "|".join(
            [
                parent_award_id,
                action_date.isoformat(),
                f"{obligated_amount:.2f}",
                description_hash,
            ]
        )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _award_source_url(raw: dict[str, Any]) -> str:
    generated_id = _clean_text(raw.get("generated_internal_id"))
    if generated_id:
        return f"https://www.usaspending.gov/award/{generated_id}"
    return USA_SPENDING_ENDPOINT


def _award_transaction_lookup_id(raw: dict[str, Any]) -> str | None:
    return (
        _clean_text(raw.get("generated_internal_id"))
        or _clean_text(raw.get("generated_unique_award_id"))
        or _clean_text(raw.get("unique_award_key"))
        or _clean_text(raw.get("internal_id"))
    )


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _positive_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(str(value).replace("$", "").replace(",", "").strip())
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _float_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(str(value).replace("$", "").replace(",", "").strip())
    except ValueError:
        return None
    return parsed if parsed == parsed else None


def _loads_dict(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest government contract awards from USAspending.")
    parser.add_argument("--lookback-days", type=int, default=365)
    parser.add_argument("--min-award-amount", type=float, default=1_000_000)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--recipient", type=str, default=None)
    parser.add_argument("--symbols", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    parsed_symbols = [symbol.strip() for symbol in (args.symbols or "").split(",") if symbol.strip()]
    result = ingest_government_contracts(
        lookback_days=args.lookback_days,
        min_award_amount=args.min_award_amount,
        limit=args.limit,
        max_pages=args.max_pages,
        recipient=args.recipient,
        symbols=parsed_symbols,
        dry_run=bool(args.dry_run),
        verbose=bool(args.verbose),
        enforce_guardrail=not bool(args.dry_run),
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
