from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import requests
from sqlalchemy import text

from app.db import SessionLocal


logger = logging.getLogger(__name__)
_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    try:
        return json.dumps(value, default=str, separators=(",", ":"))
    except Exception:
        return "{}"


def _input_chars(payload: dict[str, Any]) -> int:
    value = payload.get("input") or payload.get("messages") or ""
    if isinstance(value, str):
        return len(value)
    return len(_json(value))


def _request_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    tools = payload.get("tools") if isinstance(payload.get("tools"), list) else []
    return {
        "input_chars": _input_chars(payload),
        "max_output_tokens": payload.get("max_output_tokens") or payload.get("max_tokens"),
        "store": payload.get("store"),
        "tool_types": [str(tool.get("type") or "") for tool in tools if isinstance(tool, dict)],
        "has_structured_output": bool(payload.get("text") or payload.get("response_format")),
    }


def _ensure_schema(session: Any) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS openai_request_audit (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    feature TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    method TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    model TEXT,
                    status_code INTEGER,
                    succeeded BOOLEAN NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    request_metadata_json TEXT NOT NULL,
                    response_id TEXT,
                    usage_json TEXT,
                    error TEXT
                )
                """
            )
        )
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_openai_request_audit_created ON openai_request_audit (created_at)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_openai_request_audit_feature_created ON openai_request_audit (feature, created_at)"))
        session.commit()
        _SCHEMA_READY = True


def _persist(event: dict[str, Any]) -> None:
    session = None
    try:
        session = SessionLocal()
        _ensure_schema(session)
        session.execute(
            text(
                """
                INSERT INTO openai_request_audit (
                    id, created_at, feature, operation, method, endpoint, model,
                    status_code, succeeded, duration_ms, request_metadata_json,
                    response_id, usage_json, error
                ) VALUES (
                    :id, :created_at, :feature, :operation, :method, :endpoint, :model,
                    :status_code, :succeeded, :duration_ms, :request_metadata_json,
                    :response_id, :usage_json, :error
                )
                """
            ),
            event,
        )
        session.commit()
    except Exception:
        if session is not None:
            session.rollback()
        logger.exception("openai_request_audit_persist_failed feature=%s operation=%s", event.get("feature"), event.get("operation"))
    finally:
        if session is not None:
            session.close()


def audited_openai_request(
    *,
    feature: str,
    operation: str,
    method: str,
    endpoint: str,
    payload: dict[str, Any] | None,
    send: Callable[[], requests.Response],
    model: str | None = None,
) -> requests.Response:
    """Send an OpenAI request and durably log outcome metadata without prompts or secrets."""
    request_payload = payload if isinstance(payload, dict) else {}
    started = time.perf_counter()
    response: requests.Response | None = None
    error: str | None = None
    try:
        response = send()
        return response
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"[:1000]
        raise
    finally:
        response_data: dict[str, Any] = {}
        if response is not None:
            try:
                parsed = response.json()
                response_data = parsed if isinstance(parsed, dict) else {}
            except Exception:
                response_data = {}
        status_code = int(response.status_code) if response is not None else None
        _persist(
            {
                "id": f"oar_{uuid.uuid4().hex}",
                "created_at": _now(),
                "feature": feature,
                "operation": operation,
                "method": method.upper(),
                "endpoint": endpoint,
                "model": model or str(request_payload.get("model") or "") or None,
                "status_code": status_code,
                "succeeded": bool(response is not None and response.status_code < 400),
                "duration_ms": round((time.perf_counter() - started) * 1000),
                "request_metadata_json": _json(_request_metadata(request_payload)),
                "response_id": str(response_data.get("id") or "") or None,
                "usage_json": _json(response_data.get("usage") if isinstance(response_data.get("usage"), dict) else {}),
                "error": error or (str(response_data.get("error") or "")[:1000] if response is not None and response.status_code >= 400 else None),
            }
        )
