from __future__ import annotations

import hashlib
import logging
import math
import os
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Iterable

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.auth import current_user, normalize_email, require_admin_user, reset_token_hash
from app.db import get_db
from app.models import UserAccount

logger = logging.getLogger(__name__)

RATE_LIMIT_MESSAGE = "Too many requests. Please try again later."


@dataclass(frozen=True)
class RateLimitRule:
    bucket: str
    limit: int
    window_seconds: int
    key: str


class InMemoryRateLimiter:
    """
    Small v1 sliding-window limiter.

    TODO(security): move rate-limit storage to Redis or another shared store before
    running multiple Fly machines that must enforce global limits.
    """

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic
        self._hits: dict[tuple[str, str], deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def set_clock(self, clock: Callable[[], float]) -> None:
        with self._lock:
            self._clock = clock
            self._hits.clear()

    def reset(self) -> None:
        with self._lock:
            self._hits.clear()

    def check(self, rules: Iterable[RateLimitRule], *, endpoint_group: str) -> None:
        if not rate_limit_enabled():
            return

        now = self._clock()
        materialized = list(rules)
        if not materialized:
            return

        with self._lock:
            retry_after = 0
            blocked_rule: RateLimitRule | None = None
            for rule in materialized:
                entries = self._hits[(rule.bucket, rule.key)]
                self._prune(entries, now, rule.window_seconds)
                if len(entries) >= rule.limit:
                    retry_after = max(1, math.ceil(rule.window_seconds - (now - entries[0])))
                    blocked_rule = rule
                    break

            if blocked_rule is not None:
                logger.warning(
                    "rate_limit_blocked bucket=%s key_hash=%s endpoint_group=%s retry_after=%s",
                    blocked_rule.bucket,
                    _safe_key_hash(blocked_rule.key),
                    endpoint_group,
                    retry_after,
                )
                raise HTTPException(
                    status_code=429,
                    detail=RATE_LIMIT_MESSAGE,
                    headers={"Retry-After": str(retry_after)},
                )

            for rule in materialized:
                self._hits[(rule.bucket, rule.key)].append(now)

    @staticmethod
    def _prune(entries: deque[float], now: float, window_seconds: int) -> None:
        cutoff = now - window_seconds
        while entries and entries[0] <= cutoff:
            entries.popleft()


rate_limiter = InMemoryRateLimiter()


def rate_limit_enabled() -> bool:
    return os.getenv("RATE_LIMIT_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}


def reset_rate_limiter_for_tests() -> None:
    rate_limiter.reset()


def _safe_key_hash(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _client_ip(request: Request | None) -> str:
    if request is None or request.client is None or not request.client.host:
        return "unknown"
    return request.client.host


def _request_actor_key(request: Request, db: Session, *, required: bool = False) -> str:
    user = current_user(db, request, required=required)
    if isinstance(user, UserAccount) and user.id is not None:
        return f"user:{user.id}"
    return f"ip:{_client_ip(request)}"


def _admin_actor_key(request: Request, db: Session) -> str:
    user = require_admin_user(db, request)
    return f"admin:{user.id}"


async def _request_json(request: Request) -> dict:
    try:
        payload = await request.json()
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _check(endpoint_group: str, rules: Iterable[RateLimitRule]) -> None:
    rate_limiter.check(rules, endpoint_group=endpoint_group)


async def rate_limit_auth_login(request: Request) -> None:
    payload = await _request_json(request)
    email = normalize_email(str(payload.get("email") or ""))
    ip = _client_ip(request)
    _check(
        "auth_login",
        [
            RateLimitRule("auth_login_ip_email", 5, 5 * 60, f"ip:{ip}:email:{email or 'missing'}"),
            RateLimitRule("auth_login_ip", 20, 60 * 60, f"ip:{ip}"),
        ],
    )


def rate_limit_register(request: Request) -> None:
    ip = _client_ip(request)
    _check("auth_register", [RateLimitRule("auth_register_ip", 5, 60 * 60, f"ip:{ip}")])


async def rate_limit_password_reset_request(request: Request) -> None:
    payload = await _request_json(request)
    email = normalize_email(str(payload.get("email") or ""))
    ip = _client_ip(request)
    _check(
        "password_reset_request",
        [
            RateLimitRule("password_reset_request_ip_email", 3, 15 * 60, f"ip:{ip}:email:{email or 'missing'}"),
            RateLimitRule("password_reset_request_ip", 10, 60 * 60, f"ip:{ip}"),
        ],
    )


async def rate_limit_password_reset_confirm(request: Request) -> None:
    payload = await _request_json(request)
    token = str(payload.get("token") or "")
    token_key = reset_token_hash(token) if token else "missing"
    ip = _client_ip(request)
    _check(
        "password_reset_confirm",
        [
            RateLimitRule("password_reset_confirm_ip_token", 5, 15 * 60, f"ip:{ip}:token:{token_key}"),
            RateLimitRule("password_reset_confirm_ip", 20, 15 * 60, f"ip:{ip}"),
        ],
    )


def rate_limit_export(request: Request, db: Session = Depends(get_db)) -> None:
    actor_key = _request_actor_key(request, db, required=False)
    _check("export", [RateLimitRule("export_actor", 10, 10 * 60, actor_key)])


def rate_limit_admin_export(request: Request, db: Session = Depends(get_db)) -> None:
    actor_key = _admin_actor_key(request, db)
    _check("admin_export", [RateLimitRule("admin_export_actor", 20, 10 * 60, actor_key)])


def rate_limit_provider_backed(request: Request, db: Session = Depends(get_db)) -> None:
    actor_key = _request_actor_key(request, db, required=False)
    _check("provider_backed", [RateLimitRule("provider_backed_actor", 60, 60, actor_key)])


def rate_limit_backtest_run(request: Request, db: Session = Depends(get_db)) -> None:
    actor_key = _request_actor_key(request, db, required=True)
    _check("backtest_run", [RateLimitRule("backtest_run_user", 10, 10 * 60, actor_key)])


def rate_limit_notification_mutation(request: Request, db: Session = Depends(get_db)) -> None:
    actor_key = _request_actor_key(request, db, required=True)
    _check(
        "notification_mutation",
        [RateLimitRule("notification_mutation_user", 30, 60, actor_key)],
    )


def rate_limit_admin_digest_run(request: Request, db: Session = Depends(get_db)) -> None:
    actor_key = _admin_actor_key(request, db)
    ip = _client_ip(request)
    _check(
        "admin_digest_run",
        [RateLimitRule("admin_digest_run_admin_ip", 5, 60 * 60, f"{actor_key}:ip:{ip}")],
    )


def rate_limit_admin_mutation(request: Request, db: Session = Depends(get_db)) -> None:
    actor_key = _admin_actor_key(request, db)
    _check("admin_mutation", [RateLimitRule("admin_mutation_user", 30, 60, actor_key)])
