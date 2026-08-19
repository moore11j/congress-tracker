from __future__ import annotations

import json
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import current_user
from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.models import Security, UserAccount, Watchlist, WatchlistAlertRule, WatchlistAlertRuleState, WatchlistItem
from app.rate_limit import rate_limit_notification_mutation
from app.services.custom_alert_rules import (
    DELIVERIES,
    MATCH_TYPES,
    SCOPE_TYPES,
    format_rule_summary,
    metric_registry_payload,
    rule_payload,
    validate_conditions,
)

router = APIRouter(tags=["custom-alert-rules"])


class RuleScopePayload(BaseModel):
    type: Literal["any_watchlist_ticker", "specific_ticker", "watchlist_aggregate"] = "any_watchlist_ticker"
    ticker: str | None = Field(default=None, max_length=16)


class RulePayload(BaseModel):
    name: str | None = Field(default=None, max_length=120)
    enabled: bool = True
    scope: RuleScopePayload = RuleScopePayload()
    match_type: Literal["all", "any"] = "all"
    conditions: list[dict[str, Any]] = Field(min_length=1, max_length=10)
    delivery: Literal["immediate", "daily", "both"] = "immediate"


class RulePatchPayload(BaseModel):
    name: str | None = Field(default=None, max_length=120)
    enabled: bool | None = None
    scope: RuleScopePayload | None = None
    match_type: Literal["all", "any"] | None = None
    conditions: list[dict[str, Any]] | None = Field(default=None, min_length=1, max_length=10)
    delivery: Literal["immediate", "daily", "both"] | None = None


class RuleValidationPayload(BaseModel):
    conditions: list[dict[str, Any]] = Field(min_length=1, max_length=10)
    match_type: Literal["all", "any"] = "all"


def _watchlist(db: Session, user: UserAccount, watchlist_id: int) -> Watchlist:
    watchlist = db.execute(select(Watchlist).where(Watchlist.id == watchlist_id, Watchlist.owner_user_id == user.id)).scalar_one_or_none()
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found.")
    return watchlist


def _rule(db: Session, user: UserAccount, watchlist_id: int, rule_id: int) -> WatchlistAlertRule:
    rule = db.execute(select(WatchlistAlertRule).where(WatchlistAlertRule.id == rule_id, WatchlistAlertRule.user_id == user.id, WatchlistAlertRule.watchlist_id == watchlist_id)).scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Custom alert rule not found.")
    return rule


def _symbols(db: Session, watchlist_id: int) -> set[str]:
    return {str(symbol).upper() for symbol in db.execute(select(Security.symbol).join(WatchlistItem, WatchlistItem.security_id == Security.id).where(WatchlistItem.watchlist_id == watchlist_id, WatchlistItem.target_type == "ticker")).scalars().all() if symbol}


def _apply_payload(rule: WatchlistAlertRule, payload: RulePayload | RulePatchPayload, symbols: set[str]) -> None:
    scope = payload.scope if payload.scope is not None else RuleScopePayload(type=rule.scope_type, ticker=rule.scope_ticker)
    if scope.type not in SCOPE_TYPES:
        raise HTTPException(status_code=422, detail="Unsupported custom alert scope.")
    ticker = scope.ticker.strip().upper() if scope.ticker else None
    if scope.type == "specific_ticker" and (not ticker or ticker not in symbols):
        raise HTTPException(status_code=422, detail="The selected ticker must be in this watchlist.")
    conditions = validate_conditions(payload.conditions if payload.conditions is not None else json.loads(rule.conditions_json))
    match_type = payload.match_type if payload.match_type is not None else rule.match_type
    delivery = payload.delivery if payload.delivery is not None else rule.delivery
    if match_type not in MATCH_TYPES or delivery not in DELIVERIES:
        raise HTTPException(status_code=422, detail="Unsupported custom alert rule configuration.")
    rule.scope_type, rule.scope_ticker = scope.type, ticker
    rule.match_type, rule.delivery, rule.conditions_json = match_type, delivery, json.dumps(conditions)
    generated_name = format_rule_summary({"conditions": conditions, "match_type": match_type})[:120]
    if payload.name is not None:
        rule.name = payload.name.strip() or generated_name
    elif not rule.name:
        rule.name = generated_name
    if payload.enabled is not None:
        rule.enabled = payload.enabled


def _require_pro(request: Request, db: Session) -> None:
    require_feature(current_entitlements(request, db), "custom_alert_rules", message="Custom Alert Rules are included with Walnut Pro.")


@router.get("/watchlists/{watchlist_id}/alert-rules")
def list_rules(watchlist_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    _watchlist(db, user, watchlist_id)
    rules = db.execute(select(WatchlistAlertRule).where(WatchlistAlertRule.user_id == user.id, WatchlistAlertRule.watchlist_id == watchlist_id).order_by(WatchlistAlertRule.updated_at.desc(), WatchlistAlertRule.id.desc())).scalars().all()
    return {"items": [rule_payload(rule) for rule in rules], "metrics": metric_registry_payload()}


@router.get("/watchlists/{watchlist_id}/alert-rules/metrics")
def rule_metrics(watchlist_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    _watchlist(db, user, watchlist_id)
    return {"items": metric_registry_payload()}


@router.post("/watchlists/{watchlist_id}/alert-rules/validate", dependencies=[Depends(rate_limit_notification_mutation)])
def validate_rule(watchlist_id: int, payload: RuleValidationPayload, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    _require_pro(request, db)
    _watchlist(db, user, watchlist_id)
    conditions = validate_conditions(payload.conditions)
    return {"valid": True, "conditions": conditions, "summary": format_rule_summary({"conditions": conditions, "match_type": payload.match_type})}


@router.get("/watchlists/{watchlist_id}/alert-rules/{rule_id}")
def get_rule(watchlist_id: int, rule_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    _watchlist(db, user, watchlist_id)
    return rule_payload(_rule(db, user, watchlist_id, rule_id))


@router.post("/watchlists/{watchlist_id}/alert-rules", dependencies=[Depends(rate_limit_notification_mutation)])
def create_rule(watchlist_id: int, payload: RulePayload, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    _require_pro(request, db)
    _watchlist(db, user, watchlist_id)
    rule = WatchlistAlertRule(user_id=user.id, watchlist_id=watchlist_id, name="Custom alert", enabled=payload.enabled)
    _apply_payload(rule, payload, _symbols(db, watchlist_id))
    db.add(rule)
    db.commit(); db.refresh(rule)
    return rule_payload(rule)


@router.patch("/watchlists/{watchlist_id}/alert-rules/{rule_id}", dependencies=[Depends(rate_limit_notification_mutation)])
def update_rule(watchlist_id: int, rule_id: int, payload: RulePatchPayload, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    _require_pro(request, db)
    _watchlist(db, user, watchlist_id)
    rule = _rule(db, user, watchlist_id, rule_id)
    logic_changed = payload.conditions is not None or payload.scope is not None or payload.match_type is not None
    _apply_payload(rule, payload, _symbols(db, watchlist_id))
    if logic_changed:
        for state in db.execute(select(WatchlistAlertRuleState).where(WatchlistAlertRuleState.rule_id == rule.id)).scalars().all(): db.delete(state)
        rule.last_triggered_at, rule.last_triggered_ticker = None, None
    db.commit(); db.refresh(rule)
    return rule_payload(rule)


@router.post("/watchlists/{watchlist_id}/alert-rules/{rule_id}/duplicate", dependencies=[Depends(rate_limit_notification_mutation)])
def duplicate_rule(watchlist_id: int, rule_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    _require_pro(request, db)
    _watchlist(db, user, watchlist_id)
    source = _rule(db, user, watchlist_id, rule_id)
    copy = WatchlistAlertRule(user_id=user.id, watchlist_id=watchlist_id, name=f"{source.name} copy"[:120], enabled=False, scope_type=source.scope_type, scope_ticker=source.scope_ticker, match_type=source.match_type, conditions_json=source.conditions_json, delivery=source.delivery)
    db.add(copy); db.commit(); db.refresh(copy)
    return rule_payload(copy)


@router.delete("/watchlists/{watchlist_id}/alert-rules/{rule_id}", status_code=204, dependencies=[Depends(rate_limit_notification_mutation)])
def delete_rule(watchlist_id: int, rule_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    _require_pro(request, db)
    _watchlist(db, user, watchlist_id)
    rule = _rule(db, user, watchlist_id, rule_id)
    db.delete(rule); db.commit()
    return None
