from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import require_admin_user
from app.db import get_db
from app.rate_limit import rate_limit_admin_mutation
from app.services.data_sources_status import build_data_sources_status, enqueue_admin_data_source_run
from app.services.provider_settings import cleanup_invalid_provider_settings, provider_setting_payload, update_provider_setting

router = APIRouter(tags=["admin-data-sources"])


class ProviderSettingPatchPayload(BaseModel):
    active_provider: str | None = Field(default=None, max_length=80)
    fallback_provider: str | None = Field(default=None, max_length=80)
    mode: str | None = Field(default=None, max_length=40)
    is_enabled: bool | None = None
    allow_external_live_fetch: bool | None = None
    allow_user_route_sync_fetch: bool | None = None
    builder_safe_required: bool | None = None
    notes: str | None = Field(default=None, max_length=1000)
    reason: str | None = Field(default=None, max_length=1000)


class DataSourceRunPayload(BaseModel):
    mode: str | None = Field(default="dry_run", max_length=40)
    reason: str | None = Field(default=None, max_length=1000)


def _payload_changes(payload: ProviderSettingPatchPayload) -> dict[str, Any]:
    if hasattr(payload, "model_dump"):
        changes = payload.model_dump(exclude_unset=True)
    else:
        changes = payload.dict(exclude_unset=True)
    changes.pop("reason", None)
    return changes


@router.get("/admin/data-sources/status")
def admin_data_sources_status(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    cleanup_invalid_provider_settings(db)
    db.commit()
    return build_data_sources_status(db)


@router.patch("/admin/data-sources/settings/{domain_key}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_update_data_source_setting(
    domain_key: str,
    payload: ProviderSettingPatchPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    try:
        setting = update_provider_setting(
            db,
            domain_key=domain_key,
            changes=_payload_changes(payload),
            changed_by=admin.email,
            reason=payload.reason,
        )
        db.commit()
    except KeyError as exc:
        db.rollback()
        raise HTTPException(status_code=404, detail="Unknown data source domain.") from exc
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return provider_setting_payload(setting)


@router.post("/admin/data-sources/run/{domain_key}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_run_data_source(
    domain_key: str,
    payload: DataSourceRunPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    try:
        result = enqueue_admin_data_source_run(
            db,
            domain_key=domain_key,
            mode=(payload.mode or "dry_run").strip().lower(),
            requested_by=admin.email,
        )
        db.commit()
    except KeyError as exc:
        db.rollback()
        raise HTTPException(status_code=404, detail="Unknown data source domain.") from exc
    return result
