"""Admin-only video workflow inside AI Growth. No publishing endpoints."""
from __future__ import annotations

import importlib.util
import json
import os
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import Field
from sqlalchemy import text

from app.auth import require_admin_user
from app.db import get_db
from app.rate_limit import rate_limit_admin_mutation
from app.services import growth_video_store as store
from app.services.growth_video_domain import BRIEF_FIELDS, COPY, FORMATS, Strict, Storyboard, validate_storyboard
from app.services.growth_video_media import AssetStore
from app.services.growth_video_pipeline import RUNNABLE, validate_job
from app.services.ai_marketing import OPENAI_API_KEY, resolved_setting_value


def admin(request: Request, db=Depends(get_db)):
    user = require_admin_user(db, request)
    store.ensure_schema(db)
    return user


router = APIRouter(prefix="/admin/ai-growth/video", tags=["admin-ai-growth-video"], dependencies=[Depends(admin)])
MUTATION = [Depends(rate_limit_admin_mutation)]


class Generate(Strict):
    opportunity_id: str = Field(max_length=100)
    platform: Literal["tiktok", "instagram"] = "instagram"
    format: Literal["research_finding", "product_investigation", "search_explanation"] = "research_finding"


class Decision(Strict):
    action: Literal["render", "approve", "reject", "regenerate", "edit", "retry"]
    feedback: str = Field(default="", max_length=1500)
    storyboard: Storyboard | None = None
    acknowledge_provider_retry: bool = False


class BriefEdit(Strict):
    sections: dict[str, str]


class ConfigEdit(Strict):
    config: dict


class MemoryEdit(Strict):
    feedback: str = Field(max_length=1500)
    active: bool = True


def safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None


def readiness(db):
    cfg = store.config(db)
    return {"openai": bool(resolved_setting_value(db, OPENAI_API_KEY)),
            "creatomate": bool(os.getenv("CREATOMATE_API_KEY")),
            "narration": bool(os.getenv("ELEVENLABS_API_KEY") and cfg["voice"]),
            "asset_bucket": bool(os.getenv("GROWTH_ASSET_BUCKET")),
            "asset_sdk": bool(importlib.util.find_spec("boto3")),
            "worker_enabled": os.getenv("GROWTH_VIDEO_WORKER_ENABLED", "").lower() == "true",
            "note": "Configuration presence only. Worker, bucket access and provider access are verified by running a draft."}


@router.get("")
def state(db=Depends(get_db)):
    opportunities = [json.loads(r[0]) for r in db.execute(text("SELECT payload_json FROM growth_content_opportunities ORDER BY score DESC,created_at DESC LIMIT 50"))]
    jobs = []
    for row in db.execute(text("SELECT id FROM growth_video_jobs ORDER BY created_at DESC LIMIT 50")):
        item = store.job(db, row[0])
        item.pop("lease_token", None)
        jobs.append(item)
    memory = []
    for r in db.execute(text("SELECT * FROM growth_memory ORDER BY created_at DESC LIMIT 50")).mappings():
        memory.append({**{k: v for k, v in r.items() if k != "payload_json"}, "payload": json.loads(r["payload_json"])})
    versions = [dict(r) for r in db.execute(text("SELECT id,created_at,actor_id FROM growth_brief_versions ORDER BY created_at DESC LIMIT 20")).mappings()]
    return {"opportunities": opportunities, "jobs": jobs, "memory": memory, "brief": store.brief(db),
            "brief_fields": BRIEF_FIELDS, "brief_versions": versions, "config": store.config(db), "readiness": readiness(db), "copy_library": COPY, "formats": FORMATS}


@router.get("/opportunities/{opportunity_id}")
def detail(opportunity_id: str, db=Depends(get_db)):
    return {"opportunity": store.opportunity(db, opportunity_id), "evidence": store.evidence_for(db, opportunity_id, verify=False)}


@router.post("/discover", dependencies=MUTATION)
def discover(db=Depends(get_db)):
    return safe_call(store.discover, db)


@router.put("/brief", dependencies=MUTATION)
def brief(payload: BriefEdit, user=Depends(admin), db=Depends(get_db)):
    return safe_call(store.save_brief, db, user.id, payload.sections)


@router.get("/brief/{version_id}")
def brief_version(version_id: str, db=Depends(get_db)):
    row = db.execute(text("SELECT payload_json FROM growth_brief_versions WHERE id=:id"), {"id": version_id}).first()
    if not row:
        raise HTTPException(404, "Growth Brief version not found.")
    return json.loads(row[0])


@router.put("/config", dependencies=MUTATION)
def configure(payload: ConfigEdit, db=Depends(get_db)):
    return safe_call(store.save_config, db, payload.config)


@router.put("/memory/{memory_id}", dependencies=MUTATION)
def memory_edit(memory_id: str, payload: MemoryEdit, user=Depends(admin), db=Depends(get_db)):
    row = db.execute(text("SELECT payload_json FROM growth_memory WHERE id=:id"), {"id": memory_id}).first()
    if not row:
        raise HTTPException(404, "Growth Memory entry not found.")
    item = json.loads(row[0])
    item.setdefault("corrections", []).append({"previous_feedback": item.get("feedback"), "actor_id": user.id, "at": store.now()})
    item["feedback"] = payload.feedback
    db.execute(text("UPDATE growth_memory SET active=:active,payload_json=:payload WHERE id=:id"),
               {"active": int(payload.active), "payload": store.dumps(item), "id": memory_id})
    db.commit()
    return {"ok": True}


@router.post("/jobs", dependencies=MUTATION)
def generate(payload: Generate, user=Depends(admin), db=Depends(get_db)):
    return safe_call(store.create_job, db, payload.opportunity_id, user.id, payload.platform, payload.format)


@router.post("/jobs/{job_id}/decision", dependencies=MUTATION)
def decision(job_id: str, payload: Decision, user=Depends(admin), db=Depends(get_db)):
    item = store.job(db, job_id)
    if item["lease_token"]:
        raise HTTPException(409, "This draft is processing. Wait for its current stage to finish.")
    action = payload.action
    if action in {"edit", "regenerate"}:
        if item["status"] in RUNNABLE or item["status"] in {"CREATIVE_GENERATING", "CAPTURING"}:
            raise HTTPException(409, "Wait for the active draft to finish before creating a revision.")
        raw = payload.storyboard.model_dump() if payload.storyboard else None
        if action == "edit":
            if raw is None:
                raise HTTPException(422, "An edited storyboard is required.")
            safe_call(validate_storyboard, raw, store.opportunity(db, item["opportunity_id"]), safe_call(store.evidence_for, db, item["opportunity_id"]))
        child = safe_call(store.create_job, db, item["opportunity_id"], user.id, item["payload"]["platform"],
            item["payload"]["format"], parent=item, feedback=payload.feedback, storyboard=raw)
        store.remember(db, item, user.id, action, payload.feedback, revised=raw)
        return child
    if action == "render":
        if item["status"] != "CREATIVE_READY":
            raise HTTPException(409, "A validated creative must be ready before capture/render.")
        safe_call(validate_job, db, item)
        item["status"] = "CAPTURE_PENDING"
    elif action == "approve":
        if item["status"] != "READY_FOR_REVIEW" or not item["payload"].get("video"):
            raise HTTPException(409, "Only a completed rendered video can be approved.")
        safe_call(validate_job, db, item)
        item["status"] = "APPROVED"
        item["payload"]["approval"] = {"actor_id": user.id, "at": store.now(), "asset_hash": item["payload"]["video"]["sha256"]}
    elif action == "reject":
        if item["status"] not in {"CREATIVE_READY", "READY_FOR_REVIEW", "APPROVED", "FAILED"}:
            raise HTTPException(409, "Only a completed or failed draft can be rejected.")
        if not payload.feedback.strip():
            raise HTTPException(422, "Add a reason so future drafts can use your feedback.")
        item["status"] = "REJECTED"
    elif action == "retry":
        if item["status"] != "FAILED":
            raise HTTPException(409, "Only failed jobs can be retried.")
        if not payload.acknowledge_provider_retry:
            raise HTTPException(422, "A retry can repeat an uncertain paid request. Check the provider dashboard and acknowledge before retrying.")
        stage = item["payload"].get("failed_stage")
        if stage not in RUNNABLE:
            raise HTTPException(409, "This stage cannot be retried; create a new revision.")
        if stage == "RENDERING" and item["payload"].get("render_provider_failed"):
            stage = "RENDER_PENDING"
            item["payload"].pop("render_id", None)
        item["status"] = stage
        item["payload"]["retry_count"] += 1
    safe_call(store.save_job, db, item)
    store.remember(db, item, user.id, action, payload.feedback)
    return item


@router.get("/jobs/{job_id}/media")
def media(job_id: str, download: bool = False, db=Depends(get_db)):
    item = store.job(db, job_id)
    if download and item["status"] != "APPROVED":
        raise HTTPException(409, "Approve the finished video before downloading it.")
    if download:
        safe_call(validate_job, db, item)
    assets = item["payload"]
    try:
        storage = AssetStore()
        video = assets.get("video") or (assets.get("preview_video") if not download else None)
        return {"video_url": storage.url(video, download=download) if video else None,
                "thumbnail_url": storage.url(assets["thumbnail"]) if assets.get("thumbnail") else None}
    except (ValueError, ImportError):
        raise HTTPException(503, "Private asset storage is not configured on this server.") from None
