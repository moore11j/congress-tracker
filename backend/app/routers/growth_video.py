"""Admin video review and publishing, plus an approved-asset delivery route."""
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
from app.services import growth_video_automation as automation, growth_buffer as buffer


def admin(request: Request, db=Depends(get_db)):
    user = require_admin_user(db, request)
    store.ensure_schema(db)
    automation.ensure_schema(db)
    return user


router = APIRouter(prefix="/admin/ai-growth/video", tags=["admin-ai-growth-video"], dependencies=[Depends(admin)])
MUTATION = [Depends(rate_limit_admin_mutation)]


class Generate(Strict):
    opportunity_id: str = Field(max_length=100)
    platform: Literal["tiktok", "instagram"] = "instagram"
    format: Literal["research_finding", "product_investigation", "search_explanation"] = "research_finding"


class ProductAd(Strict):
    platform: Literal["tiktok", "instagram"] = "instagram"
    hook: Literal["opinion", "score", "accountability", "ownership", "navigation"] = "navigation"


class Decision(Strict):
    action: Literal["render", "approve", "reject", "regenerate", "edit", "retry"]
    feedback: str = Field(default="", max_length=1500)
    storyboard: Storyboard | None = None
    acknowledge_provider_retry: bool = False


class BriefEdit(Strict):
    sections: dict[str, str]


class ConfigEdit(Strict):
    config: dict


class AutomationEdit(Strict):
    enabled: bool


class Publish(Strict):
    platforms: list[Literal["instagram", "tiktok"]] = Field(min_length=1, max_length=2)
    caption: str = Field(min_length=1, max_length=2200)
    reviewed_video_and_caption: bool = False
    confirm_buffer_channel_settings: bool = False
    scheduled_at: str | None = Field(default=None, max_length=50)
    schedule_timezone: str | None = Field(default=None, max_length=100)


class Reconcile(Strict):
    platform: Literal["instagram", "tiktok"]
    post_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,100}$")


class RetryPublish(Strict):
    platform: Literal["instagram", "tiktok"]
    confirmed_no_buffer_post: bool = False
    scheduled_at: str | None = Field(default=None, max_length=50)
    schedule_timezone: str | None = Field(default=None, max_length=100)


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
    for row in db.execute(text("SELECT id FROM growth_video_jobs WHERE status <> 'DELETED' ORDER BY created_at DESC LIMIT 50")):
        item = store.job(db, row[0])
        item.pop("lease_token", None)
        jobs.append(item)
    memory = []
    for r in db.execute(text("SELECT * FROM growth_memory ORDER BY created_at DESC LIMIT 50")).mappings():
        memory.append({**{k: v for k, v in r.items() if k != "payload_json"}, "payload": json.loads(r["payload_json"])})
    versions = [dict(r) for r in db.execute(text("SELECT id,created_at,actor_id FROM growth_brief_versions ORDER BY created_at DESC LIMIT 20")).mappings()]
    deleted_jobs = []
    for row in db.execute(text("SELECT id FROM growth_video_jobs WHERE status = 'DELETED' ORDER BY updated_at DESC LIMIT 50")):
        item = store.job(db, row[0])
        item.pop("lease_token", None)
        deleted_jobs.append(item)
    return {"opportunities": opportunities, "jobs": jobs, "deleted_jobs": deleted_jobs, "memory": memory, "brief": store.brief(db),
            "automation": automation.state(db), "development_override": store.development_draft_override(db),
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


@router.put("/automation", dependencies=MUTATION)
def configure_automation(payload: AutomationEdit, user=Depends(admin), db=Depends(get_db)):
    return safe_call(automation.configure, db, user.id, payload.enabled)


@router.get("/buffer-status")
def buffer_status(db=Depends(get_db)):
    client = safe_call(buffer.BufferClient, db)
    return {"channels": [safe_call(client.channel, platform) for platform in buffer.CHANNELS]}


@router.post("/jobs/{job_id}/publish", dependencies=MUTATION)
def publish(job_id: str, payload: Publish, user=Depends(admin), db=Depends(get_db)):
    if not payload.reviewed_video_and_caption or not payload.confirm_buffer_channel_settings:
        raise HTTPException(422, "Review the finished video, caption and Buffer channel publishing settings before approval.")
    item = store.job(db, job_id)
    result = safe_call(buffer.approve_publish, db, item, user.id, payload.platforms, payload.caption,
                       scheduled_at=payload.scheduled_at, schedule_timezone=payload.schedule_timezone)
    store.remember(db, item, user.id, "approve", "Approved video and caption for Buffer scheduling." if payload.scheduled_at else "Approved video and caption for Buffer publishing.")
    return result


@router.delete("/jobs/{job_id}", dependencies=MUTATION)
def delete_video(job_id: str, user=Depends(admin), db=Depends(get_db)):
    return safe_call(store.trash_job, db, job_id, user.id)


@router.post("/jobs/{job_id}/restore", dependencies=MUTATION)
def restore_video(job_id: str, user=Depends(admin), db=Depends(get_db)):
    return safe_call(store.restore_job, db, job_id, user.id)


@router.post("/jobs/{job_id}/reconcile", dependencies=MUTATION)
def reconcile(job_id: str, payload: Reconcile, db=Depends(get_db)):
    row = next((p for p in buffer.publications(db, job_id) if p["platform"] == payload.platform), None)
    if not row or row["status"] not in {"UNCERTAIN", "FAILED"}:
        raise HTTPException(409, "Only an uncertain or failed submission can be reconciled.")
    client = safe_call(buffer.BufferClient, db)
    post = safe_call(client.post, payload.post_id)
    full = safe_call(client.query, "query($input:PostInput!){post(input:$input){text}}", {"input": {"id": payload.post_id}})["post"]
    if full["text"] != row["caption"]:
        raise HTTPException(422, "This Buffer post's caption does not match the approved caption.")
    if post["channelId"] != buffer.CHANNELS[payload.platform]:
        raise HTTPException(422, "This post belongs to a different channel.")
    buffer.update(db, row, buffer.post_status(post), post=post)
    return buffer.publications(db, job_id)


@router.post("/jobs/{job_id}/retry-publish", dependencies=MUTATION)
def retry_publish(job_id: str, payload: RetryPublish, db=Depends(get_db)):
    if not payload.confirmed_no_buffer_post:
        raise HTTPException(422, "Check the Buffer queue and sent posts, then confirm no post exists before retrying.")
    safe_call(validate_job, db, store.job(db, job_id))
    row = next((p for p in buffer.publications(db, job_id) if p["platform"] == payload.platform), None)
    if not row or row["status"] not in {"FAILED", "UNCERTAIN"} or row["post_id"]:
        raise HTTPException(409, "This post cannot be resubmitted. Manage confirmed Buffer posts in Buffer.")
    due, zone = safe_call(buffer.validate_schedule, payload.scheduled_at or row["scheduled_at"],
                         payload.schedule_timezone or row["schedule_timezone"])
    result = db.execute(text("""UPDATE growth_video_publications SET status='QUEUED',error=NULL,updated_at=:at
        WHERE job_id=:job AND platform=:platform AND status IN ('FAILED','UNCERTAIN') AND post_id IS NULL AND updated_at=:old"""),
        {"job": job_id, "platform": payload.platform, "at": store.now(), "old": row["updated_at"]})
    if not result.rowcount:
        db.rollback()
        raise HTTPException(409, "This post cannot be resubmitted. Manage confirmed Buffer posts in Buffer.")
    if due:
        db.execute(text("""INSERT INTO growth_video_publication_schedules(job_id,platform,scheduled_at,schedule_timezone)
            VALUES (:job,:platform,:due,:zone) ON CONFLICT(job_id,platform) DO UPDATE SET
            scheduled_at=:due,schedule_timezone=:zone,buffer_due_at=NULL"""),
            {"job": job_id, "platform": payload.platform, "due": due, "zone": zone})
    db.commit()
    return buffer.publications(db, job_id)


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


@router.post("/product-ad", dependencies=MUTATION)
def product_ad(payload: ProductAd, user=Depends(admin), db=Depends(get_db)):
    from app.services.growth_product_ad import create_job
    return safe_call(create_job,db,user.id,payload.platform,payload.hook)


@router.post("/jobs/{job_id}/decision", dependencies=MUTATION)
def decision(job_id: str, payload: Decision, user=Depends(admin), db=Depends(get_db)):
    buffer.ensure_schema(db)
    item = store.job(db, job_id)
    if item["status"] == "DELETED":
        raise HTTPException(409, "Restore this video from Trash before editing or publishing it.")
    if item["lease_token"]:
        raise HTTPException(409, "This draft is processing. Wait for its current stage to finish.")
    action = payload.action
    if buffer.publications(db, job_id):
        raise HTTPException(409, "Publishing has started. Manage or cancel the post in Buffer; this approved asset is retained for delivery.")
    if action in {"edit", "regenerate"}:
        if item["status"] in RUNNABLE or item["status"] in {"CREATIVE_GENERATING", "CAPTURING"}:
            raise HTTPException(409, "Wait for the active draft to finish before creating a revision.")
        if item["payload"].get("campaign_id"):
            if action=="edit":
                raise HTTPException(422,"Product campaigns use reviewed scripts. Choose a hook in Content Opportunities to create another direction.")
            if item["payload"]["campaign_id"] == "daily_research_v1":
                from app.services.growth_daily_video import create_job
                source = safe_call(store.research_source, db, item["payload"]["research_source"]["id"])
                child = safe_call(create_job, db, source, user.id, parent=item, feedback=payload.feedback)
                store.remember(db, item, user.id, action, payload.feedback)
                return child
            from app.services.growth_product_ad import create_job
            child=safe_call(create_job,db,user.id,item["payload"]["platform"],item["payload"]["product_hook"],parent=item,feedback=payload.feedback)
            store.remember(db,item,user.id,action,payload.feedback)
            return child
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
        item["payload"].setdefault("retry_history", []).append({
            "at": store.now(), "actor_id": user.id, "stage": stage,
            "failure_reason": item["payload"].get("failure_reason"),
            "failure_context": item["payload"].get("failure_context"),
        })
        item["status"] = stage
        item["payload"]["retry_count"] = item["payload"].get("retry_count", 0) + 1
        item["payload"].pop("failure_reason", None)
        item["payload"].pop("failed_stage", None)
        item["payload"].pop("failure_context", None)
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


# Buffer fetches only the final, explicitly approved video. Captures, audio and
# thumbnails remain private. The bucket itself is never made public.
public_router = APIRouter(tags=["growth-video-delivery"])


@public_router.api_route("/growth-video-media/{token}", methods=["GET", "HEAD"])
def public_media(token: str, request: Request, db=Depends(get_db)):
    import re
    from fastapi.responses import Response, StreamingResponse
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}", token):
        raise HTTPException(404, "Media unavailable.")
    try:
        asset = buffer.media_asset(db, token)
    except ValueError:
        raise HTTPException(404, "Media unavailable.") from None
    storage = AssetStore()
    size = asset["bytes"]
    headers = {"Accept-Ranges": "bytes", "Content-Type": "video/mp4", "Cache-Control": "private, no-store",
               "X-Robots-Tag": "noindex, nofollow", "Content-Disposition": 'inline; filename="walnut-video.mp4"'}
    start, end, status = 0, size - 1, 200
    value = request.headers.get("range")
    if value:
        match = re.fullmatch(r"bytes=(\d*)-(\d*)", value)
        if not match or not any(match.groups()):
            raise HTTPException(416, "Invalid range.", headers={"Content-Range": f"bytes */{size}"})
        left, right = match.groups()
        start = int(left) if left else max(0, size - int(right))
        end = min(int(right), size - 1) if left and right else size - 1
        if start > end or start >= size:
            raise HTTPException(416, "Invalid range.", headers={"Content-Range": f"bytes */{size}"})
        status = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    headers["Content-Length"] = str(end - start + 1)
    if request.method == "HEAD":
        return Response(status_code=status, headers=headers)
    result = storage.client.get_object(Bucket=storage.bucket, Key=asset["object_key"], Range=f"bytes={start}-{end}")
    def chunks():
        try:
            yield from result["Body"].iter_chunks(chunk_size=1024 * 1024)
        finally:
            result["Body"].close()
    return StreamingResponse(chunks(), status_code=status, headers=headers, media_type="video/mp4")
