"""One new published brief per Pacific day; durable review notifications."""
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text

from app.models import UserAccount
from app.services import growth_video_store as store
from app.services.growth_daily_video import create_job, creative

KEY = "GROWTH_VIDEO_AUTOMATION"


def ensure_schema(db):
    from app.services.growth_buffer import ensure_schema as publishing_schema
    store.ensure_schema(db)
    publishing_schema(db)
    db.execute(text("""CREATE TABLE IF NOT EXISTS growth_video_daily_runs (
      day TEXT PRIMARY KEY, brief_id TEXT UNIQUE NOT NULL, status TEXT NOT NULL,
      job_id TEXT, created_at TEXT NOT NULL, error TEXT)"""))
    db.commit()


def config(db):
    return {"enabled": False, "owner_id": None, "enabled_at": None, **store.setting(db, KEY, {})}


def configure(db, actor, enabled):
    cfg = config(db)
    if enabled and not cfg["enabled"]:
        cfg.update(enabled_at=store.now(), owner_id=actor)
    cfg["enabled"] = enabled
    store.set_setting(db, KEY, cfg)
    db.commit()
    return cfg


def state(db):
    ensure_schema(db)
    import os
    from app.services.email_delivery import email_delivery_enabled
    from app.services.growth_buffer import publications, CHANNELS
    return {"config": config(db), "buffer_key_configured": bool(os.getenv("BUFFER_API_KEY")),
            "last_pass": store.setting(db, "GROWTH_VIDEO_LAST_PASS", {}),
            "email_enabled": email_delivery_enabled(), "channels": CHANNELS,
            "runs": [dict(r) for r in db.execute(text("SELECT * FROM growth_video_daily_runs ORDER BY day DESC LIMIT 10")).mappings()],
            "publications": publications(db)}


def create_daily(db):
    ensure_schema(db)
    cfg = config(db)
    if not cfg["enabled"]:
        return {"status": "disabled"}
    owner = db.get(UserAccount, cfg["owner_id"])
    if not owner or owner.role != "admin" or owner.deleted_at or owner.is_suspended:
        return {"status": "blocked", "reason": "An active administrator must enable the daily workflow."}
    day = datetime.now(ZoneInfo("America/Los_Angeles")).date().isoformat()
    if db.execute(text("SELECT day FROM growth_video_daily_runs WHERE day=:day"), {"day": day}).first():
        return {"status": "already_created"}
    from app.services.research_briefs import ensure_research_brief_store_schema
    ensure_research_brief_store_schema(db)
    cutoff = max(cfg["enabled_at"], (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat())
    rows = db.execute(text("SELECT payload_json FROM research_brief_drafts WHERE status='published' ORDER BY updated_at DESC LIMIT 20")).all()
    selected = None
    skipped = []
    for row in rows:
        source = json.loads(row[0])
        if (source.get("published_at") or source.get("created_at") or "") < cutoff:
            continue
        if db.execute(text("SELECT day FROM growth_video_daily_runs WHERE brief_id=:id"), {"id": source["id"]}).first():
            continue
        try:
            creative(source)
        except ValueError as exc:
            skipped.append({"brief_id": source["id"], "reason": str(exc)})
            continue
        selected = source
        break
    if not selected:
        return {"status": "waiting_for_published_brief", "skipped": skipped}
    claim = db.execute(text("""INSERT INTO growth_video_daily_runs(day,brief_id,status,created_at)
       VALUES (:day,:id,'CREATING',:at) ON CONFLICT DO NOTHING"""),
       {"day": day, "id": selected["id"], "at": store.now()})
    db.commit()
    if not claim.rowcount:
        return {"status": "already_claimed"}
    try:
        item = create_job(db, selected, owner.id)
        db.execute(text("UPDATE growth_video_daily_runs SET job_id=:job,status='CREATED' WHERE day=:day"), {"day": day, "job": item["id"]})
        db.commit()
        return {"status": "created", "job_id": item["id"]}
    except Exception:
        db.rollback()
        db.execute(text("UPDATE growth_video_daily_runs SET status='FAILED',error='Creation interrupted. Inspect the video queue before retrying.' WHERE day=:day"), {"day": day})
        db.commit()
        return {"status": "failed"}


def notify_ready(db, *, sender=None):
    from app.services.email_delivery import send_email, email_delivery_enabled, _provider_api_key, _provider_name
    from app.services.ai_marketing import ai_growth_recipient
    if sender is None and (not email_delivery_enabled() or not _provider_api_key(_provider_name())):
        return [{"notification": "email_not_configured"}]
    sender = sender or send_email
    rows = db.execute(text("SELECT id FROM growth_video_jobs WHERE status IN ('READY_FOR_REVIEW','FAILED') ORDER BY created_at DESC LIMIT 30")).all()
    output = []
    for row in rows:
        item = store.job(db, row[0])
        if not item["payload"].get("daily_automation") or item["lease_token"]:
            continue
        event = "ready" if item["status"] == "READY_FOR_REVIEW" else "failed"
        if item["payload"].get("notifications", {}).get(event):
            continue
        try:
            result = sender(db, to_email=ai_growth_recipient(), template_key="growth.video_review",
                category="admin", idempotency_key=f"growth-video:{item['id']}:{event}",
                context={"video_title": item["payload"]["opportunity"]["topic"],
                         "video_status": "Ready for your review" if event == "ready" else "Video needs attention",
                         "video_message": "Watch the finished video, review its caption and source, then choose Approve and publish. Nothing is posted before your approval." if event == "ready" else item["payload"].get("failure_reason", "Open the queue to inspect the failed stage."),
                         "activity_url": f"https://app.walnutmarkets.com/admin/ai-marketing?growth_tab=drafts&video={item['id']}"})
            # Log-only/disabled email is not delivery success. Leave it visible
            # and pending so configuration problems never look like sent email.
            if result.get("status") == "sent":
                item = store.job(db, row[0])
                item["payload"].setdefault("notifications", {})[event] = store.now()
                store.save_job(db, item)
            output.append({"id": item["id"], "notification": result.get("status")})
        except Exception:
            db.rollback()
            output.append({"id": item["id"], "notification": "failed"})
    return output


def run_once(db):
    from app.services.growth_video_pipeline import run_pending
    from app.services.growth_buffer import run_pending as publish_pending
    # Hand approved posts to Buffer before capture/render work can occupy this
    # shared worker. Once confirmed, Buffer owns the selected publishing time.
    published = publish_pending(db, limit=2)
    daily = create_daily(db)
    store.set_setting(db, "GROWTH_VIDEO_LAST_PASS", {"at": store.now(), **daily})
    db.commit()
    rendered = run_pending(db, limit=2)
    notices = notify_ready(db)
    return {"daily": daily, "video_jobs": rendered, "notifications": notices, "publications": published}
