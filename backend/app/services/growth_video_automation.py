"""Published research -> durable video draft events and review notifications."""
import json
import os
from datetime import datetime, timezone

from sqlalchemy import text

from app.models import UserAccount
from app.services import growth_video_store as store
from app.services.growth_daily_video import create_job, creative

KEY = "GROWTH_VIDEO_AUTOMATION"
RECONCILE_KEY = "GROWTH_VIDEO_PUBLISH_RECONCILE"
REVIEW_EMAIL_ENV = "RESEARCH_BRIEF_REVIEW_EMAIL"
DEFAULT_REVIEW_EMAIL = "jarod@walnutmarkets.com"
EVENT_TRIGGER_NAME = "research_brief_publish_growth_video"
EVENT_FUNCTION_NAME = "enqueue_research_brief_growth_video_event"


def _install_publish_trigger(db):
    """Durably enqueue only new transitions to published; never backfill rows."""
    dialect = db.get_bind().dialect.name
    if dialect == "sqlite":
        db.execute(text(f"""CREATE TRIGGER IF NOT EXISTS {EVENT_TRIGGER_NAME}_insert
            AFTER INSERT ON research_brief_drafts
            WHEN NEW.status = 'published'
            BEGIN
              INSERT OR IGNORE INTO growth_video_brief_events
                (brief_id,status,trigger_source,published_at,created_at,updated_at,attempts)
              VALUES (NEW.id,'PENDING','research_brief_publish',NEW.published_at,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0);
            END"""))
        db.execute(text(f"""CREATE TRIGGER IF NOT EXISTS {EVENT_TRIGGER_NAME}_update
            AFTER UPDATE OF status ON research_brief_drafts
            WHEN NEW.status = 'published' AND COALESCE(OLD.status, '') <> 'published'
            BEGIN
              INSERT OR IGNORE INTO growth_video_brief_events
                (brief_id,status,trigger_source,published_at,created_at,updated_at,attempts)
              VALUES (NEW.id,'PENDING','research_brief_publish',NEW.published_at,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0);
            END"""))
        return "database_trigger"
    if dialect == "postgresql":
        db.execute(text(f"""CREATE OR REPLACE FUNCTION {EVENT_FUNCTION_NAME}() RETURNS trigger AS $$
        BEGIN
          IF NEW.status = 'published' THEN
            IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.status IS DISTINCT FROM 'published') THEN
              INSERT INTO growth_video_brief_events
                (brief_id,status,trigger_source,published_at,created_at,updated_at,attempts)
              VALUES (NEW.id,'PENDING','research_brief_publish',NEW.published_at,
                      CAST(CURRENT_TIMESTAMP AS TEXT),CAST(CURRENT_TIMESTAMP AS TEXT),0)
              ON CONFLICT (brief_id) DO NOTHING;
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql"""))
        exists = db.execute(text(
            "SELECT 1 FROM pg_trigger WHERE tgname=:name AND NOT tgisinternal"
        ), {"name": EVENT_TRIGGER_NAME}).first()
        if not exists:
            db.execute(text(f"""CREATE TRIGGER {EVENT_TRIGGER_NAME}
                AFTER INSERT OR UPDATE ON research_brief_drafts
                FOR EACH ROW EXECUTE FUNCTION {EVENT_FUNCTION_NAME}()"""))
        return "database_trigger"
    # Reconciliation remains a safe fallback for unsupported local/test dialects.
    return "reconciliation_only"


def ensure_schema(db):
    from app.services.growth_buffer import ensure_schema as publishing_schema
    from app.services.research_briefs import ensure_research_brief_store_schema

    store.ensure_schema(db)
    publishing_schema(db)
    ensure_research_brief_store_schema(db)
    # Retain the historical table so existing operator UI/state remains readable.
    db.execute(text("""CREATE TABLE IF NOT EXISTS growth_video_daily_runs (
      day TEXT PRIMARY KEY, brief_id TEXT UNIQUE NOT NULL, status TEXT NOT NULL,
      job_id TEXT, created_at TEXT NOT NULL, error TEXT)"""))
    db.execute(text("""CREATE TABLE IF NOT EXISTS growth_video_brief_events (
      brief_id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      trigger_source TEXT NOT NULL,
      published_at TEXT,
      job_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      error TEXT
    )"""))
    trigger_mode = _install_publish_trigger(db)
    db.commit()
    return trigger_mode


def config(db):
    return {"enabled": False, "owner_id": None, "enabled_at": None, **store.setting(db, KEY, {})}


def configure(db, actor, enabled):
    ensure_schema(db)
    cfg = config(db)
    if enabled and not cfg["enabled"]:
        cfg.update(enabled_at=store.now(), owner_id=actor)
        # Start recovery from enable-time, never from historical published rows.
        store.set_setting(db, RECONCILE_KEY, {"through": cfg["enabled_at"]})
    cfg["enabled"] = enabled
    store.set_setting(db, KEY, cfg)
    db.commit()
    return cfg


def state(db):
    trigger_mode = ensure_schema(db)
    from app.services.email_delivery import email_delivery_enabled
    from app.services.growth_buffer import publications, CHANNELS
    return {"config": config(db), "buffer_key_configured": bool(os.getenv("BUFFER_API_KEY")),
            "last_pass": store.setting(db, "GROWTH_VIDEO_LAST_PASS", {}),
            "email_enabled": email_delivery_enabled(), "channels": CHANNELS,
            "publish_trigger": trigger_mode,
            "brief_events": [dict(r) for r in db.execute(text(
                "SELECT * FROM growth_video_brief_events ORDER BY created_at DESC LIMIT 20"
            )).mappings()],
            "runs": [dict(r) for r in db.execute(text(
                "SELECT * FROM growth_video_daily_runs ORDER BY day DESC LIMIT 10"
            )).mappings()],
            "publications": publications(db)}


def _active_owner(db, cfg):
    owner = db.get(UserAccount, cfg.get("owner_id")) if cfg.get("owner_id") else None
    if not owner or owner.role != "admin" or owner.deleted_at or owner.is_suspended:
        return None
    return owner


def _existing_job_for_brief(db, brief_id):
    # Opportunity payloads retain the research brief ID. This is the crash-recovery
    # guard for the small window after job commit but before event acknowledgement.
    row = db.execute(text("""SELECT j.id FROM growth_video_jobs j
        JOIN growth_content_opportunities o ON o.id=j.opportunity_id
        WHERE o.payload_json LIKE :brief
        ORDER BY j.created_at DESC LIMIT 1"""), {"brief": f'%{brief_id}%'}).first()
    return row[0] if row else None


def reconcile_publish_events(db, *, limit=50):
    """Recovery scan only; the DB publish trigger is the primary event source."""
    ensure_schema(db)
    cfg = config(db)
    if not cfg["enabled"]:
        return {"status": "disabled", "enqueued": []}
    checkpoint = store.setting(db, RECONCILE_KEY, {})
    through = store.now()
    since = checkpoint.get("through")
    if not since:
        # Deployment must not turn historical published research into a backlog.
        store.set_setting(db, RECONCILE_KEY, {"through": through})
        db.commit()
        return {"status": "initialized", "enqueued": []}
    rows = db.execute(text("""SELECT id,published_at FROM research_brief_drafts
        WHERE status='published' AND published_at IS NOT NULL
          AND published_at > :since AND published_at <= :through
        ORDER BY published_at ASC LIMIT :limit"""),
        {"since": since, "through": through, "limit": limit}).all()
    enqueued = []
    for brief_id, published_at in rows:
        inserted = db.execute(text("""INSERT INTO growth_video_brief_events
            (brief_id,status,trigger_source,published_at,created_at,updated_at,attempts)
            VALUES (:id,'PENDING','reconciliation',:published,:at,:at,0)
            ON CONFLICT(brief_id) DO NOTHING"""),
            {"id": brief_id, "published": published_at, "at": store.now()})
        if inserted.rowcount:
            enqueued.append(brief_id)
    store.set_setting(db, RECONCILE_KEY, {"through": through})
    db.commit()
    return {"status": "reconciled", "enqueued": enqueued}


def process_publish_events(db, *, limit=10):
    """Create one asynchronous video job for each newly published research brief."""
    ensure_schema(db)
    cfg = config(db)
    if not cfg["enabled"]:
        return {"status": "disabled", "created": [], "skipped": [], "failed": []}
    owner = _active_owner(db, cfg)
    if not owner:
        return {"status": "blocked", "reason": "An active administrator must enable the video workflow.",
                "created": [], "skipped": [], "failed": []}
    rows = db.execute(text("""SELECT brief_id,status,attempts FROM growth_video_brief_events
        WHERE status IN ('PENDING','FAILED') AND attempts < 5
        ORDER BY created_at ASC LIMIT :limit"""), {"limit": limit}).all()
    created, skipped, failed = [], [], []
    for brief_id, _status, _attempts in rows:
        source_row = db.execute(text(
            "SELECT payload_json FROM research_brief_drafts WHERE id=:id AND status='published'"
        ), {"id": brief_id}).first()
        if not source_row:
            db.execute(text("""UPDATE growth_video_brief_events
                SET status='SKIPPED',updated_at=:at,error='Research brief is no longer published.'
                WHERE brief_id=:id"""), {"id": brief_id, "at": store.now()})
            db.commit()
            skipped.append({"brief_id": brief_id, "reason": "not_published"})
            continue
        source = json.loads(source_row[0])
        published_at = source.get("published_at") or source.get("created_at") or ""
        if cfg.get("enabled_at") and published_at and published_at < cfg["enabled_at"]:
            db.execute(text("""UPDATE growth_video_brief_events
                SET status='SKIPPED',updated_at=:at,error='Published before video automation was enabled.'
                WHERE brief_id=:id"""), {"id": brief_id, "at": store.now()})
            db.commit()
            skipped.append({"brief_id": brief_id, "reason": "pre_enable"})
            continue
        try:
            creative(source)
        except ValueError as exc:
            db.execute(text("""UPDATE growth_video_brief_events
                SET status='SKIPPED',updated_at=:at,error=:error WHERE brief_id=:id"""),
                {"id": brief_id, "at": store.now(), "error": str(exc)[:500]})
            db.commit()
            skipped.append({"brief_id": brief_id, "reason": str(exc)})
            continue
        claimed = db.execute(text("""UPDATE growth_video_brief_events
            SET status='CREATING',attempts=attempts+1,updated_at=:at,error=NULL
            WHERE brief_id=:id AND status IN ('PENDING','FAILED')"""),
            {"id": brief_id, "at": store.now()})
        db.commit()
        if claimed.rowcount != 1:
            continue
        try:
            existing = _existing_job_for_brief(db, brief_id)
            if existing:
                job_id = existing
            else:
                item = create_job(db, source, owner.id)
                job_id = item["id"]
            db.execute(text("""UPDATE growth_video_brief_events
                SET status='CREATED',job_id=:job,updated_at=:at,error=NULL WHERE brief_id=:id"""),
                {"id": brief_id, "job": job_id, "at": store.now()})
            db.commit()
            created.append({"brief_id": brief_id, "job_id": job_id, "existing": bool(existing)})
        except Exception as exc:
            db.rollback()
            db.execute(text("""UPDATE growth_video_brief_events
                SET status='FAILED',updated_at=:at,error=:error WHERE brief_id=:id"""),
                {"id": brief_id, "at": store.now(),
                 "error": f"{exc.__class__.__name__}: video creation failed"})
            db.commit()
            failed.append({"brief_id": brief_id, "reason": exc.__class__.__name__})
    return {"status": "processed", "created": created, "skipped": skipped, "failed": failed}


def create_daily(db):
    """Backward-compatible entry point; now processes per-publish events, not one/day."""
    recovery = reconcile_publish_events(db)
    events = process_publish_events(db)
    return {"status": events.get("status"), "reconciliation": recovery, **events}


def _review_recipient():
    return os.getenv(REVIEW_EMAIL_ENV, DEFAULT_REVIEW_EMAIL).strip()


def notify_ready(db, *, sender=None):
    from app.services.email_delivery import send_email, email_delivery_enabled, _provider_api_key, _provider_name
    recipient = _review_recipient()
    if not recipient:
        return [{"notification": "email_not_configured"}]
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
            result = sender(db, to_email=recipient, template_key="growth.video_review",
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
    recovery = reconcile_publish_events(db)
    events = process_publish_events(db)
    pass_state = {"status": events.get("status"), "reconciliation": recovery,
                  "created": events.get("created", []), "skipped": events.get("skipped", []),
                  "failed": events.get("failed", [])}
    store.set_setting(db, "GROWTH_VIDEO_LAST_PASS", {"at": store.now(), **pass_state})
    db.commit()
    rendered = run_pending(db, limit=2)
    notices = notify_ready(db)
    return {"daily": pass_state, "video_jobs": rendered, "notifications": notices, "publications": published}
