"""Buffer publishing outbox. Never replay a create request with an unknown outcome."""
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from sqlalchemy import text
from app.models import AiMarketingOpportunity

from app.services import growth_video_store as store
from app.services.growth_video_domain import digest

# These are the two Walnut accounts explicitly connected by the owner.
CHANNELS = {"instagram": "6aa61e05ea19ca0bde31a165", "tiktok": "6aa61e4cea19ca0bde31a20f"}
POST_FIELDS = "id channelId status externalLink sentAt dueAt schedulingType error { message }"


class BufferError(ValueError):
    pass


class BufferQuota(BufferError):
    pass


class BufferClient:
    def __init__(self, db=None):
        self.db = db
        self.key = os.getenv("BUFFER_API_KEY", "").strip()
        if not self.key:
            raise BufferError("Buffer API key has not been configured on the server.")

    def query(self, query, variables=None):
        # No request retries, no redirects carrying Authorization, no credential
        # or media-token-bearing response bodies in logs/errors.
        if self.db is not None:
            reserve_request(self.db)
        try:
            r = requests.post("https://api.buffer.com", headers={"Authorization": "Bearer " + self.key},
                              json={"query": query, "variables": variables or {}}, timeout=(10, 50), allow_redirects=False)
            if r.status_code != 200:
                raise BufferError(f"Buffer returned HTTP {r.status_code}. Check Buffer before retrying.")
            payload = r.json()
            if payload.get("errors") or not isinstance(payload.get("data"), dict):
                raise BufferError("Buffer rejected the API operation. Check API permissions and schema.")
            return payload["data"]
        except (requests.RequestException, json.JSONDecodeError):
            raise BufferError("Buffer response could not be confirmed. Check Buffer before retrying.") from None

    def channel(self, platform):
        data = self.query("query($input:ChannelInput!){channel(input:$input){id name service isDisconnected isLocked isQueuePaused}}",
                          {"input": {"id": CHANNELS[platform]}})["channel"]
        if data["id"] != CHANNELS[platform] or data["service"] != platform or data["isDisconnected"] or data["isLocked"]:
            raise BufferError("The connected Walnut channel is unavailable. Reconnect it in Buffer.")
        return data

    def create(self, platform, caption, media_url, *, scheduled_at=None):
        metadata = {platform: {"isAiGenerated": True}}
        if platform == "instagram":
            metadata[platform].update(type="reel", shouldShareToFeed=True)
        payload = {"channelId": CHANNELS[platform], "text": caption, "schedulingType": "automatic",
                   "mode": "customScheduled" if scheduled_at else "shareNow", "needsApproval": False,
                   "saveToDraft": False, "aiAssisted": True,
                   "assets": [{"video": {"url": media_url}}], "metadata": metadata}
        if scheduled_at:
            # Never fall back to shareNow when a schedule has expired in the outbox.
            if datetime.fromisoformat(scheduled_at) <= datetime.now(timezone.utc):
                raise BufferError("The scheduled time passed before Buffer received the post.")
            payload["dueAt"] = scheduled_at
        result = self.query("mutation($input:CreatePostInput!){createPost(input:$input){... on PostActionSuccess {post {" + POST_FIELDS + "}} ... on MutationError {message}}}",
            {"input": payload})["createPost"]
        if not result.get("post"):
            # Even a mutation error can follow a side effect. Manual reconciliation
            # is safer than automatically creating a second public post.
            raise BufferError("Buffer did not confirm a post ID. Inspect the channel in Buffer before retrying.")
        return result["post"]

    def post(self, post_id):
        return self.query("query($input:PostInput!){post(input:$input){" + POST_FIELDS + "}}", {"input": {"id": post_id}})["post"]


def ensure_schema(db):
    db.execute(text("""CREATE TABLE IF NOT EXISTS growth_video_publications (
        job_id TEXT NOT NULL, platform TEXT NOT NULL, status TEXT NOT NULL,
        token TEXT NOT NULL UNIQUE, asset_hash TEXT NOT NULL, caption TEXT NOT NULL,
        approved_by INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        post_id TEXT, external_url TEXT, error TEXT, checked_at TEXT,
        PRIMARY KEY(job_id,platform))"""))
    db.commit()
    # Separate additive table keeps existing immediate publications unchanged.
    db.execute(text("""CREATE TABLE IF NOT EXISTS growth_video_publication_schedules (
        job_id TEXT NOT NULL, platform TEXT NOT NULL, scheduled_at TEXT NOT NULL,
        schedule_timezone TEXT NOT NULL, buffer_due_at TEXT,
        PRIMARY KEY(job_id,platform))"""))
    db.commit()
    db.execute(text("CREATE TABLE IF NOT EXISTS growth_buffer_api_budget (window_key TEXT PRIMARY KEY, requests INTEGER NOT NULL DEFAULT 0)"))
    db.commit()


def reserve_request(db):
    now = datetime.now(timezone.utc)
    windows = [(now.strftime("month:%Y-%m"), 2800), (now.strftime("day:%Y-%m-%d"), 200),
               (now.strftime("quarter:%Y-%m-%d:%H:") + str(now.minute // 15), 80)]
    for key, limit in windows:
        db.execute(text("INSERT INTO growth_buffer_api_budget(window_key) VALUES (:key) ON CONFLICT DO NOTHING"), {"key": key})
        used = db.execute(text("UPDATE growth_buffer_api_budget SET requests=requests+1 WHERE window_key=:key AND requests<:limit"), {"key": key, "limit": limit})
        if used.rowcount != 1:
            db.rollback()
            raise BufferQuota("Walnut's Buffer API request budget is exhausted. Waiting for the next quota window.")
    db.commit()


def publications(db, job_id=None):
    fields = "job_id,platform,status,caption,approved_by,created_at,updated_at,post_id,external_url,error,checked_at"
    query = "SELECT " + ",".join("p." + field for field in fields.split(",")) + ",s.scheduled_at,s.schedule_timezone,s.buffer_due_at FROM growth_video_publications p LEFT JOIN growth_video_publication_schedules s ON s.job_id=p.job_id AND s.platform=p.platform"
    return [dict(r) for r in db.execute(text(query + (" WHERE p.job_id=:id" if job_id else " ORDER BY p.created_at DESC LIMIT 100")), {"id": job_id}).mappings()]


def validate_schedule(scheduled_at, schedule_timezone):
    if scheduled_at is None:
        if schedule_timezone is not None:
            raise ValueError("Choose a date and time when scheduling a post.")
        return None, None
    try:
        due = datetime.fromisoformat(scheduled_at)
        if due.tzinfo is None or due.utcoffset() is None:
            raise ValueError()
        zone = schedule_timezone or "UTC"
        ZoneInfo(zone)
    except (ValueError, TypeError, ZoneInfoNotFoundError):
        raise ValueError("Choose a valid scheduled date, time and timezone.") from None
    due = due.astimezone(timezone.utc)
    if due < datetime.now(timezone.utc) + timedelta(minutes=10):
        raise ValueError("Choose a time at least 10 minutes from now so Buffer can prepare the video.")
    return due.isoformat(timespec="milliseconds"), zone


def approve_publish(db, item, actor, platforms, caption, *, scheduled_at=None, schedule_timezone=None, client=None):
    from app.services.growth_video_pipeline import validate_job
    if item["lease_token"] or item["status"] not in {"READY_FOR_REVIEW", "APPROVED"}:
        raise ValueError("Only a completed video can be approved for publishing.")
    platforms = list(dict.fromkeys(platforms))
    if not platforms or set(platforms) - set(CHANNELS):
        raise ValueError("Select Instagram, TikTok, or both.")
    caption = caption.strip()
    if not 1 <= len(caption) <= 2200:
        raise ValueError("The caption must contain 1–2,200 characters.")
    scheduled_at, schedule_timezone = validate_schedule(scheduled_at, schedule_timezone)
    validate_job(db, item)
    video = item["payload"].get("video") or {}
    if video.get("width") != 1080 or video.get("height") != 1920 or not video.get("sha256"):
        raise ValueError("A completed 1080×1920 video is required.")
    if not video.get("object_key", "").startswith(f"ai-growth/{item['id']}/"):
        raise ValueError("Video asset ownership is invalid.")
    client = client or BufferClient(db)
    for platform in platforms:
        channel = client.channel(platform)
        if scheduled_at and channel.get("isQueuePaused"):
            raise BufferError("The Walnut channel is paused in Buffer. Resume its queue before scheduling.")
    # Claim the job before adding destinations. This serializes concurrent
    # approvals and rejects, so they cannot authorize different assets/captions.
    locked = db.execute(text("UPDATE growth_video_jobs SET updated_at=:at WHERE id=:id AND updated_at=:old AND lease_token IS NULL AND status IN ('READY_FOR_REVIEW','APPROVED')"),
                        {"id": item["id"], "old": item["updated_at"], "at": store.now()})
    if locked.rowcount != 1:
        db.rollback()
        raise ValueError("The video changed during review. Refresh before publishing.")
    existing = publications(db, item["id"])
    if any(p["caption"] != caption for p in existing):
        db.rollback()
        raise ValueError("Publishing already started with a different caption. Review the existing posts in Buffer.")
    if any(p["scheduled_at"] != scheduled_at for p in existing):
        db.rollback()
        raise ValueError("This video already has a different publishing time. Manage the existing schedule in Buffer.")
    for platform in platforms:
        db.execute(text("""INSERT INTO growth_video_publications
            (job_id,platform,status,token,asset_hash,caption,approved_by,created_at,updated_at)
            VALUES (:job,:platform,'QUEUED',:token,:hash,:caption,:actor,:at,:at)
            ON CONFLICT(job_id,platform) DO NOTHING"""),
            {"job": item["id"], "platform": platform, "token": secrets.token_urlsafe(32),
             "hash": video["sha256"], "caption": caption, "actor": actor, "at": store.now()})
        if scheduled_at:
            db.execute(text("""INSERT INTO growth_video_publication_schedules(job_id,platform,scheduled_at,schedule_timezone)
                VALUES (:job,:platform,:due,:zone) ON CONFLICT(job_id,platform) DO NOTHING"""),
                {"job": item["id"], "platform": platform, "due": scheduled_at, "zone": schedule_timezone})
    data = item["payload"]
    data["approval"] = {"actor_id": actor, "at": store.now(), "asset_hash": video["sha256"],
                        "caption_hash": digest(caption), "platforms": sorted(set(platforms) | {p['platform'] for p in existing}),
                        "action": "approve_and_schedule" if scheduled_at else "approve_and_publish",
                        "scheduled_at": scheduled_at, "schedule_timezone": schedule_timezone}
    db.execute(text("UPDATE growth_video_jobs SET status='APPROVED',payload_json=:payload WHERE id=:id"),
               {"id": item["id"], "payload": store.dumps(data)})
    draft = db.get(AiMarketingOpportunity, item["draft_id"])
    if draft:
        draft.status = "approved"
    db.commit()
    return publications(db, item["id"])


def media_asset(db, token):
    row = db.execute(text("SELECT * FROM growth_video_publications WHERE token=:token"), {"token": token}).mappings().first()
    if not row:
        raise ValueError("Media unavailable.")
    item = store.job(db, row["job_id"])
    p = item["payload"]
    asset = p.get("video") or {}
    if (item["status"] != "APPROVED" or asset.get("sha256") != row["asset_hash"]
            or p.get("approval", {}).get("asset_hash") != row["asset_hash"]
            or not asset.get("object_key", "").startswith(f"ai-growth/{item['id']}/")):
        raise ValueError("Media unavailable.")
    if row["status"] in {"PUBLISHED", "FAILED", "CANCELLED"} and row["updated_at"] < (datetime.now(timezone.utc) - timedelta(days=7)).isoformat():
        raise ValueError("Media unavailable.")
    return asset


def update(db, row, status, *, post=None, error=None):
    post = post or {}
    external = post.get("externalLink")
    if external:
        host = urlsplit(external).hostname or ""
        if urlsplit(external).scheme != "https" or not any(host == h or host.endswith("." + h) for h in ("instagram.com", "tiktok.com")):
            external = None
    changed = db.execute(text("""UPDATE growth_video_publications SET status=:status,updated_at=:at,checked_at=:at,
       post_id=COALESCE(:post,post_id),external_url=COALESCE(:url,external_url),error=:error
       WHERE job_id=:job AND platform=:platform AND status=:previous"""),
       {"job": row["job_id"], "platform": row["platform"], "status": status, "at": store.now(),
        "post": post.get("id"), "url": external, "error": error, "previous": row["status"]})
    if not changed.rowcount:
        db.rollback()
        return
    if post.get("dueAt"):
        due = datetime.fromisoformat(post["dueAt"]).astimezone(timezone.utc).isoformat(timespec="milliseconds")
        db.execute(text("UPDATE growth_video_publication_schedules SET buffer_due_at=:due WHERE job_id=:job AND platform=:platform"),
                   {"due": due, "job": row["job_id"], "platform": row["platform"]})
    db.commit()


def post_status(post):
    if post is None or post.get("status") in {"deleted", "cancelled"}:
        return "CANCELLED"
    if post.get("error") or post.get("status") in {"error", "failed"}:
        return "FAILED"
    if post.get("status") == "sent" and post.get("externalLink") and post.get("schedulingType") == "automatic":
        return "PUBLISHED"
    if post.get("status") == "scheduled" and post.get("dueAt") and post.get("schedulingType") == "automatic":
        return "SCHEDULED"
    return "SUBMITTED"


def run_pending(db, *, client=None, limit=2):
    ensure_schema(db)
    if not os.getenv("BUFFER_API_KEY") and client is None:
        return []
    client = client or BufferClient(db)
    before = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
    # A crashed request is NEVER automatically retried.
    db.execute(text("UPDATE growth_video_publications SET status='UNCERTAIN',error='Worker interrupted. Inspect Buffer before reconciliation.' WHERE status='SUBMITTING' AND updated_at<:before"), {"before": before})
    db.commit()
    now = datetime.now(timezone.utc)
    rows = db.execute(text("""SELECT p.*,s.scheduled_at,s.schedule_timezone FROM growth_video_publications p
       LEFT JOIN growth_video_publication_schedules s ON s.job_id=p.job_id AND s.platform=p.platform
       WHERE p.status='QUEUED' OR (p.status='SUBMITTED' AND (p.checked_at IS NULL OR p.checked_at<:before))
       OR (p.status='SCHEDULED' AND (p.checked_at IS NULL OR p.checked_at<:scheduled_before
           OR (COALESCE(s.buffer_due_at,s.scheduled_at)<=:now AND p.checked_at<:before)))
       ORDER BY CASE WHEN p.status='QUEUED' THEN 0 ELSE 1 END,p.updated_at LIMIT :limit"""),
       {"before": before, "scheduled_before": (now-timedelta(hours=6)).isoformat(), "now": now.isoformat(), "limit": limit}).mappings().all()
    output = []
    for row in rows:
        row = dict(row)
        try:
            if row["status"] in {"SUBMITTED", "SCHEDULED"}:
                # Persist poll time first to bound usage across concurrent workers.
                claim = db.execute(text("UPDATE growth_video_publications SET checked_at=:at WHERE job_id=:job AND platform=:platform AND status=:status AND COALESCE(checked_at,'')=COALESCE(:old,'')"),
                    {"at": store.now(), "job": row["job_id"], "platform": row["platform"], "status": row["status"], "old": row["checked_at"]})
                db.commit()
                if not claim.rowcount:
                    continue
                post = client.post(row["post_id"])
                update(db, row, post_status(post), post=post, error="Buffer reports a publishing error. Inspect the post in Buffer." if (post or {}).get("error") else None)
            else:
                from app.services.growth_video_pipeline import validate_job
                item = store.job(db, row["job_id"])
                validate_job(db, item)
                media_asset(db, row["token"])
                if row["scheduled_at"] and datetime.fromisoformat(row["scheduled_at"]) <= datetime.now(timezone.utc):
                    update(db, row, "FAILED", error="The scheduled time passed before submission. Nothing was sent. Choose a new time before retrying.")
                    continue
                channel = client.channel(row["platform"])
                if row["scheduled_at"] and channel.get("isQueuePaused"):
                    update(db, row, "FAILED", error="The Walnut channel is paused in Buffer. Resume its queue before retrying the schedule.")
                    continue
                claimed = db.execute(text("UPDATE growth_video_publications SET status='SUBMITTING',updated_at=:at WHERE job_id=:job AND platform=:platform AND status='QUEUED'"),
                    {"at": store.now(), "job": row["job_id"], "platform": row["platform"]})
                db.commit()
                if not claimed.rowcount:
                    continue
                row["status"] = "SUBMITTING"
                schedule = {"scheduled_at": row["scheduled_at"]} if row["scheduled_at"] else {}
                post = client.create(row["platform"], row["caption"], "https://congress-tracker-api.fly.dev/api/growth-video-media/" + row["token"], **schedule)
                update(db, row, post_status(post), post=post)
            output.append({"job_id": row["job_id"], "platform": row["platform"], "checked": True})
        except BufferQuota as exc:
            db.rollback()
            if row["status"] not in {"SUBMITTED", "SCHEDULED"}:
                update(db, row, "QUEUED", error=str(exc))
        except Exception:
            db.rollback()
            if row["status"] not in {"SUBMITTED", "SCHEDULED"}:
                update(db, row, "UNCERTAIN" if row["status"] == "SUBMITTING" else "FAILED",
                       error="Check Buffer before reconciliation; no automatic retry." if row["status"] == "SUBMITTING" else "Publishing preflight failed. Check the source, approval and channel connection.")
    return output
