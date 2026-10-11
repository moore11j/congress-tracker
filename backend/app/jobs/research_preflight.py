"""Read-only no-work checks before loading the research application.

Unknown state always runs the existing worker. No claims, timestamps, settings,
provider calls or delivery identities are changed here.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from zoneinfo import ZoneInfo


@contextmanager
def _cursor():
    dsn = os.getenv("DATABASE_URL", "")
    if dsn.startswith("postgresql+psycopg://"):
        dsn = dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    if not dsn.startswith(("postgres://", "postgresql://")):
        raise ValueError("PostgreSQL preflight only")
    import psycopg
    with psycopg.connect(dsn, connect_timeout=2,
            application_name=f"walnut:research-preflight:{os.getpid()}",
            options="-c default_transaction_read_only=on -c statement_timeout=1500 -c lock_timeout=500") as conn:
        with conn.cursor() as cursor:
            yield cursor


def campaign_idle(*, generation=True, publication=True, now=None):
    """Skip only when every requested operation has no due candidates."""
    now = now or datetime.now(timezone.utc)
    try:
        with _cursor() as cursor:
            if generation:
                cursor.execute("""SELECT EXISTS (SELECT 1 FROM research_campaign_items i
                    JOIN research_campaigns c ON c.id=i.campaign_id
                    WHERE i.status='pending' AND c.active=TRUE AND i.generate_at <= %s)""", (now.isoformat(),))
                if cursor.fetchone() != (False,):
                    return False
            if publication:
                cursor.execute("""SELECT EXISTS (SELECT 1 FROM research_brief_drafts
                    WHERE status='approved_scheduled' AND scheduled_at <= %s)""", (now.isoformat(),))
                if cursor.fetchone() != (False,):
                    return False
        return True
    except Exception:
        return False


def seo_idle(*, now=None):
    """Retain queued manual plans and daily Search Console attempts."""
    now = now or datetime.now(timezone.utc)
    try:
        with _cursor() as cursor:
            cursor.execute("SELECT last_attempt_at FROM research_gsc_connection WHERE id=1")
            gsc = cursor.fetchone()
            cutoff = now.astimezone(ZoneInfo("America/Los_Angeles")).replace(
                hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
            # Match the existing TEXT comparison in the daily sync claim.
            if gsc is not None and (gsc[0] is None or gsc[0] < cutoff):
                return False
            cursor.execute("SELECT config_json, owner_id FROM research_seo_settings WHERE id=1")
            settings = cursor.fetchone()
            if settings is None:
                return True
            config = {"enabled": False, "draft_time": "07:00", "timezone": "America/Los_Angeles",
                      **json.loads(settings[0])}
            local = now.astimezone(ZoneInfo(config["timezone"]))
            cursor.execute("SELECT status FROM research_seo_runs WHERE day=%s", (local.date().isoformat(),))
            run = cursor.fetchone()
            if not settings[1]:
                return True
            if run is not None:
                return run[0] != "queued"
            return not config["enabled"] or local.strftime("%H:%M") < config["draft_time"]
    except Exception:
        return False
