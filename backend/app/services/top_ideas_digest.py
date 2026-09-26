"""Top Ideas uses the existing delivery, template, consent and deduplication path."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from html import escape
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.entitlements import entitlements_for_user
from app.models import UserAccount
from app.services.ranking_access import idea_limit, project_ranking
from app.services.top_stocks import build_top_stocks_response

logger = logging.getLogger(__name__)
TEMPLATE = "alerts.top_stock_ideas"


def build_top_ideas_digest(db, user, *, now=None):
    from app.services.email_digests import DigestBuild, _frontend_base_url
    entitlement = entitlements_for_user(db, user)
    snapshot = project_ranking(build_top_stocks_response(db, entitlements=entitlement), authenticated=True,
                               entitlements=entitlement, stocks=True, full=entitlement.has_feature("leaderboards"),
                               stock_limit=idea_limit(entitlement))
    items = snapshot["items"]
    sections = []
    html_sections = []
    for item in items:
        lines = [f"#{item['rank']} {item['symbol']} — {item['company_name']}",
                 f"Why this ranked: {item.get('why_ranked', 'Confirmation in available evidence')}",
                 " · ".join(item.get("key_drivers") or [])]
        if "confirmation_score" in item:
            lines.append(f"Confirmation Score: {item['confirmation_score']}")
            coverage = item.get("confirmation_coverage") or {}
            if isinstance(coverage.get("aligned_source_count"), int):
                lines.append(f"{coverage['aligned_source_count']} of {coverage['source_count']} sources aligned")
            lines.extend(f"{e['source']}: {e['summary']}" for e in item.get("why_this_ranked", []) if e.get("summary"))
        lines.append(f"{_frontend_base_url()}/ticker/{item['symbol']}")
        sections.append("\n".join(lines))
        html_sections.append('<p style="white-space:pre-line"><strong>' + escape(lines[0]) + '</strong>\n' + escape("\n".join(lines[1:])) + '</p>')
    summary = f"{len(items)} ranked stock ideas. Snapshot: {snapshot.get('generated_at') or 'unavailable'}."
    return DigestBuild(template_key=TEMPLATE, context={
        "first_name": user.first_name or user.name or "there", "summary": summary,
        "items_text": "\n\n".join(sections),
        "items_html": "".join(html_sections),
        "digest_url": f"{_frontend_base_url()}/leaderboards#top-stocks",
        "preferences_url": f"{_frontend_base_url()}/account#monitoring-email-preferences",
    }, items_count=len(items), summary=summary, items=items)


def run_top_ideas_digest(db, *, limit=100, dry_run=False, now=None):
    from app.services.email_digests import _alert_skip_reason, _duplicate_digest_result, _preview_result, _send_digest, _with_preview
    now = now or datetime.now(timezone.utc)
    local = now.astimezone(ZoneInfo("America/Los_Angeles"))
    if local.weekday() > 4:
        return []
    results = []
    cursor = 0
    # Seek past delivered recipients so bounded retries cannot starve later users.
    while len(results) < limit:
        users = db.scalars(select(UserAccount).where(UserAccount.id > cursor)
            .where(UserAccount.top_stock_ideas_frequency.in_(["daily", "weekly"]))
            .where(UserAccount.deleted_at.is_(None)).order_by(UserAccount.id).limit(100)).all()
        if not users:
            break
        for user in users:
            cursor = user.id
            try:
                if _alert_skip_reason(user, "top_ideas") or not user.email_verified_at:
                    continue
                entitlement = entitlements_for_user(db, user)
                frequency = user.top_stock_ideas_frequency if entitlement.has_feature("leaderboards") else "weekly"
                if frequency == "weekly" and local.weekday() != 4:
                    continue
                period = local.date() if frequency == "daily" else (local - timedelta(days=local.weekday())).date()
                key = f"digest:v1:{TEMPLATE}:user:{user.id}:{frequency}:{period}"
                if _duplicate_digest_result(db, key):
                    continue
                digest = build_top_ideas_digest(db, user, now=now)
                if not digest.items_count:
                    results.append(_preview_result(user, template_key=TEMPLATE, skip_reason="no_qualified_ideas"))
                elif dry_run:
                    results.append(_with_preview(_preview_result(user, template_key=TEMPLATE, skip_reason=None), digest))
                else:
                    results.append(_with_preview(_send_digest(db, user=user, digest=digest, category="alerts", idempotency_key=key), digest))
            except Exception:
                db.rollback()
                logger.exception("top_ideas_digest_user_failed user_id=%s", cursor)
                results.append({"user_id": cursor, "status": "failed", "error": "digest_build_failed"})
            if len(results) >= limit:
                break
    return results
