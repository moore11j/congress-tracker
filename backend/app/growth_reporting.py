"""Read-only growth reporting: page views, event reach and billing evidence.

Browser telemetry is consent-dependent. Account and invoice counts are separate
database facts, not a stitched or ordered conversion funnel.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from sqlalchemy import case, func, or_, select

from app.auth import is_admin_user
from app.models import BillingTransaction, PageViewEvent, UserAccount
from app.paid_analytics import metadata, production_enabled


def page_analytics(db, *, start, period, limit, include_internal=False):
    users = db.scalars(select(UserAccount)).all()
    configured_ids = {int(value) for value in os.getenv("ANALYTICS_EXCLUDED_USER_IDS", "").split(",") if value.strip().isdigit()}
    excluded_ids = {u.id for u in users if is_admin_user(u)} | configured_ids
    filters = [PageViewEvent.created_at >= start]
    if not include_internal:
        internal_sessions = select(PageViewEvent.session_id_hash).where(
            PageViewEvent.created_at >= start,
            PageViewEvent.user_id.in_(excluded_ids),
            PageViewEvent.session_id_hash.is_not(None),
        )
        filters.extend([
            or_(PageViewEvent.user_id.is_(None), PageViewEvent.user_id.not_in(excluded_ids)),
            or_(PageViewEvent.session_id_hash.is_(None), PageViewEvent.session_id_hash.not_in(internal_sessions)),
            ~PageViewEvent.path.like("/admin%"),
        ])
    page_filters = [*filters, ~PageViewEvent.normalized_path.like("/events/%")]
    counts = (
        func.count(PageViewEvent.id).label("views"),
        func.count(func.distinct(PageViewEvent.user_id)).label("accounts"),
        func.count(func.distinct(PageViewEvent.session_id_hash)).label("sessions"),
        func.sum(case((PageViewEvent.session_id_hash.is_(None), 1), else_=0)).label("views_without_session"),
    )
    totals = db.execute(select(*counts, func.count(func.distinct(PageViewEvent.path)).label("pages")).where(*page_filters)).one()
    grouped = select(
        PageViewEvent.normalized_path.label("page"), PageViewEvent.route_group.label("route_group"), *counts,
        func.sum(case((PageViewEvent.is_authenticated.is_(True), 1), else_=0)).label("authenticated_views"),
        func.sum(case((PageViewEvent.plan_at_time.in_(["premium", "pro"]), 1), else_=0)).label("paid_views"),
        func.sum(case((PageViewEvent.plan_at_time == "pro", 1), else_=0)).label("pro_views"),
        func.sum(case((PageViewEvent.device_type == "mobile", 1), else_=0)).label("mobile_views"),
        func.max(PageViewEvent.created_at).label("last_viewed_at"),
    ).where(*page_filters).group_by(PageViewEvent.normalized_path, PageViewEvent.route_group)

    def serialize(row):
        views = int(row.views or 0)
        percent = lambda count: round(int(count or 0) * 100 / views, 1) if views else 0
        return {
            "page": row.page, "route_group": row.route_group, "views": views,
            "unique_users": int(row.accounts or 0),  # compatibility: now known account IDs only
            "accounts": int(row.accounts or 0), "sessions": int(row.sessions or 0),
            "views_without_session": int(row.views_without_session or 0),
            "authenticated_views": int(row.authenticated_views or 0),
            "anonymous_views": views - int(row.authenticated_views or 0),
            "auth_percent": percent(row.authenticated_views), "paid_percent": percent(row.paid_views),
            "pro_percent": percent(row.pro_views), "mobile_percent": percent(row.mobile_views),
            "last_viewed_at": row.last_viewed_at,
        }

    top = db.execute(grouped.order_by(func.count(PageViewEvent.id).desc(), PageViewEvent.normalized_path).limit(limit)).all()
    low = db.execute(grouped.order_by(func.count(PageViewEvent.id), PageViewEvent.normalized_path).limit(10)).all()
    destinations = db.execute(select(PageViewEvent.path.label("page"), *counts).where(*page_filters)
                              .group_by(PageViewEvent.path).order_by(func.count(PageViewEvent.id).desc(), PageViewEvent.path).limit(limit)).all()
    event_rows = db.execute(select(PageViewEvent.normalized_path.label("event"), *counts)
                            .where(*filters, PageViewEvent.normalized_path.like("/events/%"))
                            .group_by(PageViewEvent.normalized_path).order_by(PageViewEvent.normalized_path)).all()
    trends = db.execute(select(func.date(PageViewEvent.created_at).label("day"), func.count(PageViewEvent.id).label("views"))
                        .where(*page_filters).group_by(func.date(PageViewEvent.created_at)).order_by(func.date(PageViewEvent.created_at))).all()
    eligible = [u for u in users if not u.deleted_at and not u.is_suspended and (include_internal or u.id not in excluded_ids)]
    eligible_ids = {u.id for u in eligible}
    utc = lambda value: value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value
    invoices = db.scalars(select(BillingTransaction).where(BillingTransaction.charged_at >= start)).all()
    live_invoices, test_invoices, unknown_invoices, zero_invoices = [], 0, 0, 0
    for invoice in invoices:
        if not include_internal and invoice.user_id in excluded_ids:
            continue
        if invoice.payment_status != "paid":
            continue
        payload = metadata(invoice.payload_json)
        if payload.get("livemode") is False:
            test_invoices += 1
        elif payload.get("livemode") is not True or not isinstance(payload.get("amount_paid"), (int, float)):
            unknown_invoices += 1
        elif payload["amount_paid"] <= 0:
            zero_invoices += 1
        else:
            live_invoices.append(invoice)
    paid_ids = {row.user_id for row in live_invoices if row.user_id is not None}
    return {
        "period": period, "generated_at": datetime.now(timezone.utc), "include_internal": include_internal,
        "totals": {key: int(getattr(totals, key) or 0) for key in ("views", "accounts", "sessions", "views_without_session", "pages")},
        "top_pages": [serialize(row) for row in top], "low_usage_pages": [serialize(row) for row in low],
        "top_destinations": [{"page": row.page, "views": row.views, "accounts": row.accounts, "sessions": row.sessions} for row in destinations],
        "trend_by_day": [{"day": str(row.day), "views": row.views} for row in trends],
        "event_reach": [{"event": row.event.removeprefix("/events/"), "events": row.views, "accounts": row.accounts, "sessions": row.sessions} for row in event_rows],
        "accounts": {
            "current_accounts": len(eligible),
            "new_accounts": sum(utc(u.created_at) >= start for u in eligible if u.created_at),
            "active_accounts": sum(utc(u.last_seen_at) >= start for u in eligible if u.last_seen_at),
            "verified_new_accounts": sum(utc(u.created_at) >= start and u.email_verified_at is not None for u in eligible if u.created_at),
        },
        "payments": {
            "live_paid_invoices": len(live_invoices), "live_paying_accounts": len(paid_ids),
            "current_accounts_with_payment": len(paid_ids & eligible_ids),
            "test_paid_invoices": test_invoices, "unverified_paid_invoices": unknown_invoices,
            "zero_paid_invoices": zero_invoices,
            "paid_invoices_with_refund": sum(bool(row.refund_status and row.refund_status != "none") for row in live_invoices),
            "unmatched_live_paid_invoices": sum(row.user_id is None for row in live_invoices),
        },
        "measurement": {
            "production_enabled": production_enabled(),
            "ga4_secret_configured": bool(os.getenv("GA4_API_SECRET", "").strip()),
            "heycatch_bridge_configured": len(os.getenv("ANALYTICS_FORWARDING_SECRET", "")) >= 32,
            "excluded_account_count": len(excluded_ids),
        },
    }
