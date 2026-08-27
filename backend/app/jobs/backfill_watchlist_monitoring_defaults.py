"""Apply account-respecting watchlist monitoring defaults to existing watchlists."""
from __future__ import annotations

import argparse
import json

from sqlalchemy import select

from app.db import SessionLocal
from app.models import NotificationSubscription, UserAccount, Watchlist
from app.services.custom_alert_rules import ensure_default_intraday_price_move_alert
from app.services.watchlist_delivery import WATCHLIST_ALERT_CATEGORIES

JAROD_EMAIL = "moore11j@gmail.com"


def backfill_watchlist_monitoring_defaults(*, dry_run: bool = False) -> dict[str, int]:
    with SessionLocal() as db:
        watchlists = db.execute(
            select(Watchlist, UserAccount)
            .join(UserAccount, UserAccount.id == Watchlist.owner_user_id)
            .order_by(Watchlist.id.asc())
        ).all()
        alerts_added = subscriptions_added = subscriptions_updated = already_configured = skipped = 0

        for watchlist, user in watchlists:
            subscription = db.execute(
                select(NotificationSubscription).where(
                    NotificationSubscription.source_type == "watchlist",
                    NotificationSubscription.source_id == str(watchlist.id),
                    NotificationSubscription.email == user.email,
                )
            ).scalar_one_or_none()
            has_subscription = subscription is not None

            if user.email.strip().lower() == JAROD_EMAIL:
                # Jarod already has the two default price-move rules.  His
                # category delivery is still updated below with every other
                # watchlist, per the account-level preference policy.
                skipped += 1
                needs_alert = 0
            elif dry_run:
                from app.services.custom_alert_rules import default_intraday_price_move_alerts, is_intraday_five_percent_price_move_alert
                from app.models import WatchlistAlertRule

                rules = db.execute(
                    select(WatchlistAlertRule).where(
                        WatchlistAlertRule.user_id == user.id,
                        WatchlistAlertRule.watchlist_id == watchlist.id,
                    )
                ).scalars().all()
                needs_alert = sum(
                    not any(is_intraday_five_percent_price_move_alert(rule, conditions) for rule in rules)
                    for _, conditions in default_intraday_price_move_alerts()
                )
            else:
                needs_alert = ensure_default_intraday_price_move_alert(
                    db,
                    user_id=user.id,
                    watchlist_id=watchlist.id,
                )

            if needs_alert:
                alerts_added += needs_alert

            daily_enabled = bool(user.watchlist_activity_notifications)
            intraday_enabled = daily_enabled and bool(user.signals_notifications)
            delivery_mode = "daily" if daily_enabled else "off"
            desired_payload = {
                "daily_digest_enabled": daily_enabled,
                "intraday_alerts_enabled": intraday_enabled,
                "alert_delivery_modes": {
                    category: delivery_mode for category in WATCHLIST_ALERT_CATEGORIES
                },
            }
            if not has_subscription:
                subscriptions_added += 1
                if not dry_run:
                    db.add(
                        NotificationSubscription(
                            email=user.email,
                            source_type="watchlist",
                            source_id=str(watchlist.id),
                            source_name=watchlist.name,
                            source_payload_json=json.dumps(desired_payload),
                            frequency="daily",
                            only_if_new=True,
                            active=daily_enabled,
                            alert_triggers_json="[]",
                            min_smart_score=None,
                            large_trade_amount=None,
                        )
                    )
            else:
                try:
                    existing_payload = json.loads(subscription.source_payload_json or "{}")
                except json.JSONDecodeError:
                    existing_payload = {}
                payload = existing_payload.copy() if isinstance(existing_payload, dict) else {}
                payload.update(desired_payload)
                changed = (
                    subscription.active != daily_enabled
                    or payload != existing_payload
                )
                if changed:
                    subscriptions_updated += 1
                    if not dry_run:
                        subscription.active = daily_enabled
                        subscription.source_payload_json = json.dumps(payload, sort_keys=True)
            if not needs_alert and has_subscription:
                already_configured += 1

        if not dry_run:
            db.commit()
        return {
            "watchlists_checked": len(watchlists),
            "alerts_added": alerts_added,
            "subscriptions_added": subscriptions_added,
            "subscriptions_updated": subscriptions_updated,
            "already_configured": already_configured,
            "skipped": skipped,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill default intraday alerts and daily digests for watchlists.")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing them.")
    args = parser.parse_args()
    print(json.dumps(backfill_watchlist_monitoring_defaults(dry_run=args.dry_run), sort_keys=True))


if __name__ == "__main__":
    main()
