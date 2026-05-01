from __future__ import annotations

import argparse

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Watchlist
from app.services.monitoring_alerts import refresh_watchlist_alerts


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild unread monitoring alerts for monitored watchlists.")
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--lookback-days", type=int, default=7)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        watchlists = (
            db.execute(select(Watchlist).where(Watchlist.owner_user_id == args.user_id).order_by(Watchlist.name.asc(), Watchlist.id.asc()))
            .scalars()
            .all()
        )
        created_total = 0
        for watchlist in watchlists:
            created_total += refresh_watchlist_alerts(
                db,
                user_id=args.user_id,
                watchlist=watchlist,
                lookback_days=args.lookback_days,
                force_lookback=True,
            )
        db.commit()
        print(
            f"rebuild_monitoring_alerts user_id={args.user_id} "
            f"watchlists={len(watchlists)} lookback_days={args.lookback_days} unread_created={created_total}"
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
