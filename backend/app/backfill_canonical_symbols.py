from __future__ import annotations

import argparse
import logging

from sqlalchemy import text

from app.db import SessionLocal
from app.models import Event, InsiderTransaction, Security, Transaction, WatchlistItem
from app.utils.symbols import canonical_symbol

logger = logging.getLogger(__name__)


def backfill_canonical_symbols(*, apply: bool = False) -> dict[str, int | bool]:
    session = SessionLocal()

    events_fixed = 0
    insider_rows_fixed = 0
    securities_renamed = 0
    securities_merged = 0
    fk_rows_rewired = 0
    watchlist_duplicates_deleted = 0

    try:
        events = session.query(Event).filter(Event.symbol.isnot(None)).all()
        for event in events:
            canon = canonical_symbol(event.symbol)
            if canon and canon != event.symbol:
                event.symbol = canon
                events_fixed += 1

        insider_rows = session.query(InsiderTransaction).filter(InsiderTransaction.symbol.isnot(None)).all()
        for row in insider_rows:
            canon = canonical_symbol(row.symbol)
            if canon and canon != row.symbol:
                row.symbol = canon
                insider_rows_fixed += 1

        bad_secs = session.query(Security).filter(Security.symbol.like("$%")).all()

        for bad in bad_secs:
            canon = canonical_symbol(bad.symbol)
            if not canon:
                continue

            good = session.query(Security).filter(Security.symbol == canon).first()
            if good and good.id != bad.id:
                tx_rewired = (
                    session.query(Transaction)
                    .filter(Transaction.security_id == bad.id)
                    .update({"security_id": good.id}, synchronize_session=False)
                )
                wl_rewired = (
                    session.query(WatchlistItem)
                    .filter(WatchlistItem.security_id == bad.id)
                    .update({"security_id": good.id}, synchronize_session=False)
                )
                fk_rows_rewired += tx_rewired + wl_rewired

                session.delete(bad)
                securities_merged += 1
            else:
                if bad.symbol != canon:
                    bad.symbol = canon
                    securities_renamed += 1

        dedupe_result = session.execute(
            text(
                """
                DELETE FROM watchlist_items
                WHERE id NOT IN (
                  SELECT MIN(id)
                  FROM watchlist_items
                  GROUP BY watchlist_id, security_id
                )
                """
            )
        )
        if dedupe_result.rowcount and dedupe_result.rowcount > 0:
            watchlist_duplicates_deleted = dedupe_result.rowcount

        if apply:
            session.commit()
        else:
            session.rollback()

        result: dict[str, int | bool] = {
            "apply": apply,
            "events_fixed": events_fixed,
            "insider_rows_fixed": insider_rows_fixed,
            "securities_renamed": securities_renamed,
            "securities_merged": securities_merged,
            "fk_rows_rewired": fk_rows_rewired,
            "watchlist_duplicates_deleted": watchlist_duplicates_deleted,
        }
        logger.info("Backfill canonical symbols completed: %s", result)
        return result
    finally:
        session.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill canonical symbols and merge duplicate $-prefixed securities.")
    parser.add_argument("--apply", action="store_true", help="Apply updates. Without this flag the run is dry-run.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    result = backfill_canonical_symbols(apply=args.apply)

    print(f"events fixed: {result['events_fixed']}")
    print(f"insider rows fixed: {result['insider_rows_fixed']}")
    print(f"securities renamed: {result['securities_renamed']}")
    print(f"securities merged: {result['securities_merged']}")
    print(f"FK rows rewired: {result['fk_rows_rewired']}")
    print(f"watchlist duplicates deleted: {result['watchlist_duplicates_deleted']}")


if __name__ == "__main__":
    main()
