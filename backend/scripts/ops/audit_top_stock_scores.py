"""Read-only comparison of leaderboards, ticker caches, and current inputs.

Run with the repaired application code so scoring inputs cannot hydrate
incomplete fundamentals. Does not build ticker pages or capture history.
"""
import json
from datetime import datetime, timezone

from sqlalchemy import select

from app.db import SessionLocal
from app.models import LeaderboardSnapshot, TickerContextBundleCache
from app.services.confirmation_context import build_confirmation_score_context


def audit():
    # Reuse the deployed transformations without triggering ticker builds,
    # hydration, history capture, or cache writes.
    from app.main import (
        _merge_fresh_public_contexts_into_confirmation_bundle,
        build_ticker_signals_summary_contexts_from_cache,
    )

    with SessionLocal() as db:
        conn = db.connection()
        if conn.dialect.name == "postgresql":
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        elif conn.dialect.name == "sqlite":
            conn.exec_driver_sql("PRAGMA query_only = ON")
        else:
            raise RuntimeError("Unsupported read-only transaction")
        snapshot = db.scalar(select(LeaderboardSnapshot).where(LeaderboardSnapshot.leaderboard_key == "top_stocks"))
        payload = json.loads(snapshot.payload_json) if snapshot else {}
        items = {row["symbol"]: row for row in payload.get("items", [])}
        for rows in payload.get("filter_items", {}).values():
            for row in rows:
                items.setdefault(row["symbol"], row)
        bundles = build_confirmation_score_context(db, list(items), lookback_days=30)["bundles"]
        report = {"audited_at": datetime.now(timezone.utc).isoformat(), "generated_at": payload.get("generated_at"), "top_ten": [r["symbol"] for r in payload.get("items", [])], "rows": []}
        for symbol, item in items.items():
            bundle = bundles[symbol]
            merged = _merge_fresh_public_contexts_into_confirmation_bundle(bundle, build_ticker_signals_summary_contexts_from_cache(symbol, db=db))
            caches = db.scalars(select(TickerContextBundleCache).where(TickerContextBundleCache.symbol == symbol).where(TickerContextBundleCache.user_segment == "canonical").order_by(TickerContextBundleCache.generated_at.desc()).limit(1)).all()
            cached = []
            for row in caches:
                data = json.loads(row.payload_json).get("confirmation_score_bundle") or {}
                cached.append({"segment": row.user_segment, "generated_at": str(row.generated_at), "expires_at": str(row.expires_at), "score": data.get("score"), "direction": data.get("direction"), "sources": data.get("sources")})
            report["rows"].append({"symbol": symbol, "leaderboard_score": item.get("confirmation_score"), "leaderboard_direction": item.get("confirmation_direction"), "current_canonical": bundle, "ticker_merged": merged, "ticker_caches": cached})
        db.rollback()
        return report


if __name__ == "__main__":
    print("TOP_STOCK_AUDIT=" + json.dumps(audit(), default=str, separators=(",", ":")))
