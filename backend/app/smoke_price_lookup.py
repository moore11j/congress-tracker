from __future__ import annotations

from app.db import SessionLocal
from app.services.price_lookup import get_eod_close


if __name__ == "__main__":
    db = SessionLocal()
    try:
        print(get_eod_close(db, "AAPL", "2026-02-10"))
    finally:
        db.close()
