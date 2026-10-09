"""Observe prepared free news without changing selected public feeds or sending."""
import json
from app.db import SessionLocal
from app.services.finnhub_news_warming import run

def main():
    with SessionLocal() as db:
        result = run(db)
        db.commit()
    print(json.dumps(result,sort_keys=True))
    return 1 if result['status'] in {'partial','unavailable'} else 0

if __name__ == '__main__':
    raise SystemExit(main())
