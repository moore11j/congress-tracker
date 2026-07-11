from __future__ import annotations

import argparse
import json

from app.db import SessionLocal
from app.services.ai_marketing import run_due_article_reactive_campaigns


def main() -> None:
    parser = argparse.ArgumentParser(description="Run due AI Growth article-reactive X campaigns.")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with SessionLocal() as db:
        result = run_due_article_reactive_campaigns(db, force=args.force, dry_run=args.dry_run)
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
