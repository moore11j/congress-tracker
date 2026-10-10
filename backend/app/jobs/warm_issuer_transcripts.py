"""Prepare at most two reviewed issuer pages, each no more than daily."""
import os

from app.jobs.collect_direct_feeds import main as collect
from app.services.direct_feed_store import dumps


def main():
    if os.getenv("ISSUER_TRANSCRIPT_WARMING_ENABLED", "false").strip().lower() not in {"1", "true"}:
        print(dumps({"status": "disabled", "source": "issuer_earnings"}))
        return
    collect(["--sources", "issuer_earnings", "--limit", "2", "--recheck-hours", "24"])


if __name__ == "__main__":
    main()
