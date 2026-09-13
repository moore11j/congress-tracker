"""Run one bounded pass; cron repeats it while drafts have pending stages."""
import json
import os

from app.db import SessionLocal
from app.services.growth_video_automation import run_once


def main():
    if os.getenv("GROWTH_VIDEO_WORKER_ENABLED", "").lower() != "true":
        return
    with SessionLocal() as db:
        print(json.dumps(run_once(db)))


if __name__ == "__main__":
    main()
