"""Run one bounded pass; cron repeats it while drafts have pending stages."""
import json
import os
import signal
import sys
import threading
import logging

from app.db import SessionLocal
from app.services.growth_video_automation import run_once


def main():
    if os.getenv("GROWTH_VIDEO_WORKER_ENABLED", "").lower() != "true":
        return
    continuous = "--continuous" in sys.argv
    stopped = threading.Event()
    if continuous:
        signal.signal(signal.SIGTERM, lambda *_: stopped.set())
        signal.signal(signal.SIGINT, lambda *_: stopped.set())
    while not stopped.is_set():
        delay = 60
        try:
            with SessionLocal() as db:
                result = run_once(db, render_limit=1)
                print(json.dumps(result), flush=True)
                # One stage at a time; advance an active queue without the old
                # two-minute cron delay between every capture/audio/render step.
                if result.get("video_jobs"):
                    delay = 5
        except Exception:
            logging.exception("growth_video_worker_pass_failed")
            if not continuous:
                raise
        if not continuous:
            break
        stopped.wait(delay)


if __name__ == "__main__":
    main()
