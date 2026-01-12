# backend/app/write_last_updated.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

OUT = Path("/data/last_updated.json")  # Fly volume path (persisted)

def main() -> None:
    payload = {
        "last_updated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload), encoding="utf-8")
    print(payload)

if __name__ == "__main__":
    main()
