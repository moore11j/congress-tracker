from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run() -> int:
    parser = argparse.ArgumentParser(description="Create a timestamped SQLite backup without mutating the source DB.")
    parser.add_argument("--sqlite-path", required=True, help="SQLite file to copy, e.g. /data/app.db.")
    parser.add_argument("--backup-dir", help="Destination directory. Defaults to source file directory.")
    args = parser.parse_args()

    source = Path(args.sqlite_path)
    if not source.exists():
        raise SystemExit(f"SQLite source does not exist: {source}")
    destination_dir = Path(args.backup_dir) if args.backup_dir else source.parent
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / f"{source.stem}.backup.pre-postgres.{_stamp()}{source.suffix}"
    shutil.copy2(source, destination)

    manifest = {
        "source": str(source),
        "backup": str(destination),
        "bytes": destination.stat().st_size,
        "sha256": _sha256(destination),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = destination.with_suffix(destination.suffix + ".sha256.json")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
