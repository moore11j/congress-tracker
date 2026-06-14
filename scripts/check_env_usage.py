#!/usr/bin/env python3
"""Scan the repository for environment variable references.

The script reports variable names and source locations only. It does not read
local .env files, except example files, and it never prints configured values.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Iterable


SKIP_DIRS = {
    ".codex",
    ".git",
    ".next",
    ".pytest_cache",
    ".vercel",
    ".venv",
    ".venv312",
    ".codex_py314_deps",
    "__pycache__",
    "artifacts",
    "node_modules",
}
SKIP_SUFFIXES = {
    ".db",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".log",
    ".pdf",
    ".png",
    ".pyc",
    ".sqlite",
    ".tsbuildinfo",
    ".webp",
    ".zip",
}

ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,}$")
DIRECT_PATTERNS = (
    ("os.getenv", re.compile(r"os\.getenv\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]")),
    ("os.environ.get", re.compile(r"os\.environ\.get\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]")),
    ("os.environ[]", re.compile(r"os\.environ\[\s*['\"]([A-Z][A-Z0-9_]+)['\"]\s*\]")),
    ("process.env", re.compile(r"process\.env\.([A-Z][A-Z0-9_]+)")),
    ("process.env[]", re.compile(r"process\.env\[\s*['\"]([A-Z][A-Z0-9_]+)['\"]\s*\]")),
    ("import.meta.env", re.compile(r"import\.meta\.env\.([A-Z][A-Z0-9_]+)")),
    ("test setenv", re.compile(r"monkeypatch\.(?:setenv|delenv)\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]")),
)
ENV_HELPER_PATTERNS = (
    ("env helper", re.compile(r"\b_env(?:_bool|_int|_price_id)?\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]")),
    ("env url helper", re.compile(r"\b_env_url\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]")),
)
CONFIG_TOKEN_RE = re.compile(r"\b([A-Z][A-Z0-9_]{2,})\b")
SHELL_ENV_RE = re.compile(r"\$\{([A-Z][A-Z0-9_]+)(?:[:}])")
STRING_ENV_RE = re.compile(r"['\"]([A-Z][A-Z0-9_]+)['\"]")
STRING_ENV_CONTEXT_RE = re.compile(
    r"(os\.getenv|os\.environ|process\.env|_env|env_key|env_keys|ENV_VAR|ENV_VARS|"
    r"ENABLE_FLAGS|REQUIRED_ENV_VARS|PRICE_ENV|missing_env_vars|support_info)"
)
ENV_STRING_PREFIXES = (
    "CT_",
    "DATA_ENRICHMENT_",
    "EMAIL_",
    "FMP_",
    "GOOGLE_",
    "NEXT_PUBLIC_",
    "POSTMARK_",
    "STRIPE_",
)
ENV_STRING_EXACT = {"API_BASE", "DATABASE_URL", "ENV", "FLY_APP_NAME", "NODE_ENV", "TMPDIR"}

TOKEN_IGNORE = {
    "API",
    "CSV",
    "DB",
    "DELETE",
    "ETF",
    "FALSE",
    "GET",
    "HTML",
    "HTTP",
    "HTTPS",
    "ID",
    "JSON",
    "LOCAL_DEV",
    "NO",
    "PATCH",
    "POST",
    "PUT",
    "README",
    "SQL",
    "TRUE",
    "UI",
    "URL",
    "USD",
    "UTC",
    "YES",
}


def is_example_env_file(path: Path) -> bool:
    name = path.name.lower()
    return name == ".env.example" or name.endswith(".env.example") or "env.example" in name


def should_scan(path: Path) -> bool:
    if path.as_posix().endswith("scripts/check_env_usage.py"):
        return False
    if any(part in SKIP_DIRS for part in path.parts):
        return False
    if path.suffix.lower() in SKIP_SUFFIXES:
        return False
    if path.name.startswith(".env") and not is_example_env_file(path):
        return False
    return True


def is_configish(path: Path) -> bool:
    rel = path.as_posix()
    name = path.name.lower()
    return (
        ".github/workflows/" in rel
        or "/docs/" in rel
        or name in {"dockerfile", "fly.toml", "crontab", "local_dev.md", "readme.md"}
        or name.endswith((".yml", ".yaml", ".env.example"))
        or "runbook" in name
        or "deployment" in name
    )


def is_shellish(path: Path) -> bool:
    return path.suffix.lower() == ".sh" or path.name.lower() in {"crontab", "dockerfile"}


def iter_files(root: Path) -> Iterable[Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [dirname for dirname in dirnames if dirname not in SKIP_DIRS]
        for filename in filenames:
            path = Path(dirpath, filename)
            if should_scan(path):
                yield path


def maybe_add(results: dict[str, list[dict[str, object]]], name: str, path: Path, root: Path, line_no: int, kind: str) -> None:
    if not ENV_NAME_RE.match(name) or name in TOKEN_IGNORE:
        return
    if name.endswith("_"):
        return
    if name.startswith(("FMP_HOUSE_", "FMP_SENATE_")):
        return
    results.setdefault(name, []).append(
        {
            "file": path.relative_to(root).as_posix(),
            "line": line_no,
            "kind": kind,
        }
    )


def scan(root: Path) -> dict[str, list[dict[str, object]]]:
    results: dict[str, list[dict[str, object]]] = {}
    for path in iter_files(root):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for line_no, line in enumerate(text.splitlines(), start=1):
            for kind, pattern in (*DIRECT_PATTERNS, *ENV_HELPER_PATTERNS):
                for match in pattern.finditer(line):
                    maybe_add(results, match.group(1), path, root, line_no, kind)
            if STRING_ENV_CONTEXT_RE.search(line):
                for match in STRING_ENV_RE.finditer(line):
                    maybe_add(results, match.group(1), path, root, line_no, "env string")
            for match in STRING_ENV_RE.finditer(line):
                name = match.group(1)
                if name in ENV_STRING_EXACT or name.startswith(ENV_STRING_PREFIXES):
                    maybe_add(results, name, path, root, line_no, "env-like string")
            if is_shellish(path):
                for match in SHELL_ENV_RE.finditer(line):
                    maybe_add(results, match.group(1), path, root, line_no, "shell expansion")
            if is_configish(path):
                for match in CONFIG_TOKEN_RE.finditer(line):
                    name = match.group(1)
                    if "_" in name or name.startswith("NEXT_PUBLIC_"):
                        maybe_add(results, name, path, root, line_no, "config/docs token")
    return {name: refs for name, refs in sorted(results.items())}


def read_names(path: Path) -> list[str]:
    names: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        name = re.split(r"[\s=,]+", line, maxsplit=1)[0].strip()
        if ENV_NAME_RE.match(name):
            names.append(name)
    return names


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan env var references without reading secret values.")
    parser.add_argument("--root", default=".", help="Repository root to scan.")
    parser.add_argument("--known", help="Optional newline-delimited list of expected env var names.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a text report.")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    refs = scan(root)
    known = read_names(Path(args.known)) if args.known else []
    unknown_from_known = [name for name in known if name not in refs]

    if args.json:
        print(json.dumps({"referenced": refs, "unknown_from_provided_list": unknown_from_known}, indent=2))
    else:
        print("Referenced env vars:")
        for name, locations in refs.items():
            rendered = ", ".join(f"{item['file']}:{item['line']}" for item in locations[:8])
            suffix = "" if len(locations) <= 8 else f" (+{len(locations) - 8} more)"
            print(f"- {name}: {rendered}{suffix}")
        if known:
            print()
            print("Unknown vars from provided list:")
            for name in unknown_from_known:
                print(f"- {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
