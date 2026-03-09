from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

LEGISLATORS_CURRENT_JSON = (
    "https://unitedstates.github.io/congress-legislators/legislators-current.json"
)

logger = logging.getLogger(__name__)
CACHE_ENV_VAR = "CONGRESS_METADATA_CACHE_PATH"
DEFAULT_CACHE_PATH = "/data/cache/legislators-current.json"


def _cache_path() -> Path:
    configured = os.getenv(CACHE_ENV_VAR, DEFAULT_CACHE_PATH)
    return Path(configured)


def _norm(value: str | None) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"[^a-z\s\-']", "", value.strip().lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _last_variants(last: str | None) -> list[str]:
    normalized = _norm(last)
    if not normalized:
        return []
    variants = [normalized]
    parts = normalized.split()
    if len(parts) > 1:
        variants.append(parts[-1])
    seen: set[str] = set()
    out: list[str] = []
    for item in variants:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _normalize_party(raw: str | None) -> str | None:
    if not raw:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    token = re.sub(r"[^a-z]", "", cleaned.lower())
    if token in {"d", "dem", "democrat", "democratic"}:
        return "Democrat"
    if token in {"r", "rep", "republican"}:
        return "Republican"
    if token in {"i", "ind", "independent"}:
        return "Independent"
    return cleaned


@dataclass(frozen=True)
class MemberMetadata:
    party: str | None
    chamber: str | None
    state: str | None


class CongressMetadataResolver:
    def __init__(self, rows: list[dict[str, Any]]):
        self._by_bioguide: dict[str, MemberMetadata] = {}
        self._by_house_district: dict[tuple[str, int], MemberMetadata] = {}
        self._by_name_state_chamber: dict[tuple[str, str, str, str], MemberMetadata] = {}
        self._by_name_chamber_unique: dict[tuple[str, str, str], MemberMetadata | None] = {}

        name_chamber_bucket: dict[tuple[str, str, str], list[MemberMetadata]] = {}

        for person in rows:
            ids = person.get("id", {}) or {}
            name = person.get("name", {}) or {}
            terms = person.get("terms", []) or []
            if not terms:
                continue

            term = terms[-1]
            term_type = (term.get("type") or "").strip().lower()
            chamber = "house" if term_type == "rep" else "senate" if term_type == "sen" else None
            if chamber is None:
                continue

            party = _normalize_party(term.get("party"))
            state = (term.get("state") or "").strip().upper() or None
            metadata = MemberMetadata(party=party, chamber=chamber, state=state)

            bioguide = (ids.get("bioguide") or "").strip()
            if bioguide:
                self._by_bioguide[bioguide] = metadata

            if chamber == "house" and state:
                district = term.get("district")
                if isinstance(district, int):
                    self._by_house_district[(state, district)] = metadata

            first = _norm(name.get("first"))
            last = _norm(name.get("last"))
            if first and last and state and chamber:
                self._by_name_state_chamber[(first, last, state, chamber)] = metadata

            if first and last and chamber:
                name_chamber_bucket.setdefault((first, last, chamber), []).append(metadata)

        for key, values in name_chamber_bucket.items():
            self._by_name_chamber_unique[key] = values[0] if len(values) == 1 else None

    @classmethod
    def load(cls, timeout_s: int = 30) -> "CongressMetadataResolver":
        cache_path = _cache_path()
        fetch_error: Exception | None = None

        try:
            response = requests.get(LEGISLATORS_CURRENT_JSON, timeout=timeout_s)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                raise RuntimeError("Unexpected legislator metadata payload format")

            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(data), encoding="utf-8")
            except Exception:
                logger.warning("Unable to write congress metadata cache at %s", cache_path, exc_info=True)

            return cls([row for row in data if isinstance(row, dict)])
        except Exception as exc:
            fetch_error = exc
            logger.warning(
                "Failed to fetch congress metadata from %s; attempting cache at %s",
                LEGISLATORS_CURRENT_JSON,
                cache_path,
                exc_info=True,
            )

        if cache_path.exists():
            try:
                cached_raw = json.loads(cache_path.read_text(encoding="utf-8"))
                if isinstance(cached_raw, list):
                    logger.info("Loaded congress metadata from cache at %s", cache_path)
                    return cls([row for row in cached_raw if isinstance(row, dict)])
                logger.warning("Congress metadata cache at %s had unexpected format", cache_path)
            except Exception:
                logger.warning("Failed to read congress metadata cache at %s", cache_path, exc_info=True)

        raise RuntimeError(
            "Unable to load congress metadata from remote source or cache. "
            f"Set {CACHE_ENV_VAR} to a readable cache file to run without live GitHub access."
        ) from fetch_error

    def resolve(
        self,
        *,
        bioguide_id: str | None,
        first_name: str | None,
        last_name: str | None,
        chamber: str | None,
        state: str | None,
        house_district: str | None = None,
    ) -> MemberMetadata | None:
        normalized_chamber = (chamber or "").strip().lower() or None
        normalized_state = (state or "").strip().upper() or None

        if bioguide_id:
            matched = self._by_bioguide.get(bioguide_id.strip())
            if matched:
                return matched

        if normalized_chamber == "house" and house_district:
            digits = "".join(ch for ch in house_district if ch.isdigit())
            if normalized_state and digits:
                matched = self._by_house_district.get((normalized_state, int(digits)))
                if matched:
                    return matched

        first = _norm(first_name)
        last_candidates = _last_variants(last_name)

        if first and normalized_state and normalized_chamber and last_candidates:
            for last in last_candidates:
                matched = self._by_name_state_chamber.get(
                    (first, last, normalized_state, normalized_chamber)
                )
                if matched:
                    return matched

        if first and normalized_chamber and last_candidates:
            for last in last_candidates:
                matched = self._by_name_chamber_unique.get((first, last, normalized_chamber))
                if matched:
                    return matched

        return None


@lru_cache(maxsize=1)
def get_congress_metadata_resolver() -> CongressMetadataResolver:
    return CongressMetadataResolver.load()
