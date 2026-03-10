from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
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

_NAME_SUFFIXES = {
    "jr",
    "sr",
    "ii",
    "iii",
    "iv",
    "v",
}

_FIRST_NAME_EQUIVALENTS = {
    "jim": {"james"},
    "james": {"jim"},
    "jd": {"j d", "james"},
    "j d": {"jd", "james"},
}

# Temporary technical debt: tiny safety net for known FMP naming/ID edge-cases.
_PARTY_OVERRIDE_BY_NAME: dict[tuple[str, str, str, str], str] = {
    ("val", "hoyle", "house", "OR"): "Democrat",
    ("marjorie", "greene", "house", "GA"): "Republican",
    ("james", "justice", "senate", "WV"): "Republican",
    ("james", "banks", "senate", "IN"): "Republican",
    ("jim", "banks", "senate", "IN"): "Republican",
    ("marco", "rubio", "senate", "FL"): "Republican",
    ("tom", "carper", "senate", "DE"): "Democrat",
    ("mikie", "sherrill", "house", "NJ"): "Democrat",
    ("james", "vance", "senate", "OH"): "Republican",
    ("jd", "vance", "senate", "OH"): "Republican",
    ("j d", "vance", "senate", "OH"): "Republican",
    ("linda", "sanchez", "house", "CA"): "Democrat",
}


def _cache_path() -> Path:
    configured = os.getenv(CACHE_ENV_VAR, DEFAULT_CACHE_PATH)
    return Path(configured)


def _norm(value: str | None) -> str:
    if not value:
        return ""
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-z\s\-']", "", ascii_value.strip().lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _strip_suffix_tokens(tokens: list[str]) -> list[str]:
    if tokens and tokens[-1] in _NAME_SUFFIXES:
        return tokens[:-1]
    return tokens


def _first_variants(first: str | None) -> list[str]:
    normalized = _norm(first)
    if not normalized:
        return []

    tokens = _strip_suffix_tokens([token for token in normalized.split(" ") if token])
    if not tokens:
        return []

    variants = {
        " ".join(tokens),
        tokens[0],
    }

    compact = "".join(tokens)
    if compact:
        variants.add(compact)

    expanded = set(variants)
    for variant in list(expanded):
        expanded.update(_FIRST_NAME_EQUIVALENTS.get(variant, set()))

    return [variant for variant in expanded if variant]


def _last_variants(last: str | None) -> list[str]:
    normalized = _norm(last)
    if not normalized:
        return []
    variants = [normalized]
    parts = _strip_suffix_tokens(normalized.split())
    if parts:
        stripped = " ".join(parts)
        if stripped and stripped != normalized:
            variants.append(stripped)
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
    bioguide_id: str | None = None


class CongressMetadataResolver:
    def __init__(self, rows: list[dict[str, Any]]):
        self._by_bioguide: dict[str, MemberMetadata] = {}
        self._by_house_district: dict[tuple[str, int], MemberMetadata] = {}
        self._by_name_state_chamber: dict[tuple[str, str, str, str], MemberMetadata] = {}
        self._by_name_chamber_unique: dict[tuple[str, str, str], MemberMetadata | None] = {}
        self._by_name_unique: dict[tuple[str, str], MemberMetadata | None] = {}

        name_chamber_bucket: dict[tuple[str, str, str], list[MemberMetadata]] = {}
        name_bucket: dict[tuple[str, str], list[MemberMetadata]] = {}

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
            bioguide = (ids.get("bioguide") or "").strip() or None
            metadata = MemberMetadata(party=party, chamber=chamber, state=state, bioguide_id=bioguide)

            if bioguide:
                self._by_bioguide[bioguide] = metadata

            if chamber == "house" and state:
                district = term.get("district")
                if isinstance(district, int):
                    self._by_house_district[(state, district)] = metadata

            first_variants = set(_first_variants(name.get("first")))
            first_variants.update(_first_variants(name.get("nickname")))
            first_variants.update(_first_variants(name.get("official_full")))
            last = _norm(name.get("last"))
            if first_variants and last and state and chamber:
                for first in first_variants:
                    self._by_name_state_chamber[(first, last, state, chamber)] = metadata

            if first_variants and last and chamber:
                for first in first_variants:
                    name_chamber_bucket.setdefault((first, last, chamber), []).append(metadata)
                    name_bucket.setdefault((first, last), []).append(metadata)

        for key, values in name_chamber_bucket.items():
            self._by_name_chamber_unique[key] = values[0] if len(values) == 1 else None
        for key, values in name_bucket.items():
            self._by_name_unique[key] = values[0] if len(values) == 1 else None

    @classmethod
    def load(cls, timeout_s: int = 30) -> "CongressMetadataResolver":
        cache_path = _cache_path()
        fetch_error: Exception | None = None

        logger.info(
            "Starting congress metadata fetch from %s (timeout=%ss)",
            LEGISLATORS_CURRENT_JSON,
            timeout_s,
        )
        try:
            response = requests.get(LEGISLATORS_CURRENT_JSON, timeout=timeout_s)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                raise RuntimeError("Unexpected legislator metadata payload format")

            logger.info("Congress metadata fetch succeeded with %d rows", len(data))
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(data), encoding="utf-8")
                logger.info("Updated congress metadata cache at %s", cache_path)
            except Exception:
                logger.warning("Unable to write congress metadata cache at %s", cache_path, exc_info=True)

            return cls([row for row in data if isinstance(row, dict)])
        except Exception as exc:
            fetch_error = exc
            logger.warning(
                "Congress metadata fetch failed from %s; attempting cache fallback at %s",
                LEGISLATORS_CURRENT_JSON,
                cache_path,
                exc_info=True,
            )

        if cache_path.exists():
            logger.info("Congress metadata cache hit at %s", cache_path)
            try:
                cached_raw = json.loads(cache_path.read_text(encoding="utf-8"))
                if isinstance(cached_raw, list):
                    logger.warning(
                        "Using stale congress metadata cache fallback at %s because remote fetch failed",
                        cache_path,
                    )
                    return cls([row for row in cached_raw if isinstance(row, dict)])
                logger.warning("Congress metadata cache at %s had unexpected format", cache_path)
            except Exception:
                logger.warning("Failed to read congress metadata cache at %s", cache_path, exc_info=True)
        else:
            logger.info("Congress metadata cache miss at %s", cache_path)

        raise RuntimeError(
            "Unable to load congress metadata: remote fetch failed and no usable cache was available. "
            f"Remote URL: {LEGISLATORS_CURRENT_JSON}. "
            f"Set {CACHE_ENV_VAR} to a readable JSON cache file (list payload) for offline/retry scenarios."
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
        full_name: str | None = None,
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

        first_candidates = _first_variants(first_name)
        last_candidates = _last_variants(last_name)

        if full_name and (not first_candidates or not last_candidates):
            normalized_full = _norm(full_name)
            tokens = [token for token in normalized_full.split(" ") if token]
            tokens = _strip_suffix_tokens(tokens)
            if tokens:
                if not first_candidates:
                    first_candidates = _first_variants(tokens[0])
                if not last_candidates:
                    last_candidates = _last_variants(tokens[-1])

        if first_candidates and normalized_state and normalized_chamber and last_candidates:
            for first in first_candidates:
                for last in last_candidates:
                    matched = self._by_name_state_chamber.get(
                        (first, last, normalized_state, normalized_chamber)
                    )
                    if matched:
                        return matched

        if first_candidates and normalized_chamber and last_candidates:
            for first in first_candidates:
                for last in last_candidates:
                    matched = self._by_name_chamber_unique.get((first, last, normalized_chamber))
                    if matched:
                        return matched

        known = _resolve_known_member_override(
            first_candidates=first_candidates,
            last_candidates=last_candidates,
            state=normalized_state,
            by_name_state_chamber=self._by_name_state_chamber,
            by_name_unique=self._by_name_unique,
        )
        if known:
            return known

        return _resolve_party_override(
            first_candidates=first_candidates,
            last_candidates=last_candidates,
            chamber=normalized_chamber,
            state=normalized_state,
        )


def _resolve_party_override(
    *,
    first_candidates: list[str],
    last_candidates: list[str],
    chamber: str | None,
    state: str | None,
) -> MemberMetadata | None:
    if not chamber or not state:
        return None

    for first in first_candidates:
        for last in last_candidates:
            party = _PARTY_OVERRIDE_BY_NAME.get((first, last, chamber, state))
            if party:
                return MemberMetadata(party=party, chamber=chamber, state=state)
    return None


_CANONICAL_MEMBER_OVERRIDE_BY_NAME: dict[tuple[str, str], tuple[str, str]] = {
    ("marco", "rubio"): ("FL", "senate"),
    ("linda", "sanchez"): ("CA", "house"),
    ("james", "vance"): ("OH", "senate"),
    ("jd", "vance"): ("OH", "senate"),
    ("j d", "vance"): ("OH", "senate"),
}


def _resolve_known_member_override(
    *,
    first_candidates: list[str],
    last_candidates: list[str],
    state: str | None,
    by_name_state_chamber: dict[tuple[str, str, str, str], MemberMetadata],
    by_name_unique: dict[tuple[str, str], MemberMetadata | None],
) -> MemberMetadata | None:
    for first in first_candidates:
        for last in last_candidates:
            override = _CANONICAL_MEMBER_OVERRIDE_BY_NAME.get((first, last))
            if not override:
                continue
            override_state, override_chamber = override
            if state and state != override_state:
                continue
            matched = by_name_state_chamber.get((first, last, override_state, override_chamber))
            if matched:
                return matched

    for first in first_candidates:
        for last in last_candidates:
            if (first, last) not in _CANONICAL_MEMBER_OVERRIDE_BY_NAME:
                continue
            matched = by_name_unique.get((first, last))
            if matched:
                return matched
    return None


@lru_cache(maxsize=1)
def get_congress_metadata_resolver() -> CongressMetadataResolver:
    return CongressMetadataResolver.load()
