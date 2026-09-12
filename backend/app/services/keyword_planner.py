"""Google Ads historical keyword metrics. No campaign/budget mutation methods.

Google's OAuth scope is broad; our transport deliberately allowlists only token,
identity and two read-only Ads operations. Separate credentials from Search Console.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests
from cryptography.fernet import Fernet, InvalidToken
from fastapi import HTTPException
from sqlalchemy import text

from app.models import UserAccount
from app.services.search_console import _credentials

SCOPE = "https://www.googleapis.com/auth/adwords"
CUSTOMER = "4533759595"
API = "https://googleads.googleapis.com/v24"
HISTORICAL = f"{API}/customers/{CUSTOMER}:generateKeywordHistoricalMetrics"
ACCOUNT = f"{API}/customers/{CUSTOMER}/googleAds:search"
TARGETING = {"country": "United States", "language": "English", "network": "Google Search"}
NOTE = "Google Keyword Planner estimates, averaged over the returned months and including close variants; not exact counts or guaranteed SEO traffic. Advertising competition is not organic SEO difficulty."
SEEDS = ["institutional ownership", "insider buying", "congress stock trades", "Nvidia institutional ownership"]


def ensure_schema(db):
    db.execute(text("""CREATE TABLE IF NOT EXISTS research_ads_connection (
        id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL, email TEXT NOT NULL,
        refresh_encrypted TEXT NOT NULL, connected_at TEXT NOT NULL,
        last_attempt_at TEXT, last_sync_at TEXT, error TEXT)"""))
    db.execute(text("""CREATE TABLE IF NOT EXISTS research_ads_oauth (
        state_hash TEXT PRIMARY KEY, owner_id INTEGER NOT NULL,
        verifier_encrypted TEXT NOT NULL, expires_at TEXT NOT NULL)"""))
    db.execute(text("""CREATE TABLE IF NOT EXISTS research_keyword_metrics (
        keyword TEXT PRIMARY KEY, fetched_at TEXT NOT NULL, metrics_json TEXT NOT NULL)"""))
    db.commit()


def _cipher():
    secret = os.getenv("APP_SESSION_SECRET", "")
    if len(secret) < 32:
        raise HTTPException(503, "Keyword Planner requires a strong server session secret.")
    key = hmac.new(secret.encode(), b"walnut-keyword-planner-encryption-v1", hashlib.sha256).digest()
    return Fernet(base64.urlsafe_b64encode(key))


def _decrypt(value):
    try:
        return _cipher().decrypt(value.encode()).decode()
    except InvalidToken:
        raise HTTPException(409, "Reconnect Keyword Planner after the server encryption key change.") from None


def _request(method, url, **kwargs):
    allowed = {("POST", "https://oauth2.googleapis.com/token"),
               ("GET", "https://openidconnect.googleapis.com/v1/userinfo"),
               ("POST", HISTORICAL), ("POST", ACCOUNT)}
    if (method, url) not in allowed:
        raise ValueError("Operation is not a read-only Keyword Planner operation")
    try:
        response = requests.request(method, url, timeout=(5, 35), allow_redirects=False, **kwargs)
        if response.status_code >= 300:
            # Provider bodies can contain credentials or submitted text: never expose them.
            if response.status_code in (400, 401, 403):
                raise HTTPException(409, "Google Ads denied the request. Check Basic API access, account access and the Google Ads permission; reconnect if needed.")
            if response.status_code == 429:
                raise HTTPException(429, "Google Keyword Planner quota reached. Cached data is retained; retry later.")
            raise HTTPException(502, "Google Keyword Planner is temporarily unavailable. Cached data is retained.")
        return response.json()
    except (requests.RequestException, ValueError):
        raise HTTPException(502, "Google Keyword Planner could not be reached. Cached data is retained.") from None


def start_connection(db, admin):
    ensure_schema(db)
    client, _, redirect = _credentials()
    state, verifier = "gads_" + secrets.token_urlsafe(32), secrets.token_urlsafe(48)
    now = datetime.now(timezone.utc)
    db.execute(text("DELETE FROM research_ads_oauth WHERE owner_id=:owner OR expires_at < :now"),
               {"owner": admin.id, "now": now.isoformat()})
    db.execute(text("INSERT INTO research_ads_oauth VALUES (:state,:owner,:verifier,:expires)"),
               {"state": hashlib.sha256(state.encode()).hexdigest(), "owner": admin.id,
                "verifier": _cipher().encrypt(verifier.encode()).decode(), "expires": (now + timedelta(minutes=10)).isoformat()})
    db.commit()
    params = {"client_id": client, "redirect_uri": redirect, "response_type": "code",
              "scope": "openid email " + SCOPE, "access_type": "offline", "prompt": "consent select_account",
              "state": state, "login_hint": "moore11j@gmail.com", "code_challenge_method": "S256",
              "code_challenge": base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")}
    return {"authorization_url": "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)}


def complete_connection(db, admin, code, state):
    ensure_schema(db)
    row = db.execute(text("""DELETE FROM research_ads_oauth WHERE state_hash=:state
        AND owner_id=:owner AND expires_at > :now RETURNING verifier_encrypted"""),
        {"state": hashlib.sha256(state.encode()).hexdigest(), "owner": admin.id,
         "now": datetime.now(timezone.utc).isoformat()}).mappings().first()
    db.commit()
    if not row:
        raise HTTPException(400, "Keyword Planner authorization expired or was already used. Start Connect again.")
    client, secret, redirect = _credentials()
    token = _request("POST", "https://oauth2.googleapis.com/token", data={
        "client_id": client, "client_secret": secret, "redirect_uri": redirect, "code": code,
        "grant_type": "authorization_code", "code_verifier": _decrypt(row["verifier_encrypted"])})
    if SCOPE not in token.get("scope", "").split() or not token.get("refresh_token") or not token.get("access_token"):
        raise HTTPException(409, "Google Ads permission and offline access are required. Reconnect and select the permission.")
    headers = {"Authorization": "Bearer " + token["access_token"]}
    identity = _request("GET", "https://openidconnect.googleapis.com/v1/userinfo", headers=headers)
    if not identity.get("email_verified") or not identity.get("email"):
        raise HTTPException(409, "A verified Google email is required.")
    account = _request("POST", ACCOUNT, headers=headers, json={"query": "SELECT customer.id FROM customer LIMIT 1"})
    if not any(str(r.get("customer", {}).get("id")) == CUSTOMER for r in account.get("results", [])):
        raise HTTPException(403, "This Google account cannot access Walnut's Google Ads account 453-375-9595.")
    # Verify keyword service access, not just general Ads access, before reporting connected.
    _request("POST", HISTORICAL, headers=headers, json=_payload([SEEDS[0]]))
    db.execute(text("""INSERT INTO research_ads_connection
        (id,owner_id,email,refresh_encrypted,connected_at) VALUES (1,:owner,:email,:token,:now)
        ON CONFLICT(id) DO UPDATE SET owner_id=:owner,email=:email,refresh_encrypted=:token,
        connected_at=:now,last_attempt_at=NULL,last_sync_at=NULL,error=NULL"""),
        {"owner": admin.id, "email": identity["email"], "token": _cipher().encrypt(token["refresh_token"].encode()).decode(),
         "now": datetime.now(timezone.utc).isoformat()})
    db.execute(text("DELETE FROM research_keyword_metrics"))
    db.commit()
    lookup(db, SEEDS)
    return get_status(db)


def disconnect(db):
    ensure_schema(db)
    for table in ("research_ads_connection", "research_ads_oauth", "research_keyword_metrics"):
        db.execute(text(f"DELETE FROM {table}"))
    db.commit()
    return get_status(db)


def normalize(value):
    return " ".join(str(value).lower().split())


def _keywords(values):
    # Editorial search phrases only: no emails, URLs or individual visitor records.
    result = []
    for value in values:
        value = normalize(value)
        if value and len(value) <= 80 and len(value.split()) <= 10 and not re.search(r"[@:/\\]", value) and value not in result:
            result.append(value)
    return result[:50]


def _payload(keywords):
    return {"keywords": keywords, "language": "languageConstants/1000",
            "geoTargetConstants": ["geoTargetConstants/2840"], "keywordPlanNetwork": "GOOGLE_SEARCH"}


def _number(value):
    return max(0, int(value)) if value is not None else None


def parse_metrics(result, keywords, now):
    mapped = {}
    for row in result.get("results", []):
        metrics = row.get("keywordMetrics") or {}
        aliases = [row.get("text", ""), *row.get("closeVariants", [])]
        item = {"google_keyword": row.get("text"), "close_variants": row.get("closeVariants", []),
                "avg_monthly_searches": _number(metrics.get("avgMonthlySearches")),
                "advertising_competition": metrics.get("competition"),
                "monthly_searches": [{"year": m.get("year"), "month": m.get("month"), "searches": _number(m.get("monthlySearches"))}
                                     for m in metrics.get("monthlySearchVolumes", [])],
                "fetched_at": now.isoformat(), "source": "Google Keyword Planner", "targeting": TARGETING}
        for alias in aliases:
            mapped[normalize(alias)] = item
    # Missing metrics remain null. Never fabricate zeros or map unrelated phrases.
    return {keyword: {"keyword": keyword, **mapped.get(keyword, {
        "google_keyword": None, "close_variants": [], "avg_monthly_searches": None,
        "monthly_searches": [], "fetched_at": now.isoformat(), "source": "Google Keyword Planner", "targeting": TARGETING})}
        for keyword in keywords}


def get_status(db):
    ensure_schema(db)
    row = db.execute(text("SELECT * FROM research_ads_connection WHERE id=1")).mappings().first()
    metrics = [json.loads(r[0]) for r in db.execute(text("SELECT metrics_json FROM research_keyword_metrics ORDER BY fetched_at DESC,keyword LIMIT 50"))]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    return {"configured": bool(os.getenv("GOOGLE_CLIENT_ID") and os.getenv("GOOGLE_CLIENT_SECRET") and len(os.getenv("APP_SESSION_SECRET", "")) >= 32),
            "connected": bool(row), "customer_id": CUSTOMER, "targeting": TARGETING, "note": NOTE,
            "email": row["email"] if row else None, "last_sync_at": row["last_sync_at"] if row else None,
            "error": row["error"] if row else None,
            "metrics": [{**m, "stale": m["fetched_at"] < cutoff} for m in metrics]}


def lookup(db, values, *, now=None):
    ensure_schema(db)
    now = now or datetime.now(timezone.utc)
    keywords = _keywords(values)
    row = db.execute(text("SELECT * FROM research_ads_connection WHERE id=1")).mappings().first()
    if not row or not keywords:
        return {}
    owner = db.get(UserAccount, row["owner_id"])
    if not owner or owner.role != "admin" or owner.deleted_at:
        return {}
    cached = {r["keyword"]: json.loads(r["metrics_json"]) for r in db.execute(text(
        "SELECT * FROM research_keyword_metrics WHERE fetched_at > :cutoff"),
        {"cutoff": (now - timedelta(days=30)).isoformat()}).mappings() if r["keyword"] in keywords}
    missing = [k for k in keywords if k not in cached]
    if not missing:
        return cached
    # Atomic 5-minute request claim bounds retries and parallel admin/cron calls.
    claim = db.execute(text("""UPDATE research_ads_connection SET last_attempt_at=:now WHERE id=1
        AND connected_at=:connected AND (last_attempt_at IS NULL OR last_attempt_at < :cutoff)"""),
        {"now": now.isoformat(), "connected": row["connected_at"], "cutoff": (now - timedelta(minutes=5)).isoformat()})
    db.commit()
    if claim.rowcount != 1:
        return cached
    try:
        client, secret, _ = _credentials()
        token = _request("POST", "https://oauth2.googleapis.com/token", data={
            "client_id": client, "client_secret": secret, "grant_type": "refresh_token",
            "refresh_token": _decrypt(row["refresh_encrypted"])})
        result = _request("POST", HISTORICAL, headers={"Authorization": "Bearer " + token["access_token"]}, json=_payload(missing))
        fresh = parse_metrics(result, missing, now)
        # Lock the connection version so an in-flight request cannot repopulate
        # credentials/data after disconnect or overwrite a replacement connection.
        active = db.execute(text("""UPDATE research_ads_connection SET last_sync_at=:now,error=NULL
            WHERE id=1 AND connected_at=:connected"""), {"now": now.isoformat(), "connected": row["connected_at"]})
        if active.rowcount != 1:
            db.rollback()
            return {}
        for keyword, metrics in fresh.items():
            db.execute(text("""INSERT INTO research_keyword_metrics VALUES (:keyword,:now,:metrics)
                ON CONFLICT(keyword) DO UPDATE SET fetched_at=:now,metrics_json=:metrics"""),
                {"keyword": keyword, "now": now.isoformat(), "metrics": json.dumps(metrics)})
        db.execute(text("DELETE FROM research_keyword_metrics WHERE fetched_at < :cutoff"), {"cutoff": (now-timedelta(days=90)).isoformat()})
        db.commit()
        return {**cached, **fresh}
    except Exception as exc:
        db.rollback()
        message = exc.detail if isinstance(exc, HTTPException) else "Keyword Planner lookup failed. Cached data is retained."
        db.execute(text("UPDATE research_ads_connection SET error=:error WHERE id=1 AND connected_at=:connected"),
                   {"error": message, "connected": row["connected_at"]})
        db.commit()
        return cached


def enrich_candidates(db, candidates):
    values = [k for c in candidates for k in [c.get("target_keyword", ""), *(c.get("secondary_keywords") or [])[:3]]]
    metrics = lookup(db, values)
    return [{**c, "keyword_metrics": metrics.get(normalize(c.get("target_keyword", ""))),
             "secondary_keyword_metrics": [metrics[normalize(k)] for k in (c.get("secondary_keywords") or [])[:3] if normalize(k) in metrics]}
            for c in candidates]


def priority_bonus(candidate):
    volume = (candidate.get("keyword_metrics") or {}).get("avg_monthly_searches")
    # No secondary-keyword summing: close variants can overlap. Quality wins.
    return min(10, int(math.log10(1 + volume) * 2)) if volume is not None else 0
