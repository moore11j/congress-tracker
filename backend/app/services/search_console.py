"""Admin-only Search Console OAuth and bounded, read-only performance imports."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlencode, urlparse
from zoneinfo import ZoneInfo

import requests
from cryptography.fernet import Fernet, InvalidToken
from fastapi import HTTPException
from sqlalchemy import text

from app.models import UserAccount

SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"
PROPERTY = "sc-domain:walnutmarkets.com"
API = "https://www.googleapis.com/webmasters/v3/sites/" + quote(PROPERTY, safe="")
NOTE = "Measured Google clicks, impressions, CTR and average position for this property, not total keyword search volume. Google returns top rows and omits some queries."


def ensure_schema(db):
    db.execute(text("""CREATE TABLE IF NOT EXISTS research_gsc_connection (
        id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL, email TEXT NOT NULL,
        refresh_encrypted TEXT NOT NULL, connected_at TEXT NOT NULL,
        last_attempt_at TEXT, last_sync_at TEXT, error TEXT, snapshot_json TEXT)"""))
    db.execute(text("""CREATE TABLE IF NOT EXISTS research_gsc_oauth (
        state_hash TEXT PRIMARY KEY, owner_id INTEGER NOT NULL,
        verifier_encrypted TEXT NOT NULL, expires_at TEXT NOT NULL)"""))
    db.commit()


def _cipher():
    # Domain-separated encryption key; never use the development session default.
    secret = os.getenv("APP_SESSION_SECRET", "")
    if len(secret) < 32:
        raise HTTPException(503, "Search Console requires a strong server session secret.")
    key = hmac.new(secret.encode(), b"walnut-search-console-token-encryption-v1", hashlib.sha256).digest()
    return Fernet(base64.urlsafe_b64encode(key))


def _credentials():
    from app.routers.accounts import _google_redirect_uri
    client = os.getenv("GOOGLE_CLIENT_ID", "").strip()
    secret = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
    if not client or not secret:
        raise HTTPException(503, "Google OAuth is not configured on the server.")
    return client, secret, _google_redirect_uri()


def _decrypt(value):
    try:
        return _cipher().decrypt(value.encode()).decode()
    except InvalidToken:
        raise HTTPException(409, "The server encryption key changed. Reconnect Search Console.") from None


def _request(method, url, **kwargs):
    # Never log provider bodies, authorization headers, codes or token-bearing URLs.
    try:
        response = requests.request(method, url, timeout=(5, 25), allow_redirects=False, **kwargs)
        if response.status_code >= 300:
            if response.status_code in (400, 401, 403):
                raise HTTPException(409, "Google access was denied or expired. Reconnect Search Console and grant read-only access.")
            raise HTTPException(502, "Google Search Console is temporarily unavailable. Try syncing later.")
        return response.json()
    except (requests.RequestException, ValueError):
        raise HTTPException(502, "Google Search Console could not be reached. Try syncing later.") from None


def start_connection(db, admin):
    ensure_schema(db)
    client, _, redirect = _credentials()
    cipher = _cipher()
    state = "gsc_" + secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(48)
    now = datetime.now(timezone.utc)
    db.execute(text("DELETE FROM research_gsc_oauth WHERE owner_id=:owner OR expires_at < :now"),
               {"owner": admin.id, "now": now.isoformat()})
    db.execute(text("""INSERT INTO research_gsc_oauth VALUES (:state, :owner, :verifier, :expires)"""),
               {"state": hashlib.sha256(state.encode()).hexdigest(), "owner": admin.id,
                "verifier": cipher.encrypt(verifier.encode()).decode(), "expires": (now + timedelta(minutes=10)).isoformat()})
    db.commit()
    params = {"client_id": client, "redirect_uri": redirect, "response_type": "code",
              "scope": "openid email " + SCOPE, "access_type": "offline", "prompt": "consent select_account",
              "state": state, "login_hint": "moore11j@gmail.com", "code_challenge_method": "S256",
              "code_challenge": base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")}
    return {"authorization_url": "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)}


def complete_connection(db, admin, code, state):
    ensure_schema(db)
    digest = hashlib.sha256(state.encode()).hexdigest()
    # Atomic consume prevents replay and binds the flow to the logged-in admin.
    row = db.execute(text("""DELETE FROM research_gsc_oauth WHERE state_hash=:state
        AND owner_id=:owner AND expires_at > :now RETURNING verifier_encrypted"""),
        {"state": digest, "owner": admin.id, "now": datetime.now(timezone.utc).isoformat()}).mappings().first()
    db.commit()
    if not row:
        raise HTTPException(400, "Search Console authorization expired or was already used. Start Connect again.")
    client, secret, redirect = _credentials()
    cipher = _cipher()
    token = _request("POST", "https://oauth2.googleapis.com/token", data={
        "client_id": client, "client_secret": secret, "redirect_uri": redirect, "code": code,
        "grant_type": "authorization_code", "code_verifier": _decrypt(row["verifier_encrypted"])})
    if SCOPE not in token.get("scope", "").split() or not token.get("refresh_token") or not token.get("access_token"):
        raise HTTPException(409, "Google did not grant offline read-only access. Reconnect and select the Search Console permission.")
    headers = {"Authorization": "Bearer " + token["access_token"]}
    identity = _request("GET", "https://openidconnect.googleapis.com/v1/userinfo", headers=headers)
    if not identity.get("email_verified") or not identity.get("email"):
        raise HTTPException(409, "A verified Google email is required.")
    site = _request("GET", API, headers=headers)
    if site.get("permissionLevel") not in {"siteOwner", "siteFullUser", "siteRestrictedUser"}:
        raise HTTPException(403, "This Google account cannot read the walnutmarkets.com Search Console property.")
    db.execute(text("""INSERT INTO research_gsc_connection
        (id, owner_id, email, refresh_encrypted, connected_at) VALUES (1,:owner,:email,:token,:now)
        ON CONFLICT(id) DO UPDATE SET owner_id=:owner, email=:email, refresh_encrypted=:token,
        connected_at=:now, last_attempt_at=NULL, last_sync_at=NULL, error=NULL, snapshot_json=NULL"""),
        {"owner": admin.id, "email": identity["email"], "token": cipher.encrypt(token["refresh_token"].encode()).decode(),
         "now": datetime.now(timezone.utc).isoformat()})
    db.commit()
    return get_status(db)


def disconnect(db):
    ensure_schema(db)
    # Local removal only: revoking this shared Google OAuth client could also
    # revoke the user's ordinary Walnut login grant. Explain this in the UI.
    db.execute(text("DELETE FROM research_gsc_connection"))
    db.execute(text("DELETE FROM research_gsc_oauth"))
    db.commit()
    return get_status(db)


def _window(now):
    end = now.astimezone(ZoneInfo("America/Los_Angeles")).date() - timedelta(days=3)
    return end - timedelta(days=27), end


def _rows(headers, start, end, dimension):
    rows = []
    truncated = False
    for offset in (0, 25000):
        result = _request("POST", API + "/searchAnalytics/query", headers=headers, json={
            "startDate": str(start), "endDate": str(end), "dimensions": [dimension],
            "type": "web", "dataState": "final", "rowLimit": 25000, "startRow": offset})
        batch = result.get("rows", [])
        for row in batch:
            rows.append({dimension: row["keys"][0], "clicks": row["clicks"], "impressions": row["impressions"],
                         "ctr": row["ctr"], "position": row["position"]})
        if len(batch) < 25000:
            break
        truncated = offset == 25000
    return rows, truncated


def feedback(snapshot):
    prior = {row["page"]: row for row in snapshot.get("previous_pages", [])}
    result = []
    for row in snapshot.get("pages", []):
        parsed = urlparse(row["page"])
        if parsed.hostname not in {"walnutmarkets.com", "www.walnutmarkets.com", "app.walnutmarkets.com"} or not parsed.path.startswith("/research/"):
            continue
        old = prior.get(row["page"])
        reason = None
        if old and old["clicks"] >= 10 and row["clicks"] < old["clicks"] * .7:
            reason = "Clicks fell more than 30% versus the preceding 28 days. Review freshness and intent."
        elif row["impressions"] >= 100 and row["ctr"] < .02:
            reason = "At least 100 impressions with CTR below 2%. Review the title and preview against the query intent."
        elif row["impressions"] >= 50 and 5 <= row["position"] <= 20:
            reason = "Visible in positions 5–20. Consider deeper evidence and relevant internal links."
        if reason:
            result.append({**row, "previous_clicks": old["clicks"] if old else None, "recommendation": reason})
    return sorted(result, key=lambda row: -row["impressions"])[:20]


def get_status(db):
    ensure_schema(db)
    row = db.execute(text("SELECT * FROM research_gsc_connection WHERE id=1")).mappings().first()
    configured = bool(os.getenv("GOOGLE_CLIENT_ID") and os.getenv("GOOGLE_CLIENT_SECRET") and len(os.getenv("APP_SESSION_SECRET", "")) >= 32)
    base = {"configured": configured, "connected": bool(row), "property": PROPERTY, "note": NOTE,
            "keyword_volume_connected": False}
    if not row:
        return base
    snapshot = json.loads(row["snapshot_json"] or "{}")
    last_sync = row["last_sync_at"]
    stale = not last_sync or datetime.fromisoformat(last_sync) < datetime.now(timezone.utc) - timedelta(days=3)
    return {**base, "email": row["email"], "last_sync_at": last_sync, "last_attempt_at": row["last_attempt_at"],
            "error": row["error"], "stale": stale, "period": snapshot.get("period"),
            "row_cap_reached": snapshot.get("row_cap_reached", False),
            "queries": sorted(snapshot.get("queries", []), key=lambda r: -r["impressions"])[:20],
            "recommendations": feedback(snapshot)}


def planning_signals(db):
    status = get_status(db)
    if not status["connected"] or status.get("stale") or status.get("error"):
        return {}
    # Aggregate query metrics only, no visitor identities. Exclude low-count and
    # email-like queries from the external editorial prompt.
    return {"source": "Google Search Console", "period": status["period"],
            "queries": [r for r in status["queries"] if r["impressions"] >= 10 and "@" not in r["query"] and len(r["query"]) <= 240]}


def sync(db, *, now=None, force=False):
    ensure_schema(db)
    now = now or datetime.now(timezone.utc)
    row = db.execute(text("SELECT * FROM research_gsc_connection WHERE id=1")).mappings().first()
    if not row:
        return {"status": "not_connected"}
    # Daily on the existing five-minute worker. Manual requests are also bounded
    # to one attempt per five minutes. Claim before network calls across workers.
    cutoff = (now - timedelta(minutes=5) if force else now.astimezone(ZoneInfo("America/Los_Angeles")).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)).isoformat()
    claim = db.execute(text("""UPDATE research_gsc_connection SET last_attempt_at=:now WHERE id=1
        AND connected_at=:connected AND (last_attempt_at IS NULL OR last_attempt_at < :cutoff)"""),
        {"now": now.isoformat(), "cutoff": cutoff, "connected": row["connected_at"]})
    db.commit()
    if claim.rowcount != 1:
        return {"status": "already_attempted"}
    try:
        owner = db.get(UserAccount, row["owner_id"])
        if not owner or owner.role != "admin" or owner.deleted_at:
            raise HTTPException(409, "The connecting Walnut administrator is inactive. Reconnect with an active administrator.")
        client, secret, _ = _credentials()
        token = _request("POST", "https://oauth2.googleapis.com/token", data={"client_id": client, "client_secret": secret,
            "grant_type": "refresh_token", "refresh_token": _decrypt(row["refresh_encrypted"])})
        headers = {"Authorization": "Bearer " + token["access_token"]}
        start, end = _window(now)
        snapshot = {"period": {"start": str(start), "end": str(end)}, "row_cap_reached": False}
        for name, dimension, shift in [("queries", "query", 0), ("pages", "page", 0), ("previous_pages", "page", 28)]:
            snapshot[name], truncated = _rows(headers, start-timedelta(days=shift), end-timedelta(days=shift), dimension)
            snapshot["row_cap_reached"] |= truncated
        # All requests must succeed before replacing the previous good snapshot.
        db.execute(text("""UPDATE research_gsc_connection SET snapshot_json=:snapshot,
            last_sync_at=:now, error=NULL WHERE id=1 AND connected_at=:connected"""),
            {"snapshot": json.dumps(snapshot), "now": now.isoformat(), "connected": row["connected_at"]})
        db.commit()
        return {"status": "synced"}
    except Exception as exc:
        db.rollback()
        message = exc.detail if isinstance(exc, HTTPException) else "Search Console sync failed. Reconnect if the server encryption key changed."
        db.execute(text("UPDATE research_gsc_connection SET error=:error WHERE id=1 AND connected_at=:connected"),
                   {"error": message, "connected": row["connected_at"]})
        db.commit()
        return {"status": "failed"}
