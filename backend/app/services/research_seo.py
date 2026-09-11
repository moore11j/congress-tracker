"""Bounded daily editorial planning. Never approves or publishes an article."""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException
from sqlalchemy import text

from app.models import UserAccount
from app.services import research_briefs as briefs

logger = logging.getLogger(__name__)
DEFAULTS = {"enabled": False, "draft_time": "07:00", "timezone": "America/Los_Angeles",
            "topics": "Institutional ownership, insider activity, congressional trades, stock research",
            "tickers": [], "minimum_score": 70}


def ensure_schema(db):
    db.execute(text("""CREATE TABLE IF NOT EXISTS research_seo_settings (
        id INTEGER PRIMARY KEY, config_json TEXT NOT NULL, owner_id INTEGER NOT NULL)"""))
    db.execute(text("""CREATE TABLE IF NOT EXISTS research_seo_runs (
        day TEXT PRIMARY KEY, status TEXT NOT NULL, campaign_id TEXT,
        detail_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"""))
    db.commit()


def _config(db):
    row = db.execute(text("SELECT * FROM research_seo_settings WHERE id = 1")).mappings().first()
    return ({**DEFAULTS, **json.loads(row["config_json"])}, row["owner_id"]) if row else (dict(DEFAULTS), None)


def save_settings(db, admin, payload):
    ensure_schema(db)
    config = {key: payload.get(key, value) for key, value in DEFAULTS.items()}
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", config["draft_time"]):
        raise HTTPException(422, "Choose a valid daily draft time.")
    try:
        ZoneInfo(config["timezone"])
    except (ZoneInfoNotFoundError, ValueError):
        raise HTTPException(422, "Choose a valid IANA timezone, such as America/Los_Angeles.")
    config["tickers"] = list(dict.fromkeys(str(t).strip().upper() for t in config["tickers"] if str(t).strip()))
    if any(not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,9}", ticker) for ticker in config["tickers"]):
        raise HTTPException(422, "Enter valid ticker symbols separated by commas.")
    db.execute(text("""INSERT INTO research_seo_settings (id, config_json, owner_id)
        VALUES (1, :config, :owner) ON CONFLICT (id) DO UPDATE
        SET config_json = :config, owner_id = :owner"""), {"config": json.dumps(config), "owner": admin.id})
    db.commit()
    return get_status(db)


def customer_interest(db, now):
    # Only aggregate exact, known security symbols. Never send free-form queries,
    # names, emails, user IDs, or individual search history to the model.
    rows = db.execute(text("""SELECT s.symbol AS ticker, COUNT(*) AS searches
        FROM search_query_logs q JOIN securities s ON UPPER(q.normalized_query) = s.symbol
        WHERE q.created_at >= :since AND q.result_count > 0
        GROUP BY s.symbol HAVING COUNT(*) >= 3
        ORDER BY COUNT(*) DESC, s.symbol LIMIT 12"""), {"since": now - timedelta(days=30)}).mappings().all()
    return [{"ticker": row["ticker"], "searches": row["searches"]} for row in rows
            if re.fullmatch(r"[A-Z][A-Z0-9.-]{0,9}", row["ticker"] or "")]


def _query_key(value):
    return " ".join(re.findall(r"[a-z0-9]+", str(value).lower()))


def _same_topic(candidate, old):
    if isinstance(old, dict):
        # The same question about a different company is a different article.
        if old.get("ticker") and candidate.get("ticker") != old["ticker"]:
            return False
        old = old.get("target_keyword", "")
    return SequenceMatcher(None, _query_key(candidate.get("target_keyword")), _query_key(old)).ratio() >= 0.85


def rank_candidates(candidates, interest, recent, minimum_score):
    demand = {row["ticker"]: row["searches"] for row in interest}
    ranked = []
    for candidate in candidates:
        # Legacy non-ticker campaigns still produce a manual placeholder, not a
        # researched article. Never send those through unattended generation.
        if candidate.get("content_type") != "ticker" or not candidate.get("ticker"):
            continue
        key = _query_key(candidate.get("target_keyword", ""))
        if not key or not candidate.get("source_urls") or not candidate.get("walnut_angle"):
            continue
        if any(_same_topic(candidate, old) for old in recent):
            continue
        # Editorial score is a heuristic, not measured volume or promised traffic.
        base = max(0, min(100, int(candidate.get("opportunity_score") or 0)))
        searches = demand.get(candidate.get("ticker"), 0)
        bonus = min(10, searches // 3)
        score = min(100, base + bonus)
        if score < minimum_score:
            continue
        ranked.append({**candidate, "priority_score": score, "customer_searches_30d": searches,
                       "selection_reason": f"Editorial score {base}/100; {searches} matching on-site search events in 30 days. "
                       + str(candidate.get("rationale") or "")})
    return sorted(ranked, key=lambda row: (-row["priority_score"], row["target_keyword"]))


def _runs(db):
    rows = db.execute(text("SELECT * FROM research_seo_runs ORDER BY day DESC LIMIT 14")).mappings().all()
    runs = []
    for row in rows:
        run = {key: value for key, value in dict(row).items() if key != "detail_json"}
        run["detail"] = json.loads(row["detail_json"])
        if row["campaign_id"]:
            item = db.execute(text("""SELECT i.status AS article_status, i.research_article_id AS draft_id,
                e.status AS email_status FROM research_campaign_items i LEFT JOIN email_deliveries e
                ON e.idempotency_key = 'research-campaign-review:' || i.research_article_id
                WHERE i.campaign_id=:campaign LIMIT 1"""), {"campaign": row["campaign_id"]}).mappings().first()
            if item:
                run.update(dict(item))
        runs.append(run)
    return runs


def get_status(db):
    ensure_schema(db)
    config, _ = _config(db)
    return {"config": config, "runs": _runs(db), "review_email": os.getenv("RESEARCH_BRIEF_REVIEW_EMAIL", "jarod@walnutmarkets.com"),
            "drafts_per_day": 1, "metric_note": "On-site ticker search events and directional web signals, not verified Google search volume.",
            "worker_note": "Daily SEO requires the research SEO cron worker. Draft time starts generation; review email follows successful generation."}


def request_today(db):
    ensure_schema(db)
    config, owner = _config(db)
    if not owner:
        raise HTTPException(409, "Save Daily SEO settings first.")
    now = datetime.now(timezone.utc)
    day = now.astimezone(ZoneInfo(config["timezone"])).date().isoformat()
    db.execute(text("""INSERT INTO research_seo_runs (day, status, detail_json, created_at, updated_at)
        VALUES (:day, 'queued', '{}', :now, :now) ON CONFLICT (day) DO NOTHING"""), {"day": day, "now": now.isoformat()})
    db.commit()
    return get_status(db)


def _finish(db, day, status, detail):
    db.execute(text("UPDATE research_seo_runs SET status=:status, detail_json=:detail, updated_at=:now WHERE day=:day"),
               {"status": status, "detail": json.dumps(detail), "now": datetime.now(timezone.utc).isoformat(), "day": day})
    db.commit()


def run_daily_plan(db, *, now=None):
    ensure_schema(db)
    config, owner = _config(db)
    now = now or datetime.now(timezone.utc)
    local = now.astimezone(ZoneInfo(config["timezone"]))
    day = local.date().isoformat()
    queued = db.execute(text("SELECT day FROM research_seo_runs WHERE day=:day AND status='queued'"), {"day": day}).first()
    if not owner or (not queued and (not config["enabled"] or local.strftime("%H:%M") < config["draft_time"])):
        return {"status": "not_due"}
    if not queued:
        db.execute(text("""INSERT INTO research_seo_runs (day, status, detail_json, created_at, updated_at)
            VALUES (:day, 'queued', '{}', :now, :now) ON CONFLICT (day) DO NOTHING"""), {"day": day, "now": now.isoformat()})
        db.commit()
    # The durable claim is committed BEFORE spending. Crashes/failures are not
    # auto-retried; they remain visible for review, preventing paid retry loops.
    claim = db.execute(text("UPDATE research_seo_runs SET status='planning', updated_at=:now WHERE day=:day AND status='queued'"),
                       {"day": day, "now": now.isoformat()})
    db.commit()
    if claim.rowcount != 1:
        return {"status": "already_attempted"}
    detail = {}
    try:
        admin = db.get(UserAccount, owner)
        if not admin or admin.role != "admin" or admin.deleted_at:
            raise HTTPException(409, "Daily SEO owner must be an active administrator.")
        briefs.ensure_research_brief_store_schema(db)
        # Bound unattended backlog, not just daily spend.
        pending = db.execute(text("""SELECT COUNT(*) FROM research_campaign_items i
            JOIN research_seo_runs r ON r.campaign_id=i.campaign_id
            LEFT JOIN research_brief_drafts d ON d.id=i.research_article_id
            WHERE i.status IN ('pending','generating') OR d.status IN ('draft','scheduled_review')""")).scalar()
        if pending >= 7:
            _finish(db, day, "skipped", {"note": "Seven Daily SEO articles await review. Review those before generating more."})
            return {"status": "skipped"}
        interest = customer_interest(db, now)
        recent = [dict(row) for row in db.execute(text("""SELECT ticker, target_keyword FROM research_campaign_items
            WHERE created_at >= :since UNION SELECT primary_ticker AS ticker, target_keyword FROM research_brief_drafts
            WHERE status = 'published' OR updated_at >= :since"""), {"since": (now-timedelta(days=90)).isoformat()}).mappings()
                  if row["target_keyword"]]
        discovery = briefs.discover_research_keyword_opportunities(db, admin, {
            "seed_topics": [topic.strip()[:120] for topic in config["topics"].split(",") if topic.strip()][:12],
            "tickers": config["tickers"] or [row["ticker"] for row in interest], "max_candidates": 5,
            "customer_interest": interest, "excluded_queries": recent[-100:],
            "ticker_articles_only": True,
        })
        ranked = rank_candidates(discovery["items"], interest, recent, config["minimum_score"])
        detail = {"candidates": ranked, "customer_interest": interest, "market_note": discovery.get("market_note", "")}
        if not ranked:
            _finish(db, day, "skipped", {**detail, "note": "No distinct, sourced opportunity cleared the quality threshold. No draft generated."})
            return {"status": "skipped"}
        selected = ranked[0]
        detail["selected"] = selected
        # Link a stable ID before campaign creation commits. An interrupted run
        # can be traced even if the process exits immediately after that commit.
        campaign_id = f"rc_seo_{day}"
        db.execute(text("UPDATE research_seo_runs SET campaign_id=:campaign WHERE day=:day"), {"campaign": campaign_id, "day": day})
        db.commit()
        _finish(db, day, "planning", detail)
        campaign = briefs.create_research_campaign(db, admin, {
            "name": f"Daily SEO research · {day}", "theme": selected["recommended_theme"],
            "content_type": selected["content_type"], "tickers": [selected["ticker"]] if selected.get("ticker") else [],
            "topic": selected.get("topic"), "target_keyword": selected["target_keyword"],
            "search_intent": selected["search_intent"], "secondary_keywords": selected["secondary_keywords"],
            "source_opportunity_ids": [selected["id"]], "article_count": 1,
            "publish_start_at": (now + timedelta(days=1)).isoformat(),
            "editorial_brief": selected["walnut_angle"],
        }, campaign_id=campaign_id)
        _finish(db, day, "generating", detail)
        result = briefs.run_due_research_campaign_generation(db, campaign_id=campaign["id"], limit=1)
        detail["generation"] = result
        status = "draft_ready" if result["generated"] else "needs_attention"
        _finish(db, day, status, detail)
        return {"status": status}
    except Exception as exc:
        db.rollback()
        # HTTP errors are already sanitized by the research service. Never expose
        # provider response bodies or credentials in the admin run history.
        detail["error"] = str(exc.detail)[:500] if isinstance(exc, HTTPException) else "Daily SEO failed. Check server logs before retrying manually."
        _finish(db, day, "failed", detail)
        logger.exception("research_seo_failed day=%s", day)
        return {"status": "failed"}
