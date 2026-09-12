"""Durable video extensions to AI Growth. Binaries never enter the database."""
from __future__ import annotations

import json
import math
import os
import re
import uuid
from urllib.parse import urlencode
from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import text

from app.models import AiMarketingOpportunity, AiMarketingSetting
from app.services.growth_video_domain import BRIEF_FIELDS, DEFAULT_WEIGHTS, FORMATS, allowed_route, date_value, digest, now, score_opportunity

TABLES = {
    "growth_brief_versions": "id TEXT PRIMARY KEY, created_at TEXT NOT NULL, actor_id INTEGER NOT NULL, payload_json TEXT NOT NULL",
    "growth_content_opportunities": "id TEXT PRIMARY KEY, source_key TEXT NOT NULL UNIQUE, score FLOAT NOT NULL, created_at TEXT NOT NULL, payload_json TEXT NOT NULL",
    "growth_content_evidence": "id TEXT PRIMARY KEY, opportunity_id TEXT NOT NULL, payload_json TEXT NOT NULL",
    "growth_video_jobs": "id TEXT PRIMARY KEY, draft_id INTEGER NOT NULL UNIQUE, opportunity_id TEXT NOT NULL, owner_id INTEGER NOT NULL, status TEXT NOT NULL, revision INTEGER NOT NULL, parent_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, lease_until TEXT, lease_token TEXT, payload_json TEXT NOT NULL",
    "growth_video_assets": "id TEXT PRIMARY KEY, job_id TEXT NOT NULL, kind TEXT NOT NULL, object_key TEXT NOT NULL, created_at TEXT NOT NULL, payload_json TEXT NOT NULL",
    "growth_memory": "id TEXT PRIMARY KEY, job_id TEXT, opportunity_id TEXT, action TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, actor_id INTEGER NOT NULL, payload_json TEXT NOT NULL",
    "growth_video_budget": "day TEXT PRIMARY KEY, opportunities INTEGER NOT NULL DEFAULT 0, creatives INTEGER NOT NULL DEFAULT 0, renders INTEGER NOT NULL DEFAULT 0",
}
DEFAULTS = {"model": "gpt-6-astra", "voice": "", "narration_model": "eleven_multilingual_v2",
            "duration": 35, "opportunity_limit": 10, "render_limit": 3, "creative_limit": 6,
            "weights": DEFAULT_WEIGHTS, "template_ids": {key: "" for key in FORMATS},
            "capture_base_url": "https://walnutmarkets.com", "default_cta": "Read the full research on Walnut Markets."}


def ensure_schema(db):
    # Matches the existing research brief / SEO additive schema convention.
    for table, columns in TABLES.items():
        db.execute(text(f"CREATE TABLE IF NOT EXISTS {table} ({columns})"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_growth_jobs_status ON growth_video_jobs(status, created_at)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_growth_evidence_opportunity ON growth_content_evidence(opportunity_id)"))
    db.commit()


def uid(prefix):
    return prefix + "_" + uuid.uuid4().hex


def dumps(payload):
    return json.dumps(payload, default=str, sort_keys=True)


def setting(db, key, default):
    row = db.get(AiMarketingSetting, key)
    return json.loads(row.value) if row and row.value else default


def set_setting(db, key, value):
    row = db.get(AiMarketingSetting, key)
    if row is None:
        row = AiMarketingSetting(key=key, is_secret=False)
        db.add(row)
    row.value = dumps(value)


def config(db):
    return {**DEFAULTS, **setting(db, "GROWTH_VIDEO_CONFIG", {})}


def save_config(db, payload):
    if set(payload) - set(DEFAULTS):
        raise ValueError("Unknown video setting; credentials must be configured server-side.")
    merged = {**config(db), **payload}
    for key, low, high in [("duration", 15, 60), ("opportunity_limit", 1, 50), ("render_limit", 1, 20), ("creative_limit", 1, 40)]:
        if type(merged[key]) is not int or not low <= merged[key] <= high:
            raise ValueError(f"{key} must be between {low} and {high}.")
    if merged["capture_base_url"] != "https://walnutmarkets.com":
        raise ValueError("V1 capture is limited to the canonical production marketing host.")
    for key in ("model", "voice", "narration_model"):
        if not isinstance(merged[key], str) or len(merged[key]) > 100 or (merged[key] and not re.fullmatch(r"[a-zA-Z0-9_.-]+", merged[key])):
            raise ValueError(f"Invalid {key}.")
    if merged["default_cta"] != DEFAULTS["default_cta"]:
        raise ValueError("V1 uses the verified research CTA.")
    if set(merged["template_ids"]) != set(FORMATS) or any(not isinstance(v, str) or (v and not re.fullmatch(r"[a-zA-Z0-9-]{1,100}", v)) for v in merged["template_ids"].values()):
        raise ValueError("Invalid Creatomate template mapping.")
    score_opportunity(age_days=0, evidence_count=1, weights=merged["weights"])
    set_setting(db, "GROWTH_VIDEO_CONFIG", merged)
    db.commit()
    return merged


def brief(db):
    return setting(db, "GROWTH_BRIEF", {"version_id": None, "sections": {key: "" for key in BRIEF_FIELDS}})


def save_brief(db, actor, sections):
    if set(sections) - set(BRIEF_FIELDS) or any(not isinstance(v, str) or len(v) > 12000 for v in sections.values()):
        raise ValueError("Unknown growth brief section or section exceeds 12,000 characters.")
    version = {"version_id": uid("gb"), "sections": {**brief(db)["sections"], **sections}}
    db.execute(text("INSERT INTO growth_brief_versions VALUES (:id,:at,:actor,:payload)"),
               {"id": version["version_id"], "at": now(), "actor": actor, "payload": dumps(version)})
    set_setting(db, "GROWTH_BRIEF", version)
    db.commit()
    return version


def consume_budget(db, kind, limit):
    if kind not in {"opportunities", "creatives", "renders"}:
        raise ValueError("Invalid quota category.")
    day = now()[:10]
    db.execute(text("INSERT INTO growth_video_budget (day) VALUES (:day) ON CONFLICT(day) DO NOTHING"), {"day": day})
    row = db.execute(text(f"UPDATE growth_video_budget SET {kind}={kind}+1 WHERE day=:day AND {kind}<:limit"), {"day": day, "limit": limit})
    db.commit()
    if row.rowcount != 1:
        raise ValueError(f"Daily {kind} limit reached; review settings or wait until tomorrow UTC.")


def research_source(db, source_id):
    row = db.execute(text("SELECT payload_json,status FROM research_brief_drafts WHERE id=:id"), {"id": source_id}).first()
    if not row:
        raise ValueError("Research source is unavailable.")
    source = json.loads(row[0])
    if source.get("status") not in {"published", "approved_scheduled"} or row[1] != source.get("status"):
        raise ValueError("Research must be approved before it can supply a video.")
    return source


def source_evidence(source):
    """Read original quote values, never extract figures from generated article prose."""
    context = source.get("research_context") or {}
    primary = context.get("primary") or {}
    quote = primary.get("quote") or {}
    ticker = source.get("primary_ticker") or (primary.get("identity") or {}).get("symbol")
    if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,9}", ticker or "") or not quote.get("as_of"):
        return []
    effective = date_value(quote["as_of"])
    if effective > datetime.now(timezone.utc):
        return []
    url = "https://walnutmarkets.com/research/" + str((source.get("article") or {}).get("slug") or "")
    allowed_route(url)
    output = []
    for metric, label in [("price", "recorded share price"), ("market_cap", "recorded market capitalization")]:
        value = quote.get(metric)
        if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
            continue
        formatted = f"${value:,.2f}" if metric == "price" else f"${value:,.0f}"
        item = {"source_type": "approved_research_snapshot", "source_record_id": source["id"],
                "research_brief_id": source["id"], "source_url": url, "walnut_page_url": url,
                "ticker": ticker, "metric": metric, "raw_value": value, "formatted_value": formatted,
                "effective_date": effective.isoformat(), "captured_at": context.get("generated_at") or source.get("created_at"),
                "source_path": f"research_context.primary.quote.{metric}", "source_hash": digest(quote),
                "statement": f"As of {effective.date().isoformat()}, {ticker}'s {label} was {formatted}."}
        item["id"] = "ce_" + digest(item)[:32]
        output.append(item)
    return output


def opportunity(db, opportunity_id):
    row = db.execute(text("SELECT payload_json FROM growth_content_opportunities WHERE id=:id"), {"id": opportunity_id}).first()
    if not row:
        raise HTTPException(404, "Content opportunity not found.")
    return json.loads(row[0])


def evidence_for(db, opportunity_id, *, verify=True):
    items = [json.loads(r[0]) for r in db.execute(text("SELECT payload_json FROM growth_content_evidence WHERE opportunity_id=:id ORDER BY id"), {"id": opportunity_id})]
    if verify:
        sources = {}
        for item in items:
            source_id = item["source_record_id"]
            if source_id not in sources:
                sources[source_id] = {e["id"]: e for e in source_evidence(research_source(db, source_id))}
            if sources[source_id].get(item["id"]) != item:
                raise ValueError("Source evidence changed or is unavailable. Discover a fresh opportunity.")
    return items


def discover(db):
    from app.services.research_briefs import ensure_research_brief_store_schema
    from app.services.search_console import planning_signals
    from app.services.keyword_planner import get_status as keyword_status, normalize as keyword_key
    ensure_research_brief_store_schema(db)
    cfg = config(db)
    google = planning_signals(db)
    # Reuse cached metrics only. Discovery does not initiate an extra paid lookup.
    keywords = keyword_status(db)
    cached_keywords = {keyword_key(m["keyword"]): m for m in keywords["metrics"] if not m.get("stale")}
    rows = db.execute(text("SELECT payload_json FROM research_brief_drafts WHERE status IN ('published','approved_scheduled') ORDER BY updated_at DESC LIMIT 100")).all()
    created = []
    skipped = []
    for row in rows:
        source = json.loads(row[0])
        try:
            evidence = source_evidence(source)
        except (ValueError, TypeError):
            evidence = []
        if not evidence:
            skipped.append({"research_brief_id": source.get("id"), "reason": "No supported dated quote evidence in the approved snapshot."})
            continue
        source_key = digest({"brief": source["id"], "evidence": evidence})
        if db.execute(text("SELECT id FROM growth_content_opportunities WHERE source_key=:key"), {"key": source_key}).first():
            continue
        keyword = str(source.get("target_keyword") or "").strip().lower()
        matches = [q for q in google.get("queries", []) if keyword and str(q.get("query", "")).lower() == keyword]
        impressions = sum(q.get("impressions", 0) for q in matches)
        keyword_metric = cached_keywords.get(keyword_key(keyword)) if keyword else None
        keyword_volume = (keyword_metric or {}).get("avg_monthly_searches")
        age = (datetime.now(timezone.utc) - date_value(evidence[0]["effective_date"])).total_seconds() / 86400
        prior = db.execute(text("SELECT COUNT(*) FROM growth_content_opportunities WHERE payload_json LIKE :ticker"), {"ticker": '%"' + evidence[0]["ticker"] + '"%'}).scalar()
        score, components = score_opportunity(age_days=age, evidence_count=len(evidence), impressions=impressions, keyword_volume=keyword_volume, prior_count=prior, weights=cfg["weights"])
        item = {"id": uid("co"), "topic": (source.get("article") or {}).get("title") or keyword or evidence[0]["ticker"],
                "tickers": [evidence[0]["ticker"]], "opportunity_type": "approved_research", "detected_at": now(),
                "freshness_days": round(age, 1), "source_ids": [source["id"]], "evidence_ids": [e["id"] for e in evidence],
                "target_audience": "Self-directed investors researching a company", "platforms": ["tiktok", "instagram"],
                "destination_url": evidence[0]["walnut_page_url"], "reason": f"Approved research retains {len(evidence)} dated quote metrics; {impressions:g} matching Google impressions in the synced period.",
                "score": score, "component_scores": components, "scoring_weights": cfg["weights"], "confidence": "high for retained quote values; editorial score is a heuristic",
                "factual_data_timestamp": evidence[0]["effective_date"], "search_signal": {"period": google.get("period"), "queries": matches},
                "keyword_signal": keyword_metric,
                "suggested_format": "search_explanation" if matches else "research_finding", "research_brief_id": source["id"]}
        if keyword_volume is not None:
            item["reason"] += f" Keyword Planner estimates {keyword_volume:,} monthly searches including close variants; targeting and fetch date are retained."
        try:
            consume_budget(db, "opportunities", cfg["opportunity_limit"])
        except ValueError:
            break
        inserted = db.execute(text("INSERT INTO growth_content_opportunities VALUES (:id,:key,:score,:at,:payload) ON CONFLICT(source_key) DO NOTHING"),
                              {"id": item["id"], "key": source_key, "score": score, "at": now(), "payload": dumps(item)})
        if inserted.rowcount:
            for fact in evidence:
                db.execute(text("INSERT INTO growth_content_evidence VALUES (:id,:opp,:payload)"), {"id": fact["id"], "opp": item["id"], "payload": dumps(fact)})
            created.append(item["id"])
        db.commit()
    return {"created": created, "skipped": skipped[:30]}


def job(db, job_id):
    row = db.execute(text("SELECT * FROM growth_video_jobs WHERE id=:id"), {"id": job_id}).mappings().first()
    if not row:
        raise HTTPException(404, "Video draft not found.")
    data = dict(row)
    data["payload"] = json.loads(data.pop("payload_json"))
    return data


def save_job(db, item, *, token=None):
    params = {"id": item["id"], "status": item["status"], "payload": dumps(item["payload"]), "at": now(), "token": token, "previous": item["updated_at"]}
    clause = " AND lease_token=:token" if token else " AND lease_token IS NULL AND updated_at=:previous"
    result = db.execute(text("UPDATE growth_video_jobs SET status=:status,payload_json=:payload,updated_at=:at WHERE id=:id" + clause), params)
    if result.rowcount != 1:
        db.rollback()
        raise ValueError("Video is being processed by another worker. Refresh and try again.")
    draft = db.get(AiMarketingOpportunity, item["draft_id"])
    if draft:
        draft.status = {"APPROVED": "approved", "REJECTED": "rejected", "READY_FOR_REVIEW": "needs_review", "FAILED": "quality_failed"}.get(item["status"], "draft")
        draft.generated_content = (item["payload"].get("creative") or {}).get("narration")
    db.commit()
    item["updated_at"] = params["at"]


def create_job(db, opportunity_id, actor, platform, video_format, *, parent=None, feedback="", storyboard=None):
    if platform not in {"tiktok", "instagram"} or video_format not in FORMATS:
        raise ValueError("Unsupported platform or video format.")
    if storyboard and storyboard.get("format") != video_format:
        raise ValueError("Edited storyboard must preserve the selected video format.")
    opp = opportunity(db, opportunity_id)
    evidence_for(db, opportunity_id)
    job_id = uid("gv")
    row = AiMarketingOpportunity(platform=platform, source_provider="walnut_video", source_id=job_id,
        source_url=opp["destination_url"], source_dedupe_key=job_id, title=opp["topic"], status="draft",
        campaign_type="short_video", content_type="video", fit_score=round(opp["score"]),
        suggested_destination_url=opp["destination_url"], short_reason=opp["reason"])
    db.add(row)
    db.flush()
    payload = {"platform": platform, "format": video_format, "feedback": feedback, "retry_count": 0, "opportunity": opp,
               "captures": {}, "audio": {}, "creative": None, "storyboard_override": storyboard,
               "experiment": {"content_id": job_id, "opportunity_id": opportunity_id, "platform": platform,
                 "destination_url": opp["destination_url"], "campaign": "walnut_research_video", "utm_source": platform,
                 "utm_medium": "organic_social", "utm_campaign": "walnut_research_video", "utm_content": job_id,
                 "published_at": None, "external_post_id": None, "performance": None}}
    payload["experiment"]["tracked_url"] = opp["destination_url"] + "?" + urlencode({key: value for key, value in payload["experiment"].items() if key.startswith("utm_")})
    db.execute(text("INSERT INTO growth_video_jobs (id,draft_id,opportunity_id,owner_id,status,revision,parent_id,created_at,updated_at,payload_json) VALUES (:id,:draft,:opp,:actor,'OPPORTUNITY_CREATED',:revision,:parent,:at,:at,:payload)"),
               {"id": job_id, "draft": row.id, "opp": opportunity_id, "actor": actor,
                "revision": (parent["revision"] + 1) if parent else 1, "parent": parent["id"] if parent else None, "at": now(), "payload": dumps(payload)})
    db.commit()
    return job(db, job_id)


def remember(db, item, actor, action, feedback="", revised=None):
    creative = item["payload"].get("creative") or {}
    payload = {"feedback": feedback, "platform": item["payload"]["platform"], "format": item["payload"]["format"],
               "previous_hook": creative.get("hook"), "previous_script": creative.get("narration"),
               "previous_storyboard": creative, "revised_storyboard": revised,
               "creative_angle": creative.get("creative_angle"), "duration": creative.get("target_duration_seconds")}
    db.execute(text("INSERT INTO growth_memory (id,job_id,opportunity_id,action,created_at,actor_id,payload_json) VALUES (:id,:job,:opp,:action,:at,:actor,:payload)"),
               {"id": uid("gm"), "job": item["id"], "opp": item["opportunity_id"], "action": action, "at": now(), "actor": actor, "payload": dumps(payload)})
    db.commit()


def relevant_memory(db, platform, video_format):
    rows = db.execute(text("SELECT action,payload_json FROM growth_memory WHERE active=1 AND action IN ('approve','reject','edit','regenerate') ORDER BY created_at DESC LIMIT 100")).all()
    lessons = []
    for action, raw in rows:
        p = json.loads(raw)
        if p.get("platform") == platform or p.get("format") == video_format:
            lessons.append({"decision": action, "feedback": p.get("feedback", "")[:600], "hook": p.get("previous_hook"), "angle": p.get("creative_angle"), "duration": p.get("duration")})
    return lessons[:8]
