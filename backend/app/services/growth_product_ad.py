"""Versioned editorial product ads. Product claims and routes are reviewed in code.

These are coherent, human-directed campaigns, not financial-claim extraction from
a quote. Research storyboards retain their separate factual validation contract.
"""
from __future__ import annotations

from pathlib import Path
from sqlalchemy import text

from app.services import growth_video_store as store
from app.services.growth_video_domain import digest, now

CAMPAIGN = "nvda_show_me_why_v2"
BACKGROUND = Path(__file__).parents[1] / "assets/growth/investor-study-v2.png"
TICKER_URL = "https://app.walnutmarkets.com/ticker/NVDA"
OUTCOMES_URL = "https://app.walnutmarkets.com/outcomes"
HOOKS = {
    "opinion": "Everyone has an opinion on NVIDIA. I want to see why.",
    "score": "A stock score is only useful if you can see what's behind it.",
    "accountability": "Before trusting a stock call, ask what happened to the last one.",
}
BEATS = [
    ("chart", "Open NVIDIA in Walnut Markets. Start with the chart.", "START WITH NVIDIA", "The actual Walnut ticker page"),
    ("score", "Then look behind its Confirmation Score.", "SEE WHY IT RANKS", "Open the score. Inspect the drivers."),
    ("risks", "Check the bullish case, the signals that disagree, and the risks.", "CHECK BOTH SIDES", "The bullish case. The disagreement. The risks."),
    ("outcomes", "And with Outcomes, you can see what happened after Walnut's calls.", "WE KEEP SCORE", "Original calls. Subsequent outcomes."),
    ("brand", "Find what deserves your attention. See why. Keep score.", "FIND IT.\nUNDERSTAND IT.\nKEEP SCORE.", "Walnut Markets"),
    ("cta", "Try Walnut Markets free.", "YOUR NEXT IDEA\nSTARTS HERE.", "Create your free account"),
]


def creative(hook="opinion"):
    if hook not in HOOKS:
        raise ValueError("Unknown reviewed product-ad hook.")
    beats = [("hook", HOOKS[hook], "EVERYONE HAS\nAN NVIDIA OPINION.", "Show me why.") if hook == "opinion" else
             ("hook", HOOKS[hook], "SHOW ME WHY.", "A stock idea you can explain."), *BEATS]
    scenes, elapsed = [], 0
    for index, (shot, narration, headline, subhead) in enumerate(beats, 1):
        duration = max(3, round(len(narration.split()) / 2.6))
        scenes.append({"sequence": index, "shot": shot, "duration_seconds": duration,
            "statement_ids": ["product_" + shot], "narration": narration, "on_screen_text": headline,
            "subhead": subhead, "start": elapsed, "end": elapsed + duration,
            "visual_type": "walnut_recording" if shot in {"chart", "score", "risks", "outcomes"} else "title_card",
            "capture_target": shot, "capture_action": "record" if shot in {"chart", "score", "risks", "outcomes"} else "none",
            "transition": "cut", "evidence_ids": ["product_" + shot],
            "walnut_url": OUTCOMES_URL if shot == "outcomes" else TICKER_URL if shot in {"chart", "score", "risks"} else None})
        elapsed += duration
    return {"schema_version": 2, "campaign_id": CAMPAIGN, "format": "product_investigation",
        "creative_angle": "show_me_why", "hook_id": hook, "hook": HOOKS[hook],
        "alternate_hook_ids": [h for h in HOOKS if h != hook],
        "alternate_hooks": [v for h, v in HOOKS.items() if h != hook],
        "caption_statement_ids": [], "scenes": scenes, "storyboard": scenes,
        "target_duration_seconds": elapsed, "narration": " ".join(s["narration"] for s in scenes),
        "caption": "Everyone has an NVIDIA opinion. Walnut lets you look behind the score, check the risks, and track what happened next. Explore Walnut Markets free. Product demonstration; not a recommendation to buy NVIDIA.",
        "cta": "Try Walnut Markets free.", "target_url": "https://walnutmarkets.com/",
        "evidence_ids": ["product_" + s["shot"] for s in scenes],
        "warnings": ["Product walkthrough, not a recommendation to buy NVIDIA.",
                     "Screens are dated captures. Some features require a paid plan.",
                     "Original generated lifestyle backdrop; no claim of investment-related wealth."],
        "voice_direction": "Natural, confident male founder; one continuous conversational take.",
        "voice_id": "iP95p4xoKVk53GoZ742B", "voice_model": "eleven_v3"}


def validate(item):
    p = item["payload"]
    if p.get("campaign_id") == "daily_research_v1":
        from app.services.growth_daily_video import validate as validate_daily
        return validate_daily(item)
    if p.get("campaign_id") == "nvda_navigation_v4":
        from app.services.growth_navigation_ad import validate as validate_navigation
        return validate_navigation(item)
    if p.get("campaign_id") == "nvda_ownership_research_v3":
        from app.services.growth_research_ad import validate as validate_research
        return validate_research(item)
    expected = creative(p.get("product_hook", "opinion"))
    if p.get("campaign_id") != CAMPAIGN or p.get("creative") != expected:
        raise ValueError("Product ad differs from its reviewed campaign. Create a new campaign revision.")
    if p.get("campaign_hash") != digest(expected):
        raise ValueError("Product campaign provenance changed.")
    return expected


def create_job(db, actor, platform="instagram", hook="opinion", parent=None, feedback=""):
    if platform not in {"instagram", "tiktok"}:
        raise ValueError("Unsupported video platform.")
    if hook == "navigation":
        from app.services.growth_navigation_ad import create_job as create_navigation
        return create_navigation(db, actor, platform, parent, feedback)
    if hook == "ownership":
        from app.services.growth_research_ad import create_job as create_research
        return create_research(db, actor, platform, parent, feedback)
    board = creative(hook)
    store.consume_budget(db, "creatives", store.config(db)["creative_limit"])
    source_key = CAMPAIGN
    old = db.execute(text("SELECT id FROM growth_content_opportunities WHERE source_key=:key"), {"key": source_key}).first()
    if old:
        opp = store.opportunity(db, old[0])
    else:
        opp = {"id": store.uid("co"), "topic": "NVIDIA: show me why — Walnut product ad", "opportunity_type": "product_campaign",
            "tickers": ["NVDA"], "score": 0, "component_scores": {}, "factual_data_timestamp": now(),
            "destination_url": "https://walnutmarkets.com/", "research_brief_id": "", "campaign_id": CAMPAIGN,
            "reason": "Founder-directed product campaign. No inferred search demand or performance score.",
            "search_signal": {"status": "not_used", "queries": []}, "suggested_format": "product_investigation"}
        db.execute(text("INSERT INTO growth_content_opportunities VALUES (:id,:key,0,:at,:payload)"),
            {"id": opp["id"], "key": source_key, "at": now(), "payload": store.dumps(opp)})
        db.commit()
    payload = {"campaign_id": CAMPAIGN, "product_hook": hook, "creative": board,
        "campaign_hash": digest(board), "model_metadata": {"provider": "editorial_campaign", "version": 2,
            "brief_version": store.brief(db)["version_id"], "created_at": now(),
            "note": "Reviewed coherent product script; financial figures are not generated."}}
    return store.create_job(db, opp["id"], actor, platform, "product_investigation",
        parent=parent, feedback=feedback, reviewed_product=payload)
