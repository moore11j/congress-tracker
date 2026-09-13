"""Daily, source-bound research walkthroughs using the native cinematic renderer."""
import re

from sqlalchemy import text

from app.services import growth_video_store as store
from app.services.growth_navigation_ad import TAGLINE, INSIGHTS_URL
from app.services.growth_video_domain import allowed_route, digest

CAMPAIGN = "daily_research_v1"
# This brief was already found to contain conflicting institutional aggregates
# during the reviewed NVIDIA campaign. Navigation-only V4 remains available;
# automatic factual narration must wait for an editorial/source correction.
BLOCKED_SOURCES = {"rb_1789151207553_89bc04"}


def source_fingerprint(source):
    # Publishing bookkeeping may change; the editorial content and underlying
    # research snapshot must not change after video creation or approval.
    return digest({k: source.get(k) for k in ("id", "primary_ticker", "article", "research_context")})


def excerpt(source):
    article = source.get("article") or {}
    if source.get("status") != "published":
        raise ValueError("Daily videos require published research.")
    if source.get("id") in BLOCKED_SOURCES:
        raise ValueError("This brief's institutional aggregates need reconciliation before automatic factual narration.")
    if any(w.get("blocking") for w in (source.get("validation_warnings") or []) if isinstance(w, dict)):
        raise ValueError("Resolve the research quality warnings before creating a video.")
    # Extract a complete, short passage. No financial facts are synthesized by
    # the video layer. The approval screen identifies this as the brief's view.
    visible = [s.get("body_markdown", "") for s in article.get("sections", []) if isinstance(s, dict)]
    if article.get("judgment") or not article.get("subtitle"):
        visible.append(article.get("summary", ""))
    visible = [re.sub(r"\[([^\]\n]+)\]\((?:https?://|/)[^)]+\)", r"\1", v.replace("**", ""))
               for v in visible if isinstance(v, str)]
    # key_points are retained in the API but aren't rendered by the website.
    # Use them only when the same words also appear in a visible paragraph.
    candidates = [v for v in article.get("key_points", []) if isinstance(v, str) and any(v in body for body in visible)]
    for body in visible:
        for paragraph in re.split(r"\n\s*\n", body):
            candidates.append(paragraph)
            candidates.extend(re.split(r"(?<=[.!?])\s+(?=[A-Z])", paragraph))
    for value in candidates:
        if not isinstance(value, str):
            continue
        value = re.sub(r"\s+", " ", value).strip()
        dependent = re.match(r"^(Together|Those|These|This|That|They|Their|It|Its|Both|However|But|Also|Meanwhile)\b", value, re.I)
        if (8 <= len(value.split()) <= 65 and not dependent and not value.endswith("?")
                and not re.search(r"https?://|[<>\[\]*|]", value) and value[0].isalpha()):
            return value
    raise ValueError("The brief needs a self-contained takeaway (8–65 words) for narration.")


def creative(source):
    takeaway = excerpt(source)
    article = source["article"]
    ticker = source.get("primary_ticker", "")
    if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,9}", ticker):
        raise ValueError("Daily walkthroughs require a primary stock ticker.")
    url = "https://walnutmarkets.com/research/" + str(article.get("slug", ""))
    allowed_route(url)
    title = article.get("title", "")
    if not isinstance(title, str) or not 5 <= len(title) <= 180:
        raise ValueError("Research title is missing or too long.")
    ticker_url = f"https://app.walnutmarkets.com/ticker/{ticker}"
    as_of = str(source.get("data_as_of") or (source.get("research_context") or {}).get("generated_at") or source.get("published_at") or source.get("created_at") or "")[:10]
    hook = title if len(title.split()) <= 20 else f"Walnut's latest research on {ticker}."
    beats = [
        ("daily_search", f"{hook} Search {ticker} in Walnut and open the ticker.", f"Researching {ticker}?", f"Search → {ticker}", ticker_url),
        ("daily_insights", "Click Insights, then scroll to Research Briefs.", "Find the research.", "Insights → Research Briefs", INSIGHTS_URL),
        ("daily_brief", "Open the brief. Here's its takeaway.", "Read the brief.", f"{ticker} · Published research", url),
        ("daily_takeaway", takeaway, "The brief's takeaway", f"{ticker} · Research snapshot {as_of}", url),
        ("daily_cta", "Read the full analysis and risks on Walnut Markets. Don't follow a signal. Follow the evidence.", "Follow the evidence.", "walnutmarkets.com", None),
    ]
    scenes = [dict(sequence=i, shot=shot, narration=voice, on_screen_text=head, subhead=sub,
                   duration_seconds=max(3, round(len(voice.split()) / 2.5)), walnut_url=page,
                   capture_target=shot, capture_action="record" if page else "none",
                   visual_type="walnut_recording" if page else "brand_card", transition="cut",
                   statement_ids=[shot], evidence_ids=["published_brief:" + source["id"]])
              for i, (shot, voice, head, sub, page) in enumerate(beats, 1)]
    return dict(schema_version=5, campaign_id=CAMPAIGN, format="research_finding",
                creative_angle="published_research_walkthrough", hook_id="daily", hook=beats[0][1],
                storyboard=scenes, scenes=scenes, narration=" ".join(s["narration"] for s in scenes),
                target_duration_seconds=sum(s["duration_seconds"] for s in scenes),
                source_research_brief_id=source["id"], source_research_title=title, ticker=ticker,
                source_excerpt=takeaway, source_excerpt_path="article.sections or visible article.summary",
                caption=f"{title}\n\nWalnut's published brief: {takeaway}\n\nRead the full context and risks: {url}\nFind it in Walnut → Insights → Research Briefs. Research only, not investment advice.\n#{ticker.replace('.', '')} #StockResearch #WalnutMarkets",
                first_comment=f"Full research: {url}\nFollow along: {ticker_url}\nWhich stock should we research next?",
                posting_notes="Buffer Free does not automatically add the first comment. Links are included in the caption; paste the optional comment manually. Review the attributed takeaway against the source before approval.",
                cta="Read the full brief on Walnut Markets.", target_url=url, brand_tagline=TAGLINE,
                voice_id="iP95p4xoKVk53GoZ742B", voice_model="eleven_v3",
                warnings=["The takeaway is an excerpt of published research, not an independently verified new financial claim.",
                          "Check the brief's dates, source limitations and risks before approving.",
                          "Real admin navigation; account identity is masked. Background and voice are AI assisted."],
                caption_statement_ids=[], evidence_ids=[source["id"]])


def validate(item, db=None):
    p = item["payload"]
    source = p.get("research_source", {})
    expected = creative(source)
    if p.get("creative") != expected or p.get("campaign_hash") != digest(expected):
        raise ValueError("Daily creative changed. Create a new video revision.")
    if p.get("research_source_hash") != source_fingerprint(source):
        raise ValueError("Daily research provenance changed.")
    if db is not None:
        current = store.research_source(db, source["id"])
        if current.get("status") != "published" or source_fingerprint(current) != p["research_source_hash"]:
            raise ValueError("Research changed or is no longer published. Create a fresh video.")
    return expected


def create_job(db, source, actor, *, parent=None, feedback=""):
    board = creative(source)
    store.consume_budget(db, "creatives", store.config(db)["creative_limit"])
    key = CAMPAIGN + ":" + source["id"] + ":" + source_fingerprint(source)
    opp = {"id": "co_" + digest(key)[:32], "topic": source["article"]["title"],
           "opportunity_type": "daily_research", "tickers": [board["ticker"]], "score": 0,
           "component_scores": {}, "factual_data_timestamp": (source.get("research_context") or {}).get("generated_at") or source.get("created_at"),
           "destination_url": board["target_url"], "research_brief_id": source["id"],
           "reason": "Published research adapted into a source-bound walkthrough; owner approval required.",
           "suggested_format": "research_finding", "search_signal": {"target_keyword": source.get("target_keyword")}}
    db.execute(text("INSERT INTO growth_content_opportunities VALUES (:id,:key,0,:at,:payload) ON CONFLICT(source_key) DO NOTHING"),
               {"id": opp["id"], "key": key, "at": store.now(), "payload": store.dumps(opp)})
    db.commit()
    item = store.create_job(db, opp["id"], actor, "instagram", "research_finding", parent=parent, feedback=feedback,
        reviewed_product={"campaign_id": CAMPAIGN, "product_hook": "daily", "creative": board,
                          "campaign_hash": digest(board), "research_source": source,
                          "research_source_hash": source_fingerprint(source), "daily_automation": True,
                          "model_metadata": {"provider": "extractive_published_research", "version": 1}})
    item["status"] = "CAPTURE_PENDING"
    store.save_job(db, item)
    return item
