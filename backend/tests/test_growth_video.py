from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, get_db
from app.models import AiMarketingOpportunity, UserAccount
from app.routers import growth_video as api
from app.services import growth_video_store as store, growth_video_pipeline as pipeline
from app.services.growth_video_domain import COPY, allowed_route, score_opportunity, validate_storyboard
from app.services.growth_video_capture import validate_command
from app.services.growth_video_media import CreatomateVideoRenderer, render_spec, timed_captions


@pytest.fixture
def db():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        store.ensure_schema(session)
        session.add(UserAccount(email="growth-admin@example.com", role="admin"))
        session.commit()
        yield session
    engine.dispose()


def seed(db, monkeypatch, *, status="published", price=125.5):
    from app.services import research_briefs, search_console
    research_briefs.ensure_research_brief_store_schema(db)
    source = {"id": "rb_video", "status": status, "primary_ticker": "NVDA", "created_at": store.now(),
        "target_keyword": "how to research NVDA", "article": {"title": "Investigating NVDA", "slug": "investigating-nvda"},
        "research_context": {"generated_at": "2026-09-10T18:00:00+00:00", "primary": {
            "identity": {"symbol": "NVDA"}, "quote": {"price": price, "market_cap": 1_000_000_000,
                                                         "as_of": "2026-09-10T17:00:00+00:00"}}}}
    # Only fields used by the source adapter; production owns this existing table.
    research_briefs._upsert_db_draft(db, source)
    db.commit()
    monkeypatch.setattr(search_console, "planning_signals", lambda db: {"period":{"start":"2026-08-12","end":"2026-09-09"}, "queries":[{"query":"how to research NVDA","impressions":40,"clicks":2}]})
    result = store.discover(db)
    return store.opportunity(db, result["created"][0]), source


def storyboard(evidence):
    return {"format":"research_finding", "creative_angle":"dated_evidence", "hook_id":"hook_research",
            "alternate_hook_ids":["hook_context"], "caption_statement_ids":["hook_research","cta"],
            "scenes":[
                {"sequence":1,"duration_seconds":5,"statement_ids":["hook_research"],"visual_type":"title_card","capture_target":"none","capture_action":"none","transition":"fade"},
                {"sequence":2,"duration_seconds":10,"statement_ids":[evidence[0]["id"]],"visual_type":"walnut_screenshot","capture_target":"research_header","capture_action":"screenshot","transition":"fade"},
                {"sequence":3,"duration_seconds":5,"statement_ids":["caution"],"visual_type":"data_card","capture_target":"none","capture_action":"none","transition":"cut"},
                {"sequence":4,"duration_seconds":5,"statement_ids":["cta"],"visual_type":"cta_card","capture_target":"none","capture_action":"none","transition":"fade"},
            ]}


def prepared(db, monkeypatch):
    opp, source = seed(db, monkeypatch)
    evidence = store.evidence_for(db,opp["id"])
    board = storyboard(evidence)
    item = store.create_job(db,opp["id"],1,"instagram","research_finding",storyboard=board)
    assert pipeline.advance(db,item["id"]) == "CREATIVE_READY"
    return store.job(db,item["id"]), opp, source, board


class Storage:
    def __init__(self): self.calls=[]
    def put(self, db, job_id, kind, content, mime, metadata):
        self.calls.append(kind)
        return {"object_key":f"{job_id}/{kind}","sha256":"verified-hash","content_type":mime,**metadata}
    def url(self, asset, **kwargs): return "https://example.com/asset"


class Narrator:
    def __init__(self): self.calls=0
    def generate(self, script, voice, model):
        self.calls+=1
        return b"audio",{"duration":4,"script":script,"captions":timed_captions(script,4)}


class Renderer:
    def __init__(self): self.submissions=0; self.failed=False; self.status="succeeded"
    def create_render(self, spec, template, content_id): self.submissions+=1; return "provider-render-id"
    def get_render_status(self, render_id):
        assert render_id == "provider-render-id"
        return {"status":"failed" if self.failed else self.status,"url":"https://cdn.creatomate.com/test.mp4","width":1080,"height":1920}
    def fetch_asset(self, result): return b"0000ftypverified"


def capture(scene, **kwargs):
    return b"png", "image/png", {"evidence_ids":scene["evidence_ids"],"captured_at":store.now()}, b"png"


def render_to_review(db,item):
    item["status"]="CAPTURE_PENDING"
    store.save_job(db,item)
    storage,narrator,renderer=Storage(),Narrator(),Renderer()
    stages=[]
    for _ in range(6):
        stages.append(pipeline.advance(db,item["id"],storage=storage,capture=capture,narrator=narrator,renderer=renderer))
    assert stages == ["CAPTURE_READY","AUDIO_PENDING","AUDIO_READY","RENDER_PENDING","RENDERING","READY_FOR_REVIEW"]
    return store.job(db,item["id"]),storage,narrator,renderer


def test_trial_resolution_keeps_preview_but_blocks_approval(db, monkeypatch):
    item,*_=prepared(db,monkeypatch)
    item,storage,narrator,renderer=render_to_review(db,item)
    item["status"]="RENDERING"
    item["payload"].pop("video")
    store.save_job(db,item)
    monkeypatch.setattr(renderer,"get_render_status",lambda render_id: {"status":"succeeded","width":270,"height":480,"duration":25})
    assert pipeline.advance(db,item["id"],storage=storage,renderer=renderer)=="FAILED"
    result=store.job(db,item["id"])
    assert result["payload"]["preview_video"]["width"]==270
    assert "video" not in result["payload"]
    assert result["payload"]["render_provider_failed"] is True
    assert result["payload"]["failed_stage"]=="RENDERING"
    with pytest.raises(HTTPException) as exc:
        api.decision(item["id"],api.Decision(action="approve"),db.get(UserAccount,1),db)
    assert exc.value.status_code==409
    assert renderer.submissions==1


def test_transparent_score_rewards_real_demand_freshness_and_novelty():
    high,parts=score_opportunity(age_days=1,evidence_count=2,impressions=100,prior_count=0)
    low,_=score_opportunity(age_days=40,evidence_count=1,impressions=0,prior_count=3)
    assert high>low
    assert parts["evidence"]==80
    assert parts["search_demand"]>0
    for weights in [{}, {"evidence":1}, {**store.DEFAULT_WEIGHTS,"evidence":float("nan")}]:
        with pytest.raises(ValueError): score_opportunity(age_days=0,evidence_count=1,weights=weights or {"bad":1})


def test_discovery_uses_real_snapshot_and_deduplicates(db,monkeypatch):
    opp,source=seed(db,monkeypatch)
    assert opp["search_signal"]["queries"][0]["impressions"]==40
    assert len(store.evidence_for(db,opp["id"]))==2
    assert "2026-09-10" in store.evidence_for(db,opp["id"])[0]["statement"]
    assert store.discover(db)["created"]==[]
    assert store.source_evidence({**source,"research_context":{}})==[]


def test_mutated_or_revoked_research_fails_closed(db,monkeypatch):
    opp,source=seed(db,monkeypatch)
    source["research_context"]["primary"]["quote"]["price"]=99999
    db.execute(text("UPDATE research_brief_drafts SET payload_json=:p"),{"p":json.dumps(source)});db.commit()
    with pytest.raises(ValueError,match="changed"): store.evidence_for(db,opp["id"])
    source["status"]="rejected"
    db.execute(text("UPDATE research_brief_drafts SET payload_json=:p"),{"p":json.dumps(source)});db.commit()
    with pytest.raises(ValueError,match="approved"): store.evidence_for(db,opp["id"])


@pytest.mark.parametrize("change", ["fake_number","fake_qualitative","arbitrary_selector","wrong_action","bad_timing","no_capture","no_evidence","bad_hook"])
def test_storyboard_rejects_unsupported_claims_and_commands(db,monkeypatch,change):
    opp,_=seed(db,monkeypatch);ev=store.evidence_for(db,opp["id"]);raw=storyboard(ev)
    if change=="fake_number": raw["scenes"][1]["statement_ids"]=["NVDA returned 500%"]
    if change=="fake_qualitative": raw["caption_statement_ids"]=["Institutions are accumulating this stock"]
    if change=="arbitrary_selector": raw["scenes"][1]["capture_target"]="body:has(input[type=password])"
    if change=="wrong_action": raw["scenes"][1]["capture_action"]="record"
    if change=="bad_timing": raw["scenes"][1]["duration_seconds"]=1
    if change=="no_capture": raw["scenes"][1].update(visual_type="data_card",capture_target="none",capture_action="none")
    if change=="no_evidence": raw["scenes"][1]["statement_ids"]=["context"]
    if change=="bad_hook": raw["hook_id"]="price_will_rise"
    with pytest.raises(ValueError): validate_storyboard(raw,opp,ev)


@pytest.mark.parametrize("url",["http://walnutmarkets.com/research/nvda","https://www.walnutmarkets.com/research/nvda","https://walnutmarkets.com.evil.test/research/nvda","https://walnutmarkets.com/admin","https://walnutmarkets.com/research/nvda?redirect=https://evil.test","https://walnutmarkets.com/research/%2e%2e/admin","https://user@walnutmarkets.com/research/nvda","https://walnutmarkets.com:443/research/nvda"])
def test_route_allowlist(url):
    with pytest.raises(ValueError): allowed_route(url)


def test_capture_command_validation():
    valid={"walnut_url":"https://walnutmarkets.com/research/nvda","capture_target":"research_header","capture_action":"screenshot"}
    assert "research-header" in validate_command(valid)
    with pytest.raises(ValueError): validate_command({**valid,"capture_action":"evaluate"})


def test_authenticated_pipeline_review_memory_and_regeneration(db,monkeypatch):
    item,opp,source,board=prepared(db,monkeypatch)
    item,storage,narrator,renderer=render_to_review(db,item)
    assert renderer.submissions==1 and narrator.calls==4
    assert pipeline.advance(db,item["id"],renderer=renderer)=="READY_FOR_REVIEW"
    assert renderer.submissions==1
    user=db.get(UserAccount,1)
    approved=api.decision(item["id"],api.Decision(action="approve"),user,db)
    assert approved["status"]=="APPROVED"
    assert db.get(AiMarketingOpportunity,item["draft_id"]).status=="approved"
    rejected=api.decision(item["id"],api.Decision(action="reject",feedback="Prefer a more investigative opening."),user,db)
    assert rejected["status"]=="REJECTED"
    revised=api.decision(item["id"],api.Decision(action="regenerate",feedback="Use the investigation hook."),user,db)
    assert revised["revision"]==2 and revised["parent_id"]==item["id"]
    assert store.job(db,item["id"])["payload"]["video"]
    lessons=store.relevant_memory(db,"instagram","research_finding")
    assert {r["decision"] for r in lessons}=={"approve","reject","regenerate"}
    assert any("investigative" in r["feedback"] for r in lessons)


def test_early_approval_and_download_are_rejected(db,monkeypatch):
    item,*_=prepared(db,monkeypatch)
    with pytest.raises(api.HTTPException) as exc: api.decision(item["id"],api.Decision(action="approve"),db.get(UserAccount,1),db)
    assert exc.value.status_code==409
    with pytest.raises(api.HTTPException): api.media(item["id"],True,db)


def test_capture_failure_retains_error_and_never_calls_narrator(db,monkeypatch):
    item,*_=prepared(db,monkeypatch);item["status"]="CAPTURE_PENDING";store.save_job(db,item)
    def fail(*a,**k): raise ValueError("Capture target missing.")
    narrator=Narrator()
    assert pipeline.advance(db,item["id"],storage=Storage(),capture=fail,narrator=narrator)=="FAILED"
    failed=store.job(db,item["id"])
    assert failed["payload"]["failed_stage"]=="CAPTURE_PENDING" and narrator.calls==0
    assert pipeline.advance(db,item["id"])=="FAILED"


def test_render_poll_failure_keeps_id_and_retry_does_not_resubmit(db,monkeypatch):
    item,*_=prepared(db,monkeypatch);item["status"]="CAPTURE_PENDING";store.save_job(db,item)
    storage,narrator,renderer=Storage(),Narrator(),Renderer()
    for _ in range(5): pipeline.advance(db,item["id"],storage=storage,capture=capture,narrator=narrator,renderer=renderer)
    def fail(_): raise ValueError("Provider temporarily unavailable.")
    original=renderer.get_render_status;renderer.get_render_status=fail
    assert pipeline.advance(db,item["id"],renderer=renderer)=="FAILED"
    assert store.job(db,item["id"])["payload"]["render_id"]=="provider-render-id"
    api.decision(item["id"],api.Decision(action="retry",acknowledge_provider_retry=True),db.get(UserAccount,1),db)
    renderer.get_render_status=original
    assert pipeline.advance(db,item["id"],storage=storage,renderer=renderer)=="READY_FOR_REVIEW"
    assert renderer.submissions==1


def test_lease_prevents_double_paid_work_and_expiry_requires_manual_action(db,monkeypatch):
    opp,_=seed(db,monkeypatch);item=store.create_job(db,opp["id"],1,"instagram","research_finding")
    db.execute(text("UPDATE growth_video_jobs SET lease_token='other',lease_until=:lease"),{"lease":(datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat()});db.commit()
    assert pipeline.advance(db,item["id"])=="BUSY"
    pipeline.recover_expired(db)
    assert store.job(db,item["id"])["status"]=="FAILED"
    assert pipeline.advance(db,item["id"])=="FAILED"


def test_budget_is_durable_and_enforced(db):
    store.consume_budget(db,"renders",1)
    with pytest.raises(ValueError,match="limit reached"): store.consume_budget(db,"renders",1)
    assert db.execute(text("SELECT renders FROM growth_video_budget")).scalar()==1


def test_growth_brief_versions_and_memory_corrections(db,monkeypatch):
    a=store.save_brief(db,1,{"product":"Walnut","brand_voice":"Specific."})
    b=store.save_brief(db,1,{"brand_voice":"Investigative."})
    assert a["version_id"]!=b["version_id"] and b["sections"]["product"]=="Walnut"
    assert db.execute(text("SELECT COUNT(*) FROM growth_brief_versions")).scalar()==2
    with pytest.raises(ValueError): store.save_brief(db,1,{"secret_key":"not a brief field"})
    item,*_=prepared(db,monkeypatch);store.remember(db,item,1,"reject","Generic")
    memory_id=db.execute(text("SELECT id FROM growth_memory")).scalar()
    api.memory_edit(memory_id,api.MemoryEdit(feedback="Corrected",active=False),db.get(UserAccount,1),db)
    assert store.relevant_memory(db,"instagram","research_finding")==[]


def test_caption_timing_and_render_spec_are_deterministic(db,monkeypatch):
    item,*_=prepared(db,monkeypatch)
    creative=item["payload"]["creative"]
    audio={str(s["sequence"]):{"duration":4,"object_key":"audio","captions":timed_captions(s["narration"],4)} for s in creative["storyboard"]}
    captures={"2":{"object_key":"screen","content_type":"image/png"}}
    spec=render_spec(creative,captures,audio,Storage())
    assert (spec["width"],spec["height"],spec["output_format"])==(1080,1920,"mp4")
    assert len([e for e in spec["elements"] if e["type"]=="audio"])==4
    assert any(e.get("type")=="image" for e in spec["elements"])
    assert max(e["time"]+e["duration"] for e in spec["elements"])<=spec["duration"]+.001
    assert timed_captions("one two",1,{"characters":list("one two"),"character_start_times_seconds":[i/10 for i in range(7)],"character_end_times_seconds":[(i+1)/10 for i in range(7)]})[0]["end"]==.7


@pytest.mark.parametrize("provider_response", [{"id":"render-test"}, [{"id":"render-test"}]])
def test_creatomate_contract_and_ssrf_rejection(monkeypatch, provider_response):
    from app.services import growth_video_media as media
    calls=[]
    monkeypatch.setenv("CREATOMATE_API_KEY","unit-test-placeholder")
    monkeypatch.setattr(media.requests,"post",lambda url,**kwargs: calls.append((url,kwargs)) or SimpleNamespace(status_code=200,json=lambda:provider_response))
    renderer=CreatomateVideoRenderer()
    spec={"elements":[],"duration":25}
    assert renderer.create_render(spec,"","content-test")=="render-test"
    assert calls[0][1]["json"]=={**spec,"metadata":"content-test"}
    renderer.create_render(spec,"remote-template","content-test")
    assert calls[1][1]["json"]["modifications"]["Content.elements"]==[]
    for url in ["http://localhost/video.mp4","https://evil.test/file.mp4","https://creatomate.com.evil.test/file"]:
        with pytest.raises(ValueError): renderer.fetch_asset({"url":url})


@pytest.mark.parametrize("provider_response", [[], [{"id":"one"},{"id":"two"}], {}, {"id":None}, {"id":""}, "unexpected"])
def test_creatomate_ambiguous_submission_is_not_accepted(monkeypatch, provider_response):
    from app.services import growth_video_media as media
    monkeypatch.setenv("CREATOMATE_API_KEY","unit-test-placeholder")
    monkeypatch.setattr(media.requests,"post",lambda *args,**kwargs: SimpleNamespace(status_code=200,json=lambda:provider_response))
    with pytest.raises(ValueError, match="submission outcome is uncertain"):
        CreatomateVideoRenderer().create_render({"elements":[],"duration":25},"","content-test")


def test_api_session_authorization_and_real_http_workflow(db,monkeypatch):
    from fastapi.testclient import TestClient
    item,*_=prepared(db,monkeypatch)
    app=FastAPI();app.include_router(api.router,prefix="/api")
    app.dependency_overrides[get_db]=lambda:db
    # Preserve real session/admin checks; isolate unrelated global mutation rate buckets.
    app.dependency_overrides[api.rate_limit_admin_mutation]=lambda:None
    client=TestClient(app)
    base="/api/admin/ai-growth/video"
    assert client.get(base).status_code==401
    db.add(UserAccount(email="viewer@example.com",role="user"));db.commit()
    viewer=db.query(UserAccount).filter_by(email="viewer@example.com").one()
    client.cookies.set(SESSION_COOKIE_NAME,sign_session_payload({"uid":viewer.id,"email":viewer.email}))
    assert client.get(base).status_code==403
    assert client.post(base+"/jobs",json={"opportunity_id":item["opportunity_id"]}).status_code==403
    admin=db.get(UserAccount,1)
    client.cookies.set(SESSION_COOKIE_NAME,sign_session_payload({"uid":admin.id,"email":admin.email}))
    assert client.get(base).status_code==200
    assert client.get(base+"/opportunities/"+item["opportunity_id"]).json()["evidence"]
    response=client.post(base+f"/jobs/{item['id']}/decision",json={"action":"render"})
    assert response.status_code==200 and response.json()["status"]=="CAPTURE_PENDING"
    assert client.post(base+f"/jobs/{item['id']}/decision",json={"action":"publish"}).status_code==422


def test_legacy_queue_cannot_mutate_video(db,monkeypatch):
    from app.routers.ai_marketing import _opportunity_or_404
    from app.services.ai_marketing import clear_ai_growth_draft_history
    item,*_=prepared(db,monkeypatch)
    with pytest.raises(api.HTTPException) as exc: _opportunity_or_404(db,item["draft_id"])
    assert exc.value.status_code==409
    clear_ai_growth_draft_history(db)
    assert db.get(AiMarketingOpportunity,item["draft_id"]).status=="draft"
    from app.services.ai_marketing import _digest_opportunities, post_approved_draft_to_x
    assert _digest_opportunities(db,opportunity_ids=[item["draft_id"]],statuses=None,limit=10)==[]
    with pytest.raises(ValueError): post_approved_draft_to_x(db,db.get(AiMarketingOpportunity,item["draft_id"]))


def test_astra_strict_output_contract_and_relevant_prompt_context(db,monkeypatch):
    opp,_=seed(db,monkeypatch)
    ev=store.evidence_for(db,opp["id"])
    item=store.create_job(db,opp["id"],1,"instagram","research_finding")
    store.save_brief(db,1,{"brand_voice":"Investigative", "competitors":"Do not include this unrelated long section."})
    requests=[]
    monkeypatch.setattr(pipeline,"resolved_setting_value",lambda *a:"test-placeholder")
    monkeypatch.setattr(pipeline,"_record_openai_usage_cost",lambda *a,**k:None)
    def response(**kwargs):
        requests.append(kwargs)
        return SimpleNamespace(status_code=200,json=lambda:{"id":"response-test","usage":{"input_tokens":12},"output":[{"content":[{"type":"output_text","text":json.dumps(storyboard(ev))}]}]})
    monkeypatch.setattr(pipeline,"audited_openai_request",response)
    assert pipeline.advance(db,item["id"])=="CREATIVE_READY"
    payload=requests[0]["payload"]
    assert payload["text"]["format"]["strict"] is True
    assert payload["reasoning"] == {"effort": "low"}
    context=json.loads(payload["input"][1]["content"])
    assert "competitors" not in context["growth_brief"]
    assert context["growth_brief"]["brand_voice"]=="Investigative"
    assert store.job(db,item["id"])["payload"]["model_metadata"]["response_id"]=="response-test"


@pytest.mark.parametrize("status,content,expected", [
    ("incomplete", '{"format":"research_finding"' + ' ' * 1000, "incomplete storyboard"),
    ("completed", "", "complete JSON storyboard"),
    ("completed", '{"format":', "complete JSON storyboard"),
])
def test_unusable_openai_response_preserves_audit_and_never_advances(db,monkeypatch,status,content,expected):
    opp,_=seed(db,monkeypatch)
    item=store.create_job(db,opp["id"],1,"instagram","research_finding")
    monkeypatch.setattr(pipeline,"resolved_setting_value",lambda *a:"test-placeholder")
    monkeypatch.setattr(pipeline,"_record_openai_usage_cost",lambda *a,**k:None)
    monkeypatch.setattr(pipeline,"audited_openai_request",lambda **k:SimpleNamespace(status_code=200,json=lambda:{
        "id":"incomplete-response-test","status":status,"incomplete_details":{"reason":"max_output_tokens"} if status=="incomplete" else None,
        "usage":{"output_tokens":6000},"output":[{"content":[{"type":"output_text","text":content}]}]}))
    assert pipeline.advance(db,item["id"])=="FAILED"
    saved=store.job(db,item["id"])["payload"]
    assert expected in saved["failure_reason"]
    assert saved["model_metadata"]["response_id"]=="incomplete-response-test"
    assert saved["model_metadata"]["usage"]["output_tokens"]==6000
    assert saved["creative"] is None and not saved["captures"] and not saved["audio"]


def test_partial_audio_failure_resumes_without_resynthesizing_completed_scenes(db,monkeypatch):
    item,*_=prepared(db,monkeypatch);item["status"]="CAPTURE_PENDING";store.save_job(db,item)
    storage=Storage();narrator=Narrator()
    for _ in range(2): pipeline.advance(db,item["id"],storage=storage,capture=capture)
    original=narrator.generate
    def generate(script,voice,model):
        if narrator.calls==1: raise ValueError("Temporary narration failure.")
        return original(script,voice,model)
    narrator.generate=generate
    assert pipeline.advance(db,item["id"],storage=storage,narrator=narrator)=="FAILED"
    assert list(store.job(db,item["id"])["payload"]["audio"])==["1"]
    api.decision(item["id"],api.Decision(action="retry",acknowledge_provider_retry=True),db.get(UserAccount,1),db)
    narrator.generate=original
    assert pipeline.advance(db,item["id"],storage=storage,narrator=narrator)=="AUDIO_READY"
    assert narrator.calls==4


def test_daily_render_budget_is_reserved_before_tts(db,monkeypatch):
    item,*_=prepared(db,monkeypatch)
    store.save_config(db,{"render_limit":1})
    store.consume_budget(db,"renders",1)
    item["status"]="CAPTURE_PENDING";store.save_job(db,item)
    storage=Storage()
    assert pipeline.advance(db,item["id"],storage=storage,capture=capture)=="FAILED"
    assert not storage.calls


def test_stale_review_cannot_overwrite_another_review(db,monkeypatch):
    item,*_=prepared(db,monkeypatch)
    stale=store.job(db,item["id"])
    item["status"]="REJECTED";store.save_job(db,item)
    stale["status"]="CAPTURE_PENDING"
    with pytest.raises(ValueError): store.save_job(db,stale)
    assert store.job(db,item["id"])["status"]=="REJECTED"
