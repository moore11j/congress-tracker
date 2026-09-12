import pytest
from fastapi import HTTPException

from test_growth_video import db, Storage
from app.models import UserAccount
from app.services import growth_product_ad as product, growth_video_pipeline as pipeline, growth_video_store as store
from app.services.growth_native_render import timeline
from app.routers import growth_video as api


def aligned(script, duration=31):
    return {"duration":duration,"alignment":{"characters":list(script),
        "character_start_times_seconds":[i*duration/len(script) for i in range(len(script))],
        "character_end_times_seconds":[(i+1)*duration/len(script) for i in range(len(script))]}}


def test_product_campaign_has_one_coherent_script_and_real_ticker_route():
    board=product.creative()
    assert "NVIDIA" in board["narration"]
    assert "$" not in board["narration"]
    assert board["narration"]==" ".join(s["narration"] for s in board["storyboard"])
    assert {s["shot"] for s in board["storyboard"] if s["walnut_url"]}=={"chart","score","risks","outcomes"}
    assert board["target_url"]=="https://walnutmarkets.com/"
    assert len({product.creative(h)["hook"] for h in product.HOOKS})==3


def test_product_campaign_rejects_unreviewed_copy(db):
    item=product.create_job(db,1)
    assert item["status"]=="CREATIVE_READY"
    assert product.validate(item)["schema_version"]==2
    item["payload"]["creative"]["narration"]="NVIDIA will double."
    with pytest.raises(ValueError,match="reviewed campaign"):
        product.validate(item)
    with pytest.raises(ValueError):
        product.create_job(db,1,hook="invented")


def test_product_stages_use_one_voice_and_preserve_review_gate(db, monkeypatch):
    item=product.create_job(db,1)
    storage=Storage()
    calls=[]
    class Narrator:
        def generate(self,script,voice,model,*,continuous):
            calls.append((voice,model,continuous))
            return b"audio",aligned(script)
    def capture(shot):
        return b"footage",b"thumbnail",{"component":shot,"public_context":True}
    def render(board,captures,audio,read):
        assert set(captures)=={"chart","score","risks","outcomes"}
        timeline(board,audio)
        return b"0000ftypvideo",{"duration":31.45,"width":1080,"height":1920}
    item=api.decision(item["id"],api.Decision(action="render"),db.get(UserAccount,1),db)
    statuses=[pipeline.advance(db,item["id"],storage=storage,capture=capture,narrator=Narrator(),renderer=render) for _ in range(8)]
    assert statuses==["CAPTURE_PENDING","CAPTURE_PENDING","CAPTURE_PENDING","CAPTURE_READY","AUDIO_PENDING","AUDIO_READY","RENDER_PENDING","READY_FOR_REVIEW"]
    assert len(calls)==1 and calls[0][2] is True
    assert storage.calls.count("audio")==1
    assert pipeline.advance(db,item["id"],storage=storage)=="READY_FOR_REVIEW"
    with pytest.raises(HTTPException) as exc:
        api.media(item["id"],download=True,db=db)
    assert exc.value.status_code==409
    result=api.decision(item["id"],api.Decision(action="approve"),db.get(UserAccount,1),db)
    assert result["status"]=="APPROVED"


def test_continuous_voice_drives_shots_and_caption_timing():
    board=product.creative()
    audio=aligned(board["narration"])
    scenes,captions,duration=timeline(board,audio)
    assert duration==31.45
    assert scenes[0]["start"]==0
    assert all(a["end"]==b["start"] for a,b in zip(scenes,scenes[1:]))
    assert scenes[-1]["end"]==duration
    assert " ".join(c["text"] for c in captions)==board["narration"]
    assert all(0<=c["start"]<c["end"]<=duration for c in captions)
    audio["alignment"]["characters"][0]="x"
    with pytest.raises(ValueError,match="does not match"):
        timeline(board,audio)


def test_continuous_voice_rejects_bad_alignment():
    board=product.creative()
    audio=aligned(board["narration"])
    audio["alignment"]["character_start_times_seconds"][20]=-1
    with pytest.raises(ValueError,match="timing"):
        timeline(board,audio)


def test_product_ad_requires_active_admin_owner(db):
    item=product.create_job(db,1)
    item["status"]="CAPTURE_PENDING"
    store.save_job(db,item)
    db.get(UserAccount,1).role="user"
    db.commit()
    assert pipeline.advance(db,item["id"])=="FAILED"
    assert "active administrator" in store.job(db,item["id"])["payload"]["failure_reason"]
