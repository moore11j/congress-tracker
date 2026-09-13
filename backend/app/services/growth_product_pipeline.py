"""Product-ad stages reuse the established job lease, budgets and review gate."""
from app.services import growth_video_store as store
from app.services.growth_product_ad import validate
from app.services.growth_product_capture import capture_product_shot
from app.services.growth_native_render import render_product_video
from app.services.growth_video_media import AssetStore, ElevenLabsNarration


def advance_stage(db, item, stage, token, *, storage=None, capture=None, narrator=None, render=None):
    board=validate(item)
    data=item["payload"]
    storage=storage or AssetStore()
    data.setdefault("stage_history",[]).append({"stage":stage,"at":store.now()})
    if stage=="CAPTURE_PENDING":
        if not data.get("render_budget_reserved"):
            store.consume_budget(db,"renders",store.config(db)["render_limit"])
            data["render_budget_reserved"]=True
        item["status"]="CAPTURING"
        store.save_job(db,item,token=token)
        for scene in board["storyboard"]:
            shot=scene["shot"]
            if not scene["walnut_url"] or shot in data["captures"]:
                continue
            if capture:
                footage,thumbnail,metadata=capture(shot)
            elif board["schema_version"] == 4:
                from app.services.growth_navigation_capture import capture_navigation_shot
                footage,thumbnail,metadata=capture_navigation_shot(shot, owner_id=item["owner_id"])
            elif board["schema_version"] == 3:
                from app.services.growth_research_capture import capture_research_shot
                footage,thumbnail,metadata=capture_research_shot(shot, owner_id=item["owner_id"])
            else:
                footage,thumbnail,metadata=capture_product_shot(shot, owner_id=item["owner_id"])
            data["captures"][shot]=storage.put(db,item["id"],"capture",footage,metadata.get("media_type","video/webm"),metadata)
            if not data.get("thumbnail"):
                data["thumbnail"]=storage.put(db,item["id"],"thumbnail",thumbnail,"image/png",metadata)
            store.save_job(db,item,token=token)
            # Each live browser capture gets its own lease and cron pass. A slow
            # page cannot exhaust the lease for the entire four-shot campaign.
            break
        expected={s["shot"] for s in board["storyboard"] if s["walnut_url"]}
        item["status"]="CAPTURE_READY" if expected.issubset(data["captures"]) else "CAPTURE_PENDING"
    elif stage=="CAPTURE_READY":
        item["status"]="AUDIO_PENDING"
    elif stage=="AUDIO_PENDING":
        if "continuous" not in data["audio"]:
            voice=narrator or ElevenLabsNarration()
            content,metadata=voice.generate(board["narration"],board["voice_id"],board["voice_model"],continuous=True)
            data["audio"]["continuous"]=storage.put(db,item["id"],"audio",content,"audio/mpeg",metadata)
        item["status"]="AUDIO_READY"
    elif stage=="AUDIO_READY":
        item["status"]="RENDER_PENDING"
    elif stage=="RENDER_PENDING":
        def read_asset(asset):
            # Only immutable keys already attached to this job; no external URLs.
            if not asset["object_key"].startswith(f"ai-growth/{item['id']}/"):
                raise ValueError("Asset does not belong to this video job.")
            if asset.get("bytes",0)>200*1024*1024:
                raise ValueError("Video source exceeds the supported size.")
            response=storage.client.get_object(Bucket=storage.bucket,Key=asset["object_key"])
            try:
                content=response["Body"].read(200*1024*1024+1)
            finally:
                response["Body"].close()
            if len(content)>200*1024*1024:
                raise ValueError("Video source exceeds the supported size.")
            return content
        content,metadata=(render or render_product_video)(board,data["captures"],data["audio"]["continuous"],read_asset)
        data["video"]=storage.put(db,item["id"],"render",content,"video/mp4",metadata)
        data["actual_duration"]=metadata["duration"]
        data["experiment"]["renderer"]="walnut_native"
        item["status"]="READY_FOR_REVIEW"
    else:
        raise ValueError("Unsupported product-ad stage.")
