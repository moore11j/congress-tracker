import copy
import pytest
from PIL import Image, ImageChops
from app.services.growth_social_layout import SAFE, PANEL, focus_crop, decorate
from app.services import growth_daily_video as daily, growth_video_automation as automation
from app.services import growth_video_store as store
from test_growth_video_automation import db, source
from sqlalchemy import text


@pytest.mark.parametrize('closing',[False,True])
def test_social_copy_clears_native_chrome_and_action_rail(closing):
    image=Image.new('RGB',(1080,1920),'black')
    baseline=image.copy()
    scene={'sequence':1,'on_screen_text':'Researching MSFT?','subhead':'MSFT → Research'}
    decorate(image,scene,[scene],{'ticker':'MSFT'},
             {'text':'SEC filings, and what'},closing=closing,logo=Image.new('RGB',(100,100),'green'))
    # Check actual painted pixels, not just coordinate constants. Nothing
    # competes with the native header, caption stack, or right action rail.
    bounds=ImageChops.difference(image,baseline).getbbox()
    assert bounds[0]>=SAFE[0] and bounds[1]>=SAFE[1]
    assert bounds[2]<=SAFE[2] and bounds[3]<=SAFE[3]
    assert ImageChops.difference(image.crop((0,1450,1080,1920)),baseline.crop((0,1450,1080,1920))).getbbox() is None
    assert PANEL[3] < 1060


def test_focus_preserves_all_financial_columns_and_tracks_vertical_action():
    asset={'viewport':{'width':1040,'height':1000},'frames':[
        {'camera':{'x':0,'y':0,'width':1040,'height':1000},'cursor':[900,90]},
        {'camera':{'x':0,'y':0,'width':1040,'height':1000},'cursor':[900,920]}]}
    top,bottom=(focus_crop(i,asset) for i in (0,1))
    assert top['width']==bottom['width']==1040
    assert top['y']==0 and bottom['y']>top['y']
    assert bottom['y']+bottom['height']<=1000
    asset['frames'][1]['camera']={'x':40,'y':100,'width':900,'height':400}
    assert focus_crop(1,asset)==asset['frames'][1]['camera']


@pytest.mark.parametrize('tutorial',['research','ownership'])
def test_tutorial_is_source_bound_and_requires_review(db,monkeypatch,tutorial):
    original=source(db,monkeypatch)
    item=daily.create_job(db,original,1,tutorial=tutorial)
    board=daily.validate(item,db)
    assert board['content_kind']=='feature_tutorial'
    assert board['format']=='product_investigation'
    assert item['status']=='CAPTURE_PENDING'
    assert original['article']['key_points'][0] not in board['narration']
    bad=copy.deepcopy(item);bad['payload']['creative']['narration']='Guaranteed returns'
    with pytest.raises(ValueError):daily.validate(bad,db)
    revision=daily.create_job(db,original,1,parent=item,feedback='Review another take')
    assert daily.validate(revision,db)['tutorial_id']==tutorial
    assert revision['parent_id']==item['id']


def test_mix_repeats_three_research_then_one_tutorial_without_counting_failures(db,monkeypatch):
    source(db,monkeypatch)
    db.execute(text('DELETE FROM growth_video_brief_events'));db.commit()
    cfg={'tutorial_every':4,'tutorial_mix_since':'2026-09-26T00:00:00+00:00'}
    for name,status,at in [('failed','FAILED','2026-09-26 12:00:00+00:00'),('old','CREATED','2026-09-25 12:00:00+00:00')]:
        db.execute(text("INSERT INTO growth_video_brief_events (brief_id,status,trigger_source,created_at,updated_at,attempts) VALUES (:id,:status,'test',:at,:at,1)"),{'id':name,'status':status,'at':at})
    db.commit()
    formats=[]
    for i in range(8):
        formats.append(automation.next_tutorial(db,cfg))
        db.execute(text("INSERT INTO growth_video_brief_events (brief_id,status,trigger_source,created_at,updated_at,attempts) VALUES (:id,'CREATED','test',:at,:at,1)"),{'id':f'new-{i}','at':'2026-09-26 12:00:00+00:00' if i%2 else '2026-09-26T12:00:00+00:00'})
        db.commit()
    assert formats==[None,None,None,'research',None,None,None,'ownership']
    assert automation.next_tutorial(db,{'tutorial_every':0}) is None


def test_publish_events_create_a_real_tutorial_on_fourth_brief(db,monkeypatch):
    from app.services import research_briefs
    original=source(db,monkeypatch)
    db.execute(text('DELETE FROM growth_video_brief_events'));db.commit()
    store.set_setting(db,automation.KEY,{'enabled':True,'owner_id':1,'tutorial_every':4,'tutorial_mix_since':'2000-01-01T00:00:00+00:00'})
    db.commit()
    for i in range(4):
        published=copy.deepcopy(original);published['id']=f'rb_mix_{i}'
        research_briefs._upsert_db_draft(db,published);db.commit()
    result=automation.process_publish_events(db)
    assert not result['failed'] and len(result['created'])==4
    boards=[daily.validate(store.job(db,row['job_id']),db) for row in result['created']]
    assert [b.get('tutorial_id') for b in boards]==[None,None,None,'research']
    assert automation.process_publish_events(db)['created']==[]
