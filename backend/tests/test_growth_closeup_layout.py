import copy
import pytest
from PIL import Image, ImageChops
from app.services.growth_closeup_layout import SAFE, PANEL, focus_crop, decorate


def test_production_focus_preserves_wide_tables_and_zooms_captured_evidence():
    from app.services.growth_closeup_layout import captured_focus_crop
    from app.services.growth_navigation_capture import evidence_bounds
    full = dict(x=0,y=0,width=1040,height=1000)
    asset={'viewport':{'width':1040,'height':1000},'frames':[{'camera':full,'cursor':[900,600]}]}
    assert captured_focus_crop(0,asset)['width']==1040
    target=dict(x=61,y=210,width=598,height=336)
    crop=evidence_bounds(target,min_height=620)
    assert crop==dict(x=41,y=140,width=638,height=620)
    asset['frames'][0]['camera']=crop
    assert captured_focus_crop(0,asset)==crop
    for target in [dict(x=800,y=900,width=200,height=80),dict(x=0,y=-50,width=1040,height=1200)]:
        crop=evidence_bounds(target)
        assert 0<=crop['x']<=1040-crop['width']
        assert 0<=crop['y']<=1000-crop['height']


def test_closeup_requires_explicit_reviewed_source_bounds():
    asset = {'viewport': {'width':1040,'height':1000}, 'frames':[{}]}
    with pytest.raises(ValueError,match='reviewed evidence'):focus_crop(0,asset)
    for crop in [dict(x=-1,y=0,width=600,height=600),dict(x=500,y=0,width=600,height=600),
                 dict(x=0,y=0,width=float('nan'),height=600),dict(x=0,y=0,width=0,height=600)]:
        asset['frames'][0]['evidence_camera']=crop
        with pytest.raises(ValueError,match='original recording'):focus_crop(0,asset)
    crop=dict(x=41,y=140,width=638,height=620)
    asset['frames'][0]['evidence_camera']=crop
    original=copy.deepcopy(asset)
    assert focus_crop(0,asset)==crop
    assert asset==original


@pytest.mark.parametrize('closing',[False,True])
def test_closeup_fills_lower_canvas_and_preserves_product_pixels(closing):
    scene=dict(sequence=1,start=0,end=4,on_screen_text='Read the evidence.',subhead='MSFT · Snapshot September 24')
    image=Image.new('RGB',(1080,1920),'black')
    baseline=image.copy()
    decorate(image,scene,[scene],{'ticker':'MSFT'},{'text':'A mixed picture in the filings'},elapsed=2,closing=closing)
    bounds=ImageChops.difference(image,baseline).getbbox()
    assert SAFE[0]<=bounds[0] and SAFE[1]<=bounds[1]
    assert bounds[2]<=SAFE[2] and 1600<bounds[3]<=SAFE[3]
    if not closing:
        assert ImageChops.difference(image.crop(PANEL),baseline.crop(PANEL)).getbbox() is None
    assert ImageChops.difference(image.crop((0,1640,1080,1920)),baseline.crop((0,1640,1080,1920))).getbbox() is None
