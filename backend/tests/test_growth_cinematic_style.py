from PIL import Image, ImageStat
from app.services.growth_cinematic_style import CinematicStyle, entrance_offset


def test_cinematic_background_moves_but_keeps_caption_floor_dark():
    style = CinematicStyle((1080,1920))
    first, later = style.frame(0,1), style.frame(10,1)
    assert first.size == (1080,1920) and first.tobytes() != later.tobytes()
    assert max(ImageStat.Stat(later.crop((0,1740,1080,1920))).mean) < 30


def test_panel_preserves_product_interior_and_arrival_settles():
    style = CinematicStyle((1080,1920))
    base = style.frame(0,1)
    product = Image.new('RGB',(500,400),'#ca381b')
    style.panel(base, product, (120,550))
    # The 8px rounded frame may touch corner pixels; the evidence interior
    # must remain exact, with no tint, blur, vignette or background overlay.
    assert base.crop((128,558,612,942)).tobytes() == product.crop((8,8,492,392)).tobytes()
    offsets=[entrance_offset(t/100) for t in range(40)]
    assert offsets[0]==18 and offsets[-1]==0
    assert all(a>=b for a,b in zip(offsets,offsets[1:]))
