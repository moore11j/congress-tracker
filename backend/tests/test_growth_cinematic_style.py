from PIL import Image, ImageStat
from app.services.growth_cinematic_style import CinematicStyle, entrance_offset


def test_cinematic_background_moves_but_keeps_caption_floor_dark():
    style = CinematicStyle((1080,1920))
    first, later = style.frame(0,1), style.frame(10,1)
    assert first.size == (1080,1920) and first.tobytes() != later.tobytes()
    assert max(ImageStat.Stat(later.crop((0,1740,1080,1920))).mean) < 30


def test_panel_preserves_product_interior_without_entrance_motion():
    style = CinematicStyle((1080,1920))
    base = style.frame(0,1)
    product = Image.new('RGB',(500,400),'#ca381b')
    style.panel(base, product, (120,550))
    # The 8px rounded frame may touch corner pixels; the evidence interior
    # must remain exact, with no tint, blur, vignette or background overlay.
    assert base.crop((128,558,612,942)).tobytes() == product.crop((8,8,492,392)).tobytes()
    offsets=[entrance_offset(t/100) for t in range(40)]
    assert set(offsets)=={0}


def test_cached_background_is_isolated_from_foreground_edits():
    style = CinematicStyle((320,568), brightness=.45, background=None)
    original = style.frame(1,1).tobytes()
    edited = style.frame(1,1)
    edited.paste('red', (0,0,320,568))
    assert style.frame(1,1).tobytes() == original
    # Closing light, scene entrance, and time changes must invalidate pixels.
    for t, elapsed, closing in [(1,1,True),(1,.2,False),(8,1,False)]:
        fresh = CinematicStyle((320,568), brightness=.45, background=None)
        assert style.frame(t,elapsed,closing).tobytes() == fresh.frame(t,elapsed,closing).tobytes()


def test_shadow_cache_reuses_decoration_but_not_product_pixels(monkeypatch):
    from PIL import ImageFilter
    style = CinematicStyle((320,568), background=None)
    filters = []
    original = Image.Image.filter
    def tracked(image, effect):
        if isinstance(effect, ImageFilter.GaussianBlur):
            filters.append(effect.radius)
        return original(image, effect)
    monkeypatch.setattr(Image.Image, 'filter', tracked)
    for color in ('red','green'):
        frame = Image.new('RGB',(320,568),'black')
        product = Image.new('RGB',(100,140),color)
        style.panel(frame, product, (40,100))
        assert frame.getpixel((90,170)) == product.getpixel((50,70))
    assert filters == [15]
    style.panel(frame, product, (50,120))
    assert filters == [15,15]  # New layout must get the correct shadow.
