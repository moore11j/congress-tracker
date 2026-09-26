"""Recompose the reel around social controls; never scale the whole canvas.

The conservative shared layout is based on Walnut's actual TikTok/Instagram
screenshots. Platform chrome varies, so the top, bottom and action rail carry
only atmosphere. Product pixels are cropped around the recorded interaction.
"""
from PIL import Image, ImageDraw
from app.services.growth_research_render import brand_font, MINT, WHITE, MUTED
from app.services.growth_cinematic_style import tight_caption

SAFE = (48, 270, 912, 1390)
PANEL = (58, 435, 900, 1005)
CAPTION_Y = 1060


def focus_crop(index, asset):
    frame = asset['frames'][min(len(asset['frames'])-1, int(index))]
    camera = frame['camera']
    w, h = asset['viewport']['width'], asset['viewport']['height']
    if camera['height'] < h:
        # Honor the recorder's explicit evidence framing, including table width.
        return camera
    height = min(h, 700)
    target = max(0, min(h-height, frame['cursor'][1]-height*.42))
    return {'x': 0, 'y': target, 'width': w, 'height': height}


def _text(draw, text, y, size, *, color=WHITE, max_width=820, centered=False):
    """Fit type inside the shared safe area, preserving its vertical position."""
    size = min(size, 58)
    while size > 26 and draw.textlength(text, font=brand_font(size, True)) > max_width:
        size -= 1
    draw.text((480 if centered else 58, y), text, font=brand_font(size, True),
              fill=color, anchor='ma' if centered else 'la')


def decorate(image, scene, scenes, creative, caption, *, closing=False, logo=None):
    d = ImageDraw.Draw(image)
    tutorial = creative.get('content_kind') == 'feature_tutorial'
    eyebrow = 'WALNUT / QUICK GUIDE' if tutorial else f"{creative.get('ticker', 'WALNUT')} / RESEARCH"
    _text(d, eyebrow, 280, 22, color=MINT, centered=closing)
    if not closing:
        _text(d, scene['on_screen_text'], 332, 56)
        # A compact path line replaces the masthead and multiple footer labels.
        _text(d, scene['subhead'], 397, 21, color=MUTED)
    else:
        if logo is not None:
            image.paste(logo.resize((180,180), Image.Resampling.LANCZOS), (390,470))
        _text(d, 'Walnut Markets', 710, 58, centered=True)
        _text(d, "Don't follow a signal.", 805, 42, centered=True)
        _text(d, 'Follow the evidence.', 865, 42, color=MINT, centered=True)
    if caption:
        tight_caption(d, caption['text'], brand_font(52, True), y=CAPTION_Y,
                      width=960, max_width=790)
    gap = 12
    bar = (842-gap*(len(scenes)-1))/len(scenes)
    for i in range(len(scenes)):
        x = 58+i*(bar+gap)
        d.rounded_rectangle((x,1230,x+bar,1234), radius=2,
                            fill=MINT if i < scene['sequence'] else '#253c40')
    _text(d, 'walnutmarkets.com' if closing else ('ONE FEATURE. ONE NEXT STEP.' if tutorial else 'EXPLORE THE FULL BRIEF ON WALNUT'),
          1270, 22, color=MINT, centered=closing)
    _text(d, 'Research only · Not investment advice', 1330, 18, color=MUTED, centered=closing)
    _text(d, 'Some features require a paid plan', 1360, 16, color=MUTED, centered=closing)
    return image
