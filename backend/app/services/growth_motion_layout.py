"""Approved motion design, composed around unmodified Walnut recordings.

Animation explains sequence, not financial performance: never synthesize chart
points or magnitudes from prose. Narration, captions and actions retain their
original timestamps. The only moving UI outside the recording is decorative.
"""
from functools import lru_cache

from PIL import Image, ImageDraw, ImageFilter, ImageOps

from app.services.growth_research_render import brand_font
from app.services.growth_cinematic_style import tight_caption
from app.services.growth_social_layout import SAFE, focus_crop

PANEL = (70, 510, 894, 1000)
BG, GREEN, WHITE, MUTED = '#0D1117', '#29D981', '#F3F7F6', '#A7B8B2'
EDGE = '#29413D'
TRANSITION_SECONDS = .22


def ease(value):
    value = max(0., min(1., value))
    return 1 - (1-value)**3


@lru_cache(maxsize=128)
def wrapped(text, size, max_width, max_lines):
    """Wrap whole words; reduce size rather than truncate financial copy."""
    for candidate_size in range(size, 15, -1):
        fnt = brand_font(candidate_size, True)
        lines = []
        for paragraph in str(text).splitlines() or ['']:
            line = ''
            for word in paragraph.split():
                candidate = (line+' '+word).strip()
                if line and fnt.getlength(candidate) > max_width:
                    lines.append(line)
                    line = word
                else:
                    line = candidate
            if line:
                lines.append(line)
        if len(lines) <= max_lines and all(fnt.getlength(s) <= max_width for s in lines):
            return tuple(lines), candidate_size
    raise ValueError('Motion heading exceeds the safe text area. Shorten the on-screen copy.')


def text(draw, value, y, size=30, *, color=WHITE, max_lines=2, center=False):
    lines, size = wrapped(str(value), size, 800, max_lines)
    for i, line in enumerate(lines):
        draw.text((482 if center else 70, y+i*round(size*1.17)), line,
                  font=brand_font(size, True), fill=color, anchor='ma' if center else 'la')


class MotionStyle:
    """A cached dark plate keeps CPU work bounded on the existing video worker."""

    def __init__(self, size, background=None):
        self.background = background
        self.plate = Image.new('RGB', size, BG)
        if background:
            source = ImageOps.fit(Image.open(background).convert('RGB'), size)
            # Keep the approved NVDA server-room context subdued behind footage.
            self.plate = Image.blend(self.plate, source, .12)
        light = Image.new('RGBA', (size[0]//4, size[1]//4))
        ImageDraw.Draw(light).ellipse((-15, 55, 230, 260), fill=(41, 217, 129, 24))
        light = light.filter(ImageFilter.GaussianBlur(42)).resize(size, Image.Resampling.BILINEAR)
        self.plate = Image.alpha_composite(self.plate.convert('RGBA'), light).convert('RGB')
        d = ImageDraw.Draw(self.plate)
        for x in range(0, size[0], 100):
            d.line((x, 0, x, size[1]), fill='#172328', width=1)
        for y in range(0, size[1], 100):
            d.line((0, y, size[0], y), fill='#172328', width=1)

    def frame(self, t, elapsed, closing=False):
        # Return a copy: repeated renders cannot mutate the shared plate.
        return self.plate.copy()

    def panel(self, image, pic, xy):
        x, y = xy
        d = ImageDraw.Draw(image)
        d.rounded_rectangle((x-4, y-4, x+pic.width+4, y+pic.height+4),
                            radius=14, fill=BG, outline=EDGE, width=2)
        image.paste(pic, xy)


def decorate(image, scene, scenes, creative, caption, *, elapsed=0., closing=False, logo=None):
    """A stable layout with progressive chapter marks and tight aligned captions."""
    layer = Image.new('RGBA', image.size)
    d = ImageDraw.Draw(layer)
    tutorial = creative.get('content_kind') == 'feature_tutorial'
    text(d, f"{creative.get('ticker', 'WALNUT')} / " + ('QUICK GUIDE' if tutorial else 'RESEARCH'),
         280, 22, color=GREEN)
    if closing:
        if logo is not None:
            layer.paste(logo.resize((148, 148), Image.Resampling.LANCZOS), (408, 470))
        text(d, 'Walnut Markets', 675, 55, center=True)
        text(d, 'Know more\nbefore you buy.', 780, 55, color=GREEN, center=True)
    else:
        text(d, scene['on_screen_text'], 337, 60)
        text(d, scene['subhead'], 468, 22, color=MUTED, max_lines=1)
        # Reveal only the panel accent, never erase or blend financial figures.
        length = round((PANEL[2]-PANEL[0])*ease(elapsed/.65))
        if length:
            d.line((PANEL[0], PANEL[1]-9, PANEL[0]+length, PANEL[1]-9), fill=GREEN, width=3)

    # Dissolve presentation chrome only. Product pixels, pointer and captions
    # remain live throughout so the first narrated action is never hidden.
    alpha = ease(elapsed/TRANSITION_SECONDS)
    if alpha < 1:
        layer.putalpha(layer.getchannel('A').point([round(i*alpha) for i in range(256)]))
    image.paste(layer, (0, 0), layer)
    d = ImageDraw.Draw(image)
    if caption:
        tight_caption(d, caption['text'], brand_font(52, True), y=1060, width=964, max_width=790)

    current = next(i for i, s in enumerate(scenes) if s['sequence'] == scene['sequence'])
    duration = max(.001, scene['end']-scene['start'])
    phase = max(0., min(1., elapsed/duration))
    gap = 12
    width = (824-gap*(len(scenes)-1))/len(scenes)
    for i in range(len(scenes)):
        x = 70+i*(width+gap)
        d.rounded_rectangle((x, 1230, x+width, 1234), radius=2, fill=EDGE)
        progress = 1 if i < current else phase if i == current else 0
        if progress:
            d.rounded_rectangle((x, 1230, x+width*progress, 1234), radius=2, fill=GREEN)
    text(d, 'walnutmarkets.com' if closing else 'ACTUAL WALNUT SCREENS · PUBLISHED RESEARCH',
         1270, 21, color=GREEN, center=closing, max_lines=1)
    text(d, 'Research only · Not investment advice', 1330, 18, color=MUTED, center=closing)
    text(d, 'Some features require a paid plan', 1360, 16, color=MUTED, center=closing)
    return image
