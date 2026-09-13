"""Atmospheric presentation only: never invent or paint over product evidence."""
import math
from pathlib import Path
from PIL import Image, ImageDraw, ImageFilter, ImageOps

BACKGROUND = Path(__file__).parents[1] / 'assets/growth/walnut-cinematic-atrium-v1.png'


class CinematicStyle:
    def __init__(self, size):
        self.w, self.h = size
        self.plate = ImageOps.fit(Image.open(BACKGROUND).convert('RGB'),
                                 (self.w + 100, self.h + 160), method=Image.Resampling.LANCZOS)
        self.veil = Image.new('RGBA', size, (2, 6, 23, 95))
        draw = ImageDraw.Draw(self.veil)
        for y in range(1480, self.h):
            alpha = round(95 + 145 * min(1, (y - 1480) / 260))
            draw.line((0, y, self.w, y), fill=(2, 6, 23, alpha))

    def frame(self, t, elapsed, closing=False):
        x = round(50 + 30 * math.sin(t * .13))
        y = round(75 - 55 * math.sin(t * .075))
        image = self.plate.crop((x, y, x + self.w, y + self.h)).convert('RGBA')
        image = Image.alpha_composite(image, self.veil)
        light = Image.new('RGBA', (self.w // 4, self.h // 4))
        d = ImageDraw.Draw(light)
        cx = 120 + 60 * math.sin(t * .17)
        d.ellipse((cx - 100, 90, cx + 100, 375), fill=(64, 200, 164, 25 if closing else 16))
        light = light.filter(ImageFilter.GaussianBlur(35)).resize(image.size, Image.Resampling.BILINEAR)
        image = Image.alpha_composite(image, light)
        if 0 <= elapsed < .5:
            light = Image.new('RGBA', image.size)
            d = ImageDraw.Draw(light)
            u = elapsed / .5
            yy = round(270 + u * 1180)
            alpha = round(125 * math.sin(math.pi * u))
            for xx in (30, self.w - 30):
                d.line((xx, yy - 65, xx, yy + 65), fill=(110, 231, 183, alpha), width=3)
            image = Image.alpha_composite(image, light)
        return image.convert('RGB')

    def panel(self, image, pic, xy):
        x, y = xy
        layer = Image.new('RGBA', image.size)
        d = ImageDraw.Draw(layer)
        d.rounded_rectangle((x-8,y-8,x+pic.width+8,y+pic.height+8), radius=20, fill=(0,0,0,215))
        layer = layer.filter(ImageFilter.GaussianBlur(15))
        image.paste(layer, (0,0), layer)
        image.paste(pic, xy)
        d = ImageDraw.Draw(image)
        d.rounded_rectangle((x-2,y-2,x+pic.width+2,y+pic.height+2), radius=8, outline='#365750', width=2)
        d.line((x+12,y-3,x+min(pic.width-12,240),y-3), fill='#6ee7b7', width=2)


def entrance_offset(elapsed):
    """An eased arrival without concealing, delaying or skipping UI actions."""
    u = max(0, min(1, elapsed / .24))
    return round(18 * (1-u)**3)
