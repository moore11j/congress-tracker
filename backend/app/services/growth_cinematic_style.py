"""Atmospheric presentation only: never invent or paint over product evidence."""
import math
from pathlib import Path
from PIL import Image, ImageDraw, ImageFilter, ImageOps

BACKGROUND = Path(__file__).parents[1] / 'assets/growth/walnut-cinematic-atrium-v1.png'
NVIDIA_BACKGROUND = BACKGROUND.with_name('walnut-nvidia-server-room-v1.png')


class CinematicStyle:
    def __init__(self, size, brightness=1.0, background=BACKGROUND):
        self.w, self.h = size
        self.brightness = brightness
        self.background = Path(background) if background else None
        self._background_key = self._light_key = self._frame_key = self._panel_key = None
        self.plate = (ImageOps.fit(Image.open(self.background).convert('RGB'),
                                 (self.w + 100, self.h + 160), method=Image.Resampling.LANCZOS)
                      if self.background else Image.new('RGB', (self.w + 100, self.h + 160), '#102327'))
        self.veil = Image.new('RGBA', size, (2, 6, 23, 95))
        draw = ImageDraw.Draw(self.veil)
        for y in range(1480, self.h):
            alpha = round(95 + 145 * min(1, (y - 1480) / 260))
            draw.line((0, y, self.w, y), fill=(2, 6, 23, alpha))

    def frame(self, t, elapsed, closing=False):
        x = round(50 + 30 * math.sin(t * .13))
        y = round(75 - 55 * math.sin(t * .075))
        cx = 120 + 60 * math.sin(t * .17)
        background_key = (x, y) if self.background else (0, 0)
        # Pillow rasterizes these ellipse coordinates to integers. Cache the
        # resulting pixels, not the product footage, while the geometry matches.
        light_key = (int(cx - 100), int(cx + 100), closing)
        frame_key = (background_key, light_key, elapsed if 0 <= elapsed < .5 else None)
        if frame_key == self._frame_key:
            return self._frame.copy()
        if background_key != self._background_key:
            image = self.plate.crop((x, y, x + self.w, y + self.h)).convert('RGBA')
            self._background = Image.alpha_composite(image, self.veil)
            self._background_key = background_key
        if light_key != self._light_key:
            light = Image.new('RGBA', (self.w // 4, self.h // 4))
            d = ImageDraw.Draw(light)
            d.ellipse((light_key[0], 90, light_key[1], 375), fill=(64, 200, 164, 25 if closing else 16))
            self._light = light.filter(ImageFilter.GaussianBlur(35)).resize((self.w, self.h), Image.Resampling.BILINEAR)
            self._light_key = light_key
        image = Image.alpha_composite(self._background, self._light)
        if 0 <= elapsed < .5:
            light = Image.new('RGBA', image.size)
            d = ImageDraw.Draw(light)
            u = elapsed / .5
            yy = round(270 + u * 1180)
            alpha = round(125 * math.sin(math.pi * u))
            for xx in (30, self.w - 30):
                d.line((xx, yy - 65, xx, yy + 65), fill=(110, 231, 183, alpha), width=3)
            image = Image.alpha_composite(image, light)
        image = image.convert('RGB')
        if self.brightness != 1.0:
            image = image.point([round(v*self.brightness) for v in range(256)]*3)
        self._frame_key, self._frame = frame_key, image.copy()
        return image

    def panel(self, image, pic, xy):
        x, y = xy
        panel_key = (image.size, pic.size, xy)
        if panel_key != self._panel_key:
            layer = Image.new('RGBA', image.size)
            d = ImageDraw.Draw(layer)
            d.rounded_rectangle((x-8,y-8,x+pic.width+8,y+pic.height+8), radius=20, fill=(0,0,0,215))
            self._panel = layer.filter(ImageFilter.GaussianBlur(15))
            self._panel_key = panel_key
        image.paste(self._panel, (0,0), self._panel)
        image.paste(pic, xy)
        d = ImageDraw.Draw(image)
        d.rounded_rectangle((x-2,y-2,x+pic.width+2,y+pic.height+2), radius=8, outline='#365750', width=2)
        d.line((x+12,y-3,x+min(pic.width-12,240),y-3), fill='#6ee7b7', width=2)


def entrance_offset(elapsed):
    """Keep headlines and footage fixed across clean scene cuts."""
    return 0


def tight_caption(draw, text, font, *, y=1540, width=1080, max_width=900):
    """Fit the outline to the rendered phrase, including wrapped lines."""
    lines=[];line=''
    for word in text.split():
        candidate=(line+' '+word).strip()
        if line and draw.textlength(candidate,font=font)>max_width:
            lines.append(line);line=word
        else:line=candidate
    if line:lines.append(line)
    if not lines:return
    step=round(font.size*1.16)
    boxes=[draw.textbbox((width/2,y+i*step),line,font=font,anchor='mt') for i,line in enumerate(lines)]
    bounds=(min(b[0] for b in boxes)-16,min(b[1] for b in boxes)-10,
            max(b[2] for b in boxes)+16,max(b[3] for b in boxes)+10)
    draw.rounded_rectangle(bounds,radius=12,fill='#08121f',outline='#253c40',width=1)
    for i,line in enumerate(lines):draw.text((width/2,y+i*step),line,font=font,fill='#f1f5f9',anchor='mt')
