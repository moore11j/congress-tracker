"""Review-only, evidence-led framing of retained product recordings.

Each source frame needs a reviewed rectangle. Never guess a crop from the
pointer: that can remove a date, table column, or the subject of a number.
Older layouts and approved assets remain reproducible.
"""
import math
from app.services.growth_motion_layout import decorate as motion_decorate
from app.services.growth_social_layout import focus_crop as legacy_focus_crop

SAFE = (48, 270, 912, 1640)
PANEL = (70, 510, 894, 1310)


def captured_focus_crop(index, asset):
    """Use capture-time evidence bounds; older footage keeps its full context."""
    frame = asset['frames'][min(len(asset['frames'])-1, max(0, int(index)))]
    if frame.get('evidence_camera'):
        return focus_crop(index, asset)
    crop = legacy_focus_crop(index, asset)
    w, h = asset['viewport']['width'], asset['viewport']['height']
    width, height = min(w, crop['width']), min(h, crop['height'])
    return dict(x=max(0, min(w-width, crop['x'])), y=max(0, min(h-height, crop['y'])),
                width=width, height=height)


def focus_crop(index, asset):
    frame = asset['frames'][min(len(asset['frames'])-1, max(0, int(index)))]
    crop = frame.get('evidence_camera')
    if not crop:
        raise ValueError('Close-up rendering requires reviewed evidence framing for every frame.')
    x, y, w, h = (float(crop[key]) for key in ('x', 'y', 'width', 'height'))
    if (not all(math.isfinite(v) for v in (x, y, w, h)) or min(x, y) < 0
            or min(w, h) <= 0 or x+w > asset['viewport']['width']
            or y+h > asset['viewport']['height']):
        raise ValueError('Evidence framing exceeds the original recording.')
    return crop


def decorate(*args, **kwargs):
    return motion_decorate(*args, **kwargs, closeup=True)
